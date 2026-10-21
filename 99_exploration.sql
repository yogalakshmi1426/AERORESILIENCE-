-- =====================================================
-- AERORESILIENCE - 99_exploration.sql
-- Looking and checking. Nothing here builds the platform.
-- Run one section at a time (cursor inside the statement -> Run).
-- Role: ACCOUNTADMIN   Warehouse: AERO_WH
-- =====================================================

USE WAREHOUSE AERO_WH;

-- A. Re-index the stage after a generator uploads new files
ALTER STAGE AERORESILIENCE.BRONZE.RAW_STAGE REFRESH;

-- B. Every file in the stage
SELECT relative_path, size, last_modified
FROM DIRECTORY(@AERORESILIENCE.BRONZE.RAW_STAGE)
ORDER BY relative_path;

-- C. Files per area (expected: reference 7, truth 14, raw 328 after step 04)
SELECT SPLIT_PART(relative_path, '/', 1) AS area, COUNT(*) AS files
FROM DIRECTORY(@AERORESILIENCE.BRONZE.RAW_STAGE)
GROUP BY 1 ORDER BY 1;

-- D. Raw files per source system (expected total: 328)
SELECT SPLIT_PART(relative_path, '/', 2) AS source_system, COUNT(*) AS files
FROM DIRECTORY(@AERORESILIENCE.BRONZE.RAW_STAGE)
WHERE relative_path LIKE 'raw/%'
GROUP BY 1 ORDER BY 1;

-- E. Peek: the worst grounding (CSV)
SELECT $1 AS fault_id, $2 AS aircraft, $5 AS airport, $6 AS detected_utc,
       $8 AS part, $10 AS criticality, $11 AS action, $15 AS repair_completed_utc
FROM @AERORESILIENCE.BRONZE.RAW_STAGE/truth/maintenance_events.csv
     (FILE_FORMAT => 'AERORESILIENCE.BRONZE.CSV_FORMAT');

-- F. Peek: misconnected bags (nested JSON, gzip)
SELECT $1:event_type::STRING         AS event_type,
       $1:event_time_utc::STRING     AS event_time,
       $1:bag.tag::STRING            AS bag_tag,
       $1:location.airport::STRING   AS airport,
       $1:flight.flight_id::STRING   AS flight_id
FROM @AERORESILIENCE.BRONZE.RAW_STAGE/truth/bag_scans.jsonl.gz
     (FILE_FORMAT => 'AERORESILIENCE.BRONZE.JSON_FORMAT')
WHERE $1:event_type::STRING = 'MISSED_CONNECTION'
LIMIT 10;

-- G. Peek: the clock change in the schedule (BA from LHR at 06:00 local)
SELECT $1 AS flight_id, $5 AS origin, $6 AS dest, $10 AS dep_utc, $12 AS dep_local
FROM @AERORESILIENCE.BRONZE.RAW_STAGE/truth/flights_schedule.csv
     (FILE_FORMAT => 'AERORESILIENCE.BRONZE.CSV_FORMAT')
WHERE $3 = 'BA' AND $5 = 'LHR'
  AND ($12 LIKE '2026-10-24 06%' OR $12 LIKE '2026-10-26 06%')
ORDER BY $10;

-- H. V2.4 snapshot: full history of one aircraft (SCD Type 2)
--    Every version is kept; dbt_valid_to = NULL marks the current version.
SELECT registration, seats, dbt_valid_from, dbt_valid_to
FROM AERORESILIENCE.DBT_DEV_SNAPSHOTS.SNAP_AIRCRAFT
WHERE registration = 'G-QAH'
ORDER BY dbt_valid_from;

-- I. V2.4 test (ALREADY DONE - kept for documentation, commented out on purpose)
--    Simulates an aircraft refit at source, then reverts it. Each step needs a dbt Build
--    before re-running query H. Remove the comment markers only to repeat the test.
-- UPDATE AERORESILIENCE.BRONZE.REF_AIRCRAFT_RAW
-- SET seats = TO_VARCHAR(TRY_TO_NUMBER(seats) + 20)
-- WHERE UPPER(TRIM(registration)) = 'G-QAH';
--
-- UPDATE AERORESILIENCE.BRONZE.REF_AIRCRAFT_RAW
-- SET seats = TO_VARCHAR(TRY_TO_NUMBER(seats) - 20)
-- WHERE UPPER(TRIM(registration)) = 'G-QAH';