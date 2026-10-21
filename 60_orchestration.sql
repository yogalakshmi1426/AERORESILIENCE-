-- =====================================================
-- AERORESILIENCE - 60_orchestration.sql      (V2.5: orchestration with Snowflake Tasks)
--
-- TASK GRAPH (runs daily 06:00 London time once resumed):
--   T1_LOAD_BRONZE   -> CALL SP_LOAD_BRONZE()          all COPY INTO (idempotent)
--   T2_CHECK_BRONZE  -> CALL SP_CHECK_BRONZE()         delivery-log reconciliation; FAILS on mismatch
--   T3_DBT_BUILD     -> EXECUTE DBT PROJECT ... build  42 models + snapshot + 117 tests
-- A failed task stops its children = quality gate (dbt never runs on an incomplete load).
--
-- Run ONE SECTION AT A TIME, top to bottom. Role ACCOUNTADMIN, warehouse AERO_WH.
-- SECTION 3 needs the dbt project DEPLOYED first (see the instructions in section 3).
-- =====================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE AERO_WH;


-- =====================================================
-- SECTION 0. Schema for orchestration objects + a run log (our own audit trail)
-- =====================================================
CREATE SCHEMA IF NOT EXISTS AERORESILIENCE.ORCHESTRATION
  COMMENT = 'Pipeline orchestration: stored procedures, tasks, deployed dbt project, run log';

CREATE TABLE IF NOT EXISTS AERORESILIENCE.ORCHESTRATION.PIPELINE_RUN_LOG (
    logged_at   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    step        STRING,
    status      STRING,          -- OK / FAILED
    detail      STRING
)
COMMENT = 'One row per pipeline step per run, written by the stored procedures.';


-- =====================================================
-- SECTION 1. SP_LOAD_BRONZE: every COPY INTO from 10_bronze_load.sql + Incident B phase 3
--   Idempotent: COPY skips files already loaded into each table (load metadata, 64 days).
-- =====================================================
CREATE OR REPLACE PROCEDURE AERORESILIENCE.ORCHESTRATION.SP_LOAD_BRONZE()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    total_rows INTEGER;
BEGIN
    -- reference data
    COPY INTO AERORESILIENCE.BRONZE.REF_AIRPORTS_RAW FROM (
        SELECT $1, $2, $3, $4, $5,
               METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, METADATA$FILE_LAST_MODIFIED, CURRENT_TIMESTAMP()
        FROM @AERORESILIENCE.BRONZE.RAW_STAGE/reference/)
    FILE_FORMAT = (FORMAT_NAME = 'AERORESILIENCE.BRONZE.CSV_FORMAT') PATTERN = '.*airports[.]csv';

    COPY INTO AERORESILIENCE.BRONZE.REF_AIRLINES_RAW FROM (
        SELECT $1, $2, $3, $4,
               METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, METADATA$FILE_LAST_MODIFIED, CURRENT_TIMESTAMP()
        FROM @AERORESILIENCE.BRONZE.RAW_STAGE/reference/)
    FILE_FORMAT = (FORMAT_NAME = 'AERORESILIENCE.BRONZE.CSV_FORMAT') PATTERN = '.*airlines[.]csv';

    COPY INTO AERORESILIENCE.BRONZE.REF_AIRCRAFT_RAW FROM (
        SELECT $1, $2, $3, $4, $5, $6, $7, $8,
               METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, METADATA$FILE_LAST_MODIFIED, CURRENT_TIMESTAMP()
        FROM @AERORESILIENCE.BRONZE.RAW_STAGE/reference/)
    FILE_FORMAT = (FORMAT_NAME = 'AERORESILIENCE.BRONZE.CSV_FORMAT') PATTERN = '.*aircraft[.]csv';

    COPY INTO AERORESILIENCE.BRONZE.REF_ROUTES_RAW FROM (
        SELECT $1, $2, $3, $4, $5, $6,
               METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, METADATA$FILE_LAST_MODIFIED, CURRENT_TIMESTAMP()
        FROM @AERORESILIENCE.BRONZE.RAW_STAGE/reference/)
    FILE_FORMAT = (FORMAT_NAME = 'AERORESILIENCE.BRONZE.CSV_FORMAT') PATTERN = '.*routes[.]csv';

    COPY INTO AERORESILIENCE.BRONZE.REF_MIN_CONNECTION_TIMES_RAW FROM (
        SELECT $1, $2, $3,
               METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, METADATA$FILE_LAST_MODIFIED, CURRENT_TIMESTAMP()
        FROM @AERORESILIENCE.BRONZE.RAW_STAGE/reference/)
    FILE_FORMAT = (FORMAT_NAME = 'AERORESILIENCE.BRONZE.CSV_FORMAT') PATTERN = '.*min_connection_times[.]csv';

    COPY INTO AERORESILIENCE.BRONZE.REF_SUPPLIERS_RAW FROM (
        SELECT $1, $2, $3, $4, $5,
               METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, METADATA$FILE_LAST_MODIFIED, CURRENT_TIMESTAMP()
        FROM @AERORESILIENCE.BRONZE.RAW_STAGE/reference/)
    FILE_FORMAT = (FORMAT_NAME = 'AERORESILIENCE.BRONZE.CSV_FORMAT') PATTERN = '.*suppliers[.]csv';

    COPY INTO AERORESILIENCE.BRONZE.REF_PARTS_RAW FROM (
        SELECT $1, $2, $3, $4, $5, $6, $7, $8,
               METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, METADATA$FILE_LAST_MODIFIED, CURRENT_TIMESTAMP()
        FROM @AERORESILIENCE.BRONZE.RAW_STAGE/reference/)
    FILE_FORMAT = (FORMAT_NAME = 'AERORESILIENCE.BRONZE.CSV_FORMAT') PATTERN = '.*parts[.]csv';

    COPY INTO AERORESILIENCE.BRONZE.SCHED_FLIGHTS_RAW FROM (
        SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14,
               METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, METADATA$FILE_LAST_MODIFIED, CURRENT_TIMESTAMP()
        FROM @AERORESILIENCE.BRONZE.RAW_STAGE/truth/)
    FILE_FORMAT = (FORMAT_NAME = 'AERORESILIENCE.BRONZE.CSV_FORMAT') PATTERN = '.*flights_schedule[.]csv';

    -- operational feeds
    COPY INTO AERORESILIENCE.BRONZE.FOS_MESSAGES_RAW FROM (
        SELECT $1, METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, METADATA$FILE_LAST_MODIFIED, CURRENT_TIMESTAMP()
        FROM @AERORESILIENCE.BRONZE.RAW_STAGE/raw/flight_ops/)
    FILE_FORMAT = (FORMAT_NAME = 'AERORESILIENCE.BRONZE.JSON_FORMAT') PATTERN = '.*fos_.*[.]jsonl';

    COPY INTO AERORESILIENCE.BRONZE.BAG_SCANS_RAW FROM (
        SELECT $1, METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, METADATA$FILE_LAST_MODIFIED, CURRENT_TIMESTAMP()
        FROM @AERORESILIENCE.BRONZE.RAW_STAGE/raw/baggage/)
    FILE_FORMAT = (FORMAT_NAME = 'AERORESILIENCE.BRONZE.JSON_FORMAT') PATTERN = '.*bag_scans_.*[.]jsonl[.]gz';

    COPY INTO AERORESILIENCE.BRONZE.MRO_DEFECTS_RAW FROM (
        SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
               METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, METADATA$FILE_LAST_MODIFIED, CURRENT_TIMESTAMP()
        FROM @AERORESILIENCE.BRONZE.RAW_STAGE/raw/maintenance/)
    FILE_FORMAT = (FORMAT_NAME = 'AERORESILIENCE.BRONZE.CSV_FORMAT') PATTERN = '.*mro_defects_.*[.]csv';

    COPY INTO AERORESILIENCE.BRONZE.INVENTORY_SNAPSHOTS_RAW FROM (
        SELECT $1, $2, $3, $4, $5,
               METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, METADATA$FILE_LAST_MODIFIED, CURRENT_TIMESTAMP()
        FROM @AERORESILIENCE.BRONZE.RAW_STAGE/raw/supply_chain/inventory/)
    FILE_FORMAT = (FORMAT_NAME = 'AERORESILIENCE.BRONZE.CSV_FORMAT') PATTERN = '.*inventory_.*[.]csv';

    COPY INTO AERORESILIENCE.BRONZE.RES_BOOKINGS_RAW FROM (
        SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
               METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, METADATA$FILE_LAST_MODIFIED, CURRENT_TIMESTAMP()
        FROM @AERORESILIENCE.BRONZE.RAW_STAGE/raw/reservations/)
    FILE_FORMAT = (FORMAT_NAME = 'AERORESILIENCE.BRONZE.CSV_FORMAT') PATTERN = '.*bookings_.*[.]csv';

    COPY INTO AERORESILIENCE.BRONZE.RES_SEGMENTS_RAW FROM (
        SELECT $1, $2, $3, $4, $5, $6,
               METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, METADATA$FILE_LAST_MODIFIED, CURRENT_TIMESTAMP()
        FROM @AERORESILIENCE.BRONZE.RAW_STAGE/raw/reservations/)
    FILE_FORMAT = (FORMAT_NAME = 'AERORESILIENCE.BRONZE.CSV_FORMAT') PATTERN = '.*segments_.*[.]csv';

    COPY INTO AERORESILIENCE.BRONZE.DCS_BOARDED_RAW FROM (
        SELECT $1, $2, $3, $4, $5,
               METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, METADATA$FILE_LAST_MODIFIED, CURRENT_TIMESTAMP()
        FROM @AERORESILIENCE.BRONZE.RAW_STAGE/raw/departure_control/)
    FILE_FORMAT = (FORMAT_NAME = 'AERORESILIENCE.BRONZE.CSV_FORMAT') PATTERN = '.*boarded_.*[.]csv';

    COPY INTO AERORESILIENCE.BRONZE.WEATHER_ALERTS_RAW FROM (
        SELECT $1, METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, METADATA$FILE_LAST_MODIFIED, CURRENT_TIMESTAMP()
        FROM @AERORESILIENCE.BRONZE.RAW_STAGE/raw/external/weather/)
    FILE_FORMAT = (FORMAT_NAME = 'AERORESILIENCE.BRONZE.JSON_ARRAY_FORMAT') PATTERN = '.*weather_alerts_.*[.]json';

    COPY INTO AERORESILIENCE.BRONZE.CTRL_DELIVERY_LOG_RAW FROM (
        SELECT $1, $2, $3, $4,
               METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, METADATA$FILE_LAST_MODIFIED, CURRENT_TIMESTAMP()
        FROM @AERORESILIENCE.BRONZE.RAW_STAGE/raw/_control/)
    FILE_FORMAT = (FORMAT_NAME = 'AERORESILIENCE.BRONZE.CSV_FORMAT') PATTERN = '.*delivery_log[.]csv';

    -- supplier portal: by column NAME with schema evolution (Incident B fix)
    COPY INTO AERORESILIENCE.BRONZE.SUPPLIER_SHIPMENTS_RAW
    FROM @AERORESILIENCE.BRONZE.RAW_STAGE/raw/supply_chain/shipments/
    FILE_FORMAT = (FORMAT_NAME = 'AERORESILIENCE.BRONZE.CSV_PARSE_HEADER_FORMAT')
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
    INCLUDE_METADATA = (_source_file = METADATA$FILENAME,
                        _source_row = METADATA$FILE_ROW_NUMBER,
                        _file_last_modified = METADATA$FILE_LAST_MODIFIED)
    PATTERN = '.*supplier_shipments_.*[.]csv';

    COPY INTO AERORESILIENCE.BRONZE.SUPPLIER_POS_RAW
    FROM @AERORESILIENCE.BRONZE.RAW_STAGE/raw/supply_chain/purchase_orders/
    FILE_FORMAT = (FORMAT_NAME = 'AERORESILIENCE.BRONZE.CSV_PARSE_HEADER_FORMAT')
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
    INCLUDE_METADATA = (_source_file = METADATA$FILENAME,
                        _source_row = METADATA$FILE_ROW_NUMBER,
                        _file_last_modified = METADATA$FILE_LAST_MODIFIED)
    PATTERN = '.*supplier_purchase_orders_.*[.]csv';

    SELECT COUNT(*) INTO :total_rows FROM AERORESILIENCE.BRONZE.BAG_SCANS_RAW;

    INSERT INTO AERORESILIENCE.ORCHESTRATION.PIPELINE_RUN_LOG (step, status, detail)
    VALUES ('T1_LOAD_BRONZE', 'OK', 'All COPY INTO commands completed. BAG_SCANS_RAW rows: ' || :total_rows);

    RETURN 'BRONZE load complete. BAG_SCANS_RAW rows: ' || total_rows;
END;
$$;


-- =====================================================
-- SECTION 2. SP_CHECK_BRONZE: the quality gate
--   Rows loaded per file must equal the record count each source declared in the delivery log.
--   Any mismatch RAISES an error -> the task FAILS -> dbt (T3) does not run.
-- =====================================================
CREATE OR REPLACE PROCEDURE AERORESILIENCE.ORCHESTRATION.SP_CHECK_BRONZE()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    files_checked    INTEGER;
    files_mismatched INTEGER;
    reconciliation_failed EXCEPTION (-20001, 'BRONZE reconciliation failed: rows loaded do not match the delivery log. See PIPELINE_RUN_LOG.');
BEGIN
    SELECT COUNT(*),
           COUNT_IF(COALESCE(l.rows_loaded, 0) <> d.record_count::NUMBER)
      INTO :files_checked, :files_mismatched
    FROM AERORESILIENCE.BRONZE.CTRL_DELIVERY_LOG_RAW d
    LEFT JOIN (
                  SELECT _source_file, COUNT(*) AS rows_loaded FROM AERORESILIENCE.BRONZE.FOS_MESSAGES_RAW        GROUP BY 1
        UNION ALL SELECT _source_file, COUNT(*) FROM AERORESILIENCE.BRONZE.BAG_SCANS_RAW                          GROUP BY 1
        UNION ALL SELECT _source_file, COUNT(*) FROM AERORESILIENCE.BRONZE.MRO_DEFECTS_RAW                        GROUP BY 1
        UNION ALL SELECT _source_file, COUNT(*) FROM AERORESILIENCE.BRONZE.INVENTORY_SNAPSHOTS_RAW                GROUP BY 1
        UNION ALL SELECT _source_file, COUNT(*) FROM AERORESILIENCE.BRONZE.RES_BOOKINGS_RAW                       GROUP BY 1
        UNION ALL SELECT _source_file, COUNT(*) FROM AERORESILIENCE.BRONZE.RES_SEGMENTS_RAW                       GROUP BY 1
        UNION ALL SELECT _source_file, COUNT(*) FROM AERORESILIENCE.BRONZE.DCS_BOARDED_RAW                        GROUP BY 1
        UNION ALL SELECT _source_file, COUNT(*) FROM AERORESILIENCE.BRONZE.WEATHER_ALERTS_RAW                     GROUP BY 1
        UNION ALL SELECT _source_file, COUNT(*) FROM AERORESILIENCE.BRONZE.SUPPLIER_SHIPMENTS_RAW                 GROUP BY 1
        UNION ALL SELECT _source_file, COUNT(*) FROM AERORESILIENCE.BRONZE.SUPPLIER_POS_RAW                       GROUP BY 1
    ) l
      ON l._source_file = 'raw/' || d.file_path;

    IF (files_mismatched > 0) THEN
        INSERT INTO AERORESILIENCE.ORCHESTRATION.PIPELINE_RUN_LOG (step, status, detail)
        VALUES ('T2_CHECK_BRONZE', 'FAILED', :files_mismatched || ' of ' || :files_checked || ' files do not match the delivery log');
        RAISE reconciliation_failed;
    END IF;

    INSERT INTO AERORESILIENCE.ORCHESTRATION.PIPELINE_RUN_LOG (step, status, detail)
    VALUES ('T2_CHECK_BRONZE', 'OK', :files_checked || ' files checked, 0 mismatched');

    RETURN files_checked || ' files checked, 0 mismatched';
END;
$$;

-- Test the two procedures by hand BEFORE scheduling them (expected: both succeed)
CALL AERORESILIENCE.ORCHESTRATION.SP_LOAD_BRONZE();      -- every COPY skips: files already loaded
CALL AERORESILIENCE.ORCHESTRATION.SP_CHECK_BRONZE();     -- expected: 327 files checked, 0 mismatched


-- =====================================================
-- SECTION 2.5  DEPLOY THE dbt PROJECT  (ALREADY DONE - kept for documentation)
--   A task cannot run files from a Workspace, so the project is deployed as a Snowflake object.
--   Re-deploy after ANY change to dbt files, otherwise the schedule runs the old version.
--   (Easiest re-deploy: dbt project -> Connect v -> Deploy dbt project -> choose AERO_WH.)
-- =====================================================
-- CREATE DBT PROJECT AERORESILIENCE.ORCHESTRATION.AERORESILIENCE_DBT
--   FROM $$snow://workspace/"USER$yogalakshmishanmugajothi".PUBLIC.DEFAULT$/versions/live/aeroresilience_dbt$$
--   DEFAULT_TARGET = 'dev'
--   DBT_VERSION = '1.9.4'
--   COMMENT = 'AERORESILIENCE dbt pipeline: staging, intermediate, marts, snapshot, 160 tests';

SHOW DBT PROJECTS IN SCHEMA AERORESILIENCE.ORCHESTRATION;    -- expected: 1 row, AERORESILIENCE_DBT


-- =====================================================
-- SECTION 3. The task graph  T1 -> T2 -> T3
--   HOW TO RUN: select ALL THREE statements below together (from the first CREATE to the
--   last ';' of T3) and click Run. They must be created in this order, every time:
--   re-creating T1 alone breaks its link to T2, so always re-create all three together.
-- =====================================================
CREATE OR REPLACE TASK AERORESILIENCE.ORCHESTRATION.T1_LOAD_BRONZE
    WAREHOUSE = AERO_WH
    SCHEDULE  = 'USING CRON 0 6 * * * Europe/London'     -- every day at 06:00 London time
    TASK_AUTO_RETRY_ATTEMPTS = 1                          -- retry the whole graph once on failure
    COMMENT   = 'ROOT: load all new source files into BRONZE (idempotent COPY INTO)'
AS
    CALL AERORESILIENCE.ORCHESTRATION.SP_LOAD_BRONZE();

CREATE OR REPLACE TASK AERORESILIENCE.ORCHESTRATION.T2_CHECK_BRONZE
    WAREHOUSE = AERO_WH
    COMMENT   = 'QUALITY GATE: rows loaded vs delivery log; fails on any mismatch'
    AFTER AERORESILIENCE.ORCHESTRATION.T1_LOAD_BRONZE
AS
    CALL AERORESILIENCE.ORCHESTRATION.SP_CHECK_BRONZE();

CREATE OR REPLACE TASK AERORESILIENCE.ORCHESTRATION.T3_DBT_BUILD
    WAREHOUSE = AERO_WH
    COMMENT   = 'TRANSFORM + TEST: dbt build (models, snapshot, tests); fails if any test fails'
    AFTER AERORESILIENCE.ORCHESTRATION.T2_CHECK_BRONZE
AS
    EXECUTE DBT PROJECT AERORESILIENCE.ORCHESTRATION.AERORESILIENCE_DBT ARGS = 'build --target dev';

-- Check the links (look at the "predecessors" column): T1 empty, T2 -> T1, T3 -> T2
SHOW TASKS IN SCHEMA AERORESILIENCE.ORCHESTRATION;


-- =====================================================
-- SECTION 4. Switch on and run now
--   Tasks are created SUSPENDED. Resume children first, root last. Select all 4 lines, Run.
-- =====================================================
ALTER TASK AERORESILIENCE.ORCHESTRATION.T3_DBT_BUILD    RESUME;
ALTER TASK AERORESILIENCE.ORCHESTRATION.T2_CHECK_BRONZE RESUME;
ALTER TASK AERORESILIENCE.ORCHESTRATION.T1_LOAD_BRONZE  RESUME;
EXECUTE TASK AERORESILIENCE.ORCHESTRATION.T1_LOAD_BRONZE;    -- run the whole graph now (don't wait for 06:00)


-- =====================================================
-- SECTION 5. Monitor: wait ~2 minutes, then re-run every minute until all 3 show SUCCEEDED
-- =====================================================
SELECT name, state, scheduled_time, completed_time, error_message
FROM TABLE(AERORESILIENCE.INFORMATION_SCHEMA.TASK_HISTORY(
        SCHEDULED_TIME_RANGE_START => DATEADD('hour', -2, CURRENT_TIMESTAMP())))
WHERE database_name = 'AERORESILIENCE' AND schema_name = 'ORCHESTRATION'
ORDER BY scheduled_time DESC, name;

SELECT * FROM AERORESILIENCE.ORCHESTRATION.PIPELINE_RUN_LOG ORDER BY logged_at DESC;


-- =====================================================
-- SECTION 6. Save trial credits: suspend the daily schedule after testing
--   (the graph still runs whenever you EXECUTE TASK by hand - resume T1 first)
-- =====================================================
ALTER TASK AERORESILIENCE.ORCHESTRATION.T1_LOAD_BRONZE SUSPEND;


-- =====================================================
-- SECTION 7. Status checks (run any time)
-- =====================================================
SHOW PROCEDURES LIKE 'SP_%' IN SCHEMA AERORESILIENCE.ORCHESTRATION;   -- SP_LOAD_BRONZE, SP_CHECK_BRONZE
SHOW DBT PROJECTS IN SCHEMA AERORESILIENCE.ORCHESTRATION;              -- AERORESILIENCE_DBT
SHOW TASKS IN SCHEMA AERORESILIENCE.ORCHESTRATION;                     -- T1, T2, T3 + state