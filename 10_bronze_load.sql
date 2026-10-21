-- =====================================================
-- AERORESILIENCE - 10_bronze_load.sql
-- BRONZE = source data exactly as received, inside tables.
--   * CSV columns are all STRING (no interpretation - that is SILVER's job)
--   * JSON records go into one VARIANT column, untouched
--   * every row records WHERE it came from and WHEN it was loaded (lineage)
-- COPY INTO is idempotent: files already loaded into a table are skipped.
-- Run top to bottom (Run All). Safe to re-run.
-- NOT YET: supplier portal (shipments, purchase orders) -> Incident B lesson.
-- =====================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE AERO_WH;
USE SCHEMA AERORESILIENCE.BRONZE;


-- =====================================================
-- 1. REFERENCE DATA (master data from the airline's planning systems)
-- =====================================================
CREATE TABLE IF NOT EXISTS REF_AIRPORTS_RAW (
    airport_code STRING, airport_name STRING, city STRING, country_code STRING, timezone STRING,
    _source_file STRING, _source_row NUMBER, _file_last_modified TIMESTAMP_NTZ, _loaded_at TIMESTAMP_NTZ)
COMMENT = 'BRONZE reference: airports as received. Grain: one row per file line.';

CREATE TABLE IF NOT EXISTS REF_AIRLINES_RAW (
    airline_code STRING, airline_name STRING, country_code STRING, hub_airport_code STRING,
    _source_file STRING, _source_row NUMBER, _file_last_modified TIMESTAMP_NTZ, _loaded_at TIMESTAMP_NTZ)
COMMENT = 'BRONZE reference: airlines as received. Grain: one row per file line.';

CREATE TABLE IF NOT EXISTS REF_AIRCRAFT_RAW (
    registration STRING, airline_code STRING, icao_type STRING, manufacturer STRING, model STRING,
    body_type STRING, seats STRING, year_built STRING,
    _source_file STRING, _source_row NUMBER, _file_last_modified TIMESTAMP_NTZ, _loaded_at TIMESTAMP_NTZ)
COMMENT = 'BRONZE reference: aircraft as received. Grain: one row per file line.';

CREATE TABLE IF NOT EXISTS REF_ROUTES_RAW (
    route_id STRING, airline_code STRING, origin_airport_code STRING, destination_airport_code STRING,
    block_time_minutes STRING, required_body_type STRING,
    _source_file STRING, _source_row NUMBER, _file_last_modified TIMESTAMP_NTZ, _loaded_at TIMESTAMP_NTZ)
COMMENT = 'BRONZE reference: routes as received. Grain: one row per file line.';

CREATE TABLE IF NOT EXISTS REF_MIN_CONNECTION_TIMES_RAW (
    airport_code STRING, connection_type STRING, min_connection_minutes STRING,
    _source_file STRING, _source_row NUMBER, _file_last_modified TIMESTAMP_NTZ, _loaded_at TIMESTAMP_NTZ)
COMMENT = 'BRONZE reference: minimum connection times as received. Grain: one row per file line.';

CREATE TABLE IF NOT EXISTS REF_SUPPLIERS_RAW (
    supplier_id STRING, supplier_name STRING, country_code STRING, ship_from_airport_code STRING,
    on_time_rate STRING,
    _source_file STRING, _source_row NUMBER, _file_last_modified TIMESTAMP_NTZ, _loaded_at TIMESTAMP_NTZ)
COMMENT = 'BRONZE reference: suppliers as received. Grain: one row per file line.';

CREATE TABLE IF NOT EXISTS REF_PARTS_RAW (
    part_number STRING, description STRING, ata_chapter STRING, icao_type STRING, criticality STRING,
    unit_cost_gbp STRING, standard_lead_time_days STRING, supplier_id STRING,
    _source_file STRING, _source_row NUMBER, _file_last_modified TIMESTAMP_NTZ, _loaded_at TIMESTAMP_NTZ)
COMMENT = 'BRONZE reference: spare parts catalogue as received. Grain: one row per file line.';

COPY INTO REF_AIRPORTS_RAW FROM (
    SELECT $1, $2, $3, $4, $5,
           METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, METADATA$FILE_LAST_MODIFIED, CURRENT_TIMESTAMP()
    FROM @RAW_STAGE/reference/)
FILE_FORMAT = (FORMAT_NAME = 'CSV_FORMAT') PATTERN = '.*airports[.]csv';

COPY INTO REF_AIRLINES_RAW FROM (
    SELECT $1, $2, $3, $4,
           METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, METADATA$FILE_LAST_MODIFIED, CURRENT_TIMESTAMP()
    FROM @RAW_STAGE/reference/)
FILE_FORMAT = (FORMAT_NAME = 'CSV_FORMAT') PATTERN = '.*airlines[.]csv';

COPY INTO REF_AIRCRAFT_RAW FROM (
    SELECT $1, $2, $3, $4, $5, $6, $7, $8,
           METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, METADATA$FILE_LAST_MODIFIED, CURRENT_TIMESTAMP()
    FROM @RAW_STAGE/reference/)
FILE_FORMAT = (FORMAT_NAME = 'CSV_FORMAT') PATTERN = '.*aircraft[.]csv';

COPY INTO REF_ROUTES_RAW FROM (
    SELECT $1, $2, $3, $4, $5, $6,
           METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, METADATA$FILE_LAST_MODIFIED, CURRENT_TIMESTAMP()
    FROM @RAW_STAGE/reference/)
FILE_FORMAT = (FORMAT_NAME = 'CSV_FORMAT') PATTERN = '.*routes[.]csv';

COPY INTO REF_MIN_CONNECTION_TIMES_RAW FROM (
    SELECT $1, $2, $3,
           METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, METADATA$FILE_LAST_MODIFIED, CURRENT_TIMESTAMP()
    FROM @RAW_STAGE/reference/)
FILE_FORMAT = (FORMAT_NAME = 'CSV_FORMAT') PATTERN = '.*min_connection_times[.]csv';

COPY INTO REF_SUPPLIERS_RAW FROM (
    SELECT $1, $2, $3, $4, $5,
           METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, METADATA$FILE_LAST_MODIFIED, CURRENT_TIMESTAMP()
    FROM @RAW_STAGE/reference/)
FILE_FORMAT = (FORMAT_NAME = 'CSV_FORMAT') PATTERN = '.*suppliers[.]csv';

COPY INTO REF_PARTS_RAW FROM (
    SELECT $1, $2, $3, $4, $5, $6, $7, $8,
           METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, METADATA$FILE_LAST_MODIFIED, CURRENT_TIMESTAMP()
    FROM @RAW_STAGE/reference/)
FILE_FORMAT = (FORMAT_NAME = 'CSV_FORMAT') PATTERN = '.*parts[.]csv';


-- =====================================================
-- 2. FLIGHT OPERATIONS SYSTEM - hourly JSON Lines -> VARIANT
-- =====================================================
CREATE TABLE IF NOT EXISTS FOS_MESSAGES_RAW (
    raw                  VARIANT,          -- the whole JSON message, untouched
    _source_file STRING, _source_row NUMBER, _file_last_modified TIMESTAMP_NTZ, _loaded_at TIMESTAMP_NTZ)
COMMENT = 'BRONZE: flight operations messages as received (JSON). Grain: one row per message line.';

COPY INTO FOS_MESSAGES_RAW FROM (
    SELECT $1,
           METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, METADATA$FILE_LAST_MODIFIED, CURRENT_TIMESTAMP()
    FROM @RAW_STAGE/raw/flight_ops/)
FILE_FORMAT = (FORMAT_NAME = 'JSON_FORMAT') PATTERN = '.*fos_.*[.]jsonl';


-- =====================================================
-- 3. BAGGAGE SYSTEMS - gzip JSON Lines (one file per airport per day) -> VARIANT
-- =====================================================
CREATE TABLE IF NOT EXISTS BAG_SCANS_RAW (
    raw                  VARIANT,
    _source_file STRING, _source_row NUMBER, _file_last_modified TIMESTAMP_NTZ, _loaded_at TIMESTAMP_NTZ)
COMMENT = 'BRONZE: bag scan events as received (JSON). Grain: one row per scan line, duplicates included.';

COPY INTO BAG_SCANS_RAW FROM (
    SELECT $1,
           METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, METADATA$FILE_LAST_MODIFIED, CURRENT_TIMESTAMP()
    FROM @RAW_STAGE/raw/baggage/)
FILE_FORMAT = (FORMAT_NAME = 'JSON_FORMAT') PATTERN = '.*bag_scans_.*[.]jsonl[.]gz';


-- =====================================================
-- 4. MAINTENANCE (MRO) - daily CSV change records (already created in the first exercise)
-- =====================================================
CREATE TABLE IF NOT EXISTS MRO_DEFECTS_RAW (
    fault_ref STRING, tail_number STRING, station STRING, reported_at STRING, ata_chapter STRING,
    part_no STRING, defect_description STRING, mel_category STRING, status STRING, part_source STRING,
    completed_at STRING, record_updated_at STRING,
    _source_file STRING, _source_row NUMBER, _file_last_modified TIMESTAMP_NTZ, _loaded_at TIMESTAMP_NTZ)
COMMENT = 'BRONZE: maintenance defect change-records exactly as received. Grain: one row per file line.';

COPY INTO MRO_DEFECTS_RAW FROM (
    SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
           METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, METADATA$FILE_LAST_MODIFIED, CURRENT_TIMESTAMP()
    FROM @RAW_STAGE/raw/maintenance/)
FILE_FORMAT = (FORMAT_NAME = 'CSV_FORMAT') PATTERN = '.*mro_defects_.*[.]csv';


-- =====================================================
-- 5. INVENTORY SYSTEM - daily CSV of hourly snapshots
-- =====================================================
CREATE TABLE IF NOT EXISTS INVENTORY_SNAPSHOTS_RAW (
    snapshot_utc STRING, airline_code STRING, airport_code STRING, part_number STRING,
    quantity_on_hand STRING,
    _source_file STRING, _source_row NUMBER, _file_last_modified TIMESTAMP_NTZ, _loaded_at TIMESTAMP_NTZ)
COMMENT = 'BRONZE: inventory snapshots as received. Grain: one row per file line.';

COPY INTO INVENTORY_SNAPSHOTS_RAW FROM (
    SELECT $1, $2, $3, $4, $5,
           METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, METADATA$FILE_LAST_MODIFIED, CURRENT_TIMESTAMP()
    FROM @RAW_STAGE/raw/supply_chain/inventory/)
FILE_FORMAT = (FORMAT_NAME = 'CSV_FORMAT') PATTERN = '.*inventory_.*[.]csv';


-- =====================================================
-- 6. RESERVATIONS + DEPARTURE CONTROL - daily CSV (synthetic personal data)
-- =====================================================
CREATE TABLE IF NOT EXISTS RES_BOOKINGS_RAW (
    booking_ref STRING, passenger_id STRING, passenger_name STRING, email STRING,
    frequent_flyer_tier STRING, cabin STRING, airline STRING, origin STRING, destination STRING,
    is_connecting STRING,
    _source_file STRING, _source_row NUMBER, _file_last_modified TIMESTAMP_NTZ, _loaded_at TIMESTAMP_NTZ)
COMMENT = 'BRONZE: bookings as received. Contains (synthetic) personal data. Grain: one row per file line.';

CREATE TABLE IF NOT EXISTS RES_SEGMENTS_RAW (
    booking_ref STRING, segment_no STRING, flight_number STRING, flight_date_local STRING,
    origin STRING, destination STRING,
    _source_file STRING, _source_row NUMBER, _file_last_modified TIMESTAMP_NTZ, _loaded_at TIMESTAMP_NTZ)
COMMENT = 'BRONZE: planned booking segments as received. Grain: one row per file line.';

CREATE TABLE IF NOT EXISTS DCS_BOARDED_RAW (
    booking_ref STRING, flight_number STRING, flight_date_local STRING, origin STRING,
    boarded_at_utc STRING,
    _source_file STRING, _source_row NUMBER, _file_last_modified TIMESTAMP_NTZ, _loaded_at TIMESTAMP_NTZ)
COMMENT = 'BRONZE: departure control boarding records as received. Grain: one row per file line.';

COPY INTO RES_BOOKINGS_RAW FROM (
    SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
           METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, METADATA$FILE_LAST_MODIFIED, CURRENT_TIMESTAMP()
    FROM @RAW_STAGE/raw/reservations/)
FILE_FORMAT = (FORMAT_NAME = 'CSV_FORMAT') PATTERN = '.*bookings_.*[.]csv';

COPY INTO RES_SEGMENTS_RAW FROM (
    SELECT $1, $2, $3, $4, $5, $6,
           METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, METADATA$FILE_LAST_MODIFIED, CURRENT_TIMESTAMP()
    FROM @RAW_STAGE/raw/reservations/)
FILE_FORMAT = (FORMAT_NAME = 'CSV_FORMAT') PATTERN = '.*segments_.*[.]csv';

COPY INTO DCS_BOARDED_RAW FROM (
    SELECT $1, $2, $3, $4, $5,
           METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, METADATA$FILE_LAST_MODIFIED, CURRENT_TIMESTAMP()
    FROM @RAW_STAGE/raw/departure_control/)
FILE_FORMAT = (FORMAT_NAME = 'CSV_FORMAT') PATTERN = '.*boarded_.*[.]csv';


-- =====================================================
-- 7. EXTERNAL WEATHER SERVICE - one JSON ARRAY per file -> one row per alert
-- =====================================================
CREATE TABLE IF NOT EXISTS WEATHER_ALERTS_RAW (
    raw                  VARIANT,
    _source_file STRING, _source_row NUMBER, _file_last_modified TIMESTAMP_NTZ, _loaded_at TIMESTAMP_NTZ)
COMMENT = 'BRONZE: external weather alerts as received (JSON). Grain: one row per alert.';

COPY INTO WEATHER_ALERTS_RAW FROM (
    SELECT $1,
           METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, METADATA$FILE_LAST_MODIFIED, CURRENT_TIMESTAMP()
    FROM @RAW_STAGE/raw/external/weather/)
FILE_FORMAT = (FORMAT_NAME = 'JSON_ARRAY_FORMAT') PATTERN = '.*weather_alerts_.*[.]json';


-- =====================================================
-- 8. CONTROL: the delivery log (what each source says it sent)
-- =====================================================
CREATE TABLE IF NOT EXISTS CTRL_DELIVERY_LOG_RAW (
    file_path STRING, source_system STRING, delivered_utc STRING, record_count STRING,
    _source_file STRING, _source_row NUMBER, _file_last_modified TIMESTAMP_NTZ, _loaded_at TIMESTAMP_NTZ)
COMMENT = 'BRONZE control: one row per delivered source file, with the record count the source declared.';

COPY INTO CTRL_DELIVERY_LOG_RAW FROM (
    SELECT $1, $2, $3, $4,
           METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, METADATA$FILE_LAST_MODIFIED, CURRENT_TIMESTAMP()
    FROM @RAW_STAGE/raw/_control/)
FILE_FORMAT = (FORMAT_NAME = 'CSV_FORMAT') PATTERN = '.*delivery_log[.]csv';


-- =====================================================
-- 9. RECONCILIATION: rows loaded per file vs records the source declared
-- =====================================================
WITH loaded AS (
              SELECT _source_file, COUNT(*) AS rows_loaded FROM FOS_MESSAGES_RAW        GROUP BY 1
    UNION ALL SELECT _source_file, COUNT(*) FROM BAG_SCANS_RAW                          GROUP BY 1
    UNION ALL SELECT _source_file, COUNT(*) FROM MRO_DEFECTS_RAW                        GROUP BY 1
    UNION ALL SELECT _source_file, COUNT(*) FROM INVENTORY_SNAPSHOTS_RAW                GROUP BY 1
    UNION ALL SELECT _source_file, COUNT(*) FROM RES_BOOKINGS_RAW                       GROUP BY 1
    UNION ALL SELECT _source_file, COUNT(*) FROM RES_SEGMENTS_RAW                       GROUP BY 1
    UNION ALL SELECT _source_file, COUNT(*) FROM DCS_BOARDED_RAW                        GROUP BY 1
    UNION ALL SELECT _source_file, COUNT(*) FROM WEATHER_ALERTS_RAW                     GROUP BY 1
)
SELECT d.source_system,
       COUNT(*)                                                         AS files_declared,
       COUNT(l._source_file)                                            AS files_loaded,
       SUM(d.record_count::NUMBER)                                      AS records_declared,
       SUM(COALESCE(l.rows_loaded, 0))                                  AS rows_loaded,
       SUM(IFF(l.rows_loaded = d.record_count::NUMBER, 0, 1))           AS files_mismatched
FROM CTRL_DELIVERY_LOG_RAW d
LEFT JOIN loaded l
       ON l._source_file = 'raw/' || d.file_path
GROUP BY d.source_system
ORDER BY d.source_system;


-- =====================================================
-- 10. PLANNING SYSTEM - published flight schedule (added for SILVER key resolution)
--     Raw operational feeds identify flights by number + local date + origin;
--     the published schedule is the master list that maps those to one flight.
-- =====================================================
CREATE TABLE IF NOT EXISTS SCHED_FLIGHTS_RAW (
    flight_id STRING, flight_number STRING, airline_code STRING, route_id STRING,
    origin_airport_code STRING, destination_airport_code STRING, aircraft_registration STRING,
    rotation_seq STRING, previous_flight_id STRING,
    scheduled_departure_utc STRING, scheduled_arrival_utc STRING,
    scheduled_departure_local STRING, scheduled_arrival_local STRING, block_time_minutes STRING,
    _source_file STRING, _source_row NUMBER, _file_last_modified TIMESTAMP_NTZ, _loaded_at TIMESTAMP_NTZ)
COMMENT = 'BRONZE: published flight schedule from the planning system. Grain: one row per file line.';

COPY INTO SCHED_FLIGHTS_RAW FROM (
    SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14,
           METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, METADATA$FILE_LAST_MODIFIED, CURRENT_TIMESTAMP()
    FROM @RAW_STAGE/truth/)
FILE_FORMAT = (FORMAT_NAME = 'CSV_FORMAT') PATTERN = '.*flights_schedule[.]csv';

SELECT COUNT(*) AS schedule_rows FROM SCHED_FLIGHTS_RAW;   -- expected 472