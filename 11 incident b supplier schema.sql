-- =====================================================
-- AERORESILIENCE - 11_incident_b_supplier_schema.sql
-- INCIDENT B: the supplier portal changed its file layout on 2026-10-25
--   part_number -> component_id   and a new column carrier_name
-- Worked like a production incident: DETECT -> IMPACT -> FIX -> PREVENT -> WRITE UP
-- Run one phase at a time and read each result before moving on.
-- =====================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE AERO_WH;
USE SCHEMA AERORESILIENCE.BRONZE;


-- =====================================================
-- PHASE 1 - DETECT: what does the header of every supplier file look like?
-- CSV_LINE_FORMAT reads each line as one value, so line 1 = the header text.
-- =====================================================
SELECT METADATA$FILENAME AS file_name,
       $1                AS header_line
FROM @RAW_STAGE/raw/supply_chain/
     (FILE_FORMAT => 'CSV_LINE_FORMAT', PATTERN => '.*supplier_.*[.]csv')
WHERE METADATA$FILE_ROW_NUMBER = 1
ORDER BY file_name;

-- Group identical headers: one row per distinct layout ("schema signature")
SELECT $1                       AS header_line,
       COUNT(*)                 AS files,
       MIN(METADATA$FILENAME)   AS first_file,
       MAX(METADATA$FILENAME)   AS last_file
FROM @RAW_STAGE/raw/supply_chain/
     (FILE_FORMAT => 'CSV_LINE_FORMAT', PATTERN => '.*supplier_.*[.]csv')
WHERE METADATA$FILE_ROW_NUMBER = 1
GROUP BY 1
ORDER BY first_file;


-- =====================================================
-- PHASE 2 - IMPACT: which records are affected?
-- (rows in files from 2026-10-25 onwards use the new layout)
-- =====================================================
SELECT METADATA$FILENAME AS file_name, COUNT(*) - 1 AS data_rows      -- minus the header line
FROM @RAW_STAGE/raw/supply_chain/
     (FILE_FORMAT => 'CSV_LINE_FORMAT', PATTERN => '.*supplier_.*[.]csv')
GROUP BY 1
ORDER BY 1;


-- =====================================================
-- PHASE 3 - FIX: load by column NAME, and let the table evolve
-- =====================================================
CREATE TABLE IF NOT EXISTS SUPPLIER_SHIPMENTS_RAW (
    shipment_ref STRING, po_ref STRING, part_number STRING, quantity STRING, ship_from STRING,
    ship_to STRING, status STRING, dispatched_at STRING, promised_at STRING, delivered_at STRING,
    record_updated_at STRING,
    _source_file STRING, _source_row NUMBER, _file_last_modified TIMESTAMP_NTZ,
    _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
ENABLE_SCHEMA_EVOLUTION = TRUE      -- new columns in the files are ADDED to the table, not dropped
COMMENT = 'BRONZE: supplier shipment change-records as received, loaded BY COLUMN NAME (schema can drift).';

CREATE TABLE IF NOT EXISTS SUPPLIER_POS_RAW (
    po_ref STRING, order_type STRING, supplier_id STRING, part_number STRING, airline STRING,
    deliver_to STRING, quantity STRING, ordered_at STRING, promised_at STRING,
    _source_file STRING, _source_row NUMBER, _file_last_modified TIMESTAMP_NTZ,
    _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
ENABLE_SCHEMA_EVOLUTION = TRUE
COMMENT = 'BRONZE: supplier purchase orders as received, loaded BY COLUMN NAME (schema can drift).';

COPY INTO SUPPLIER_SHIPMENTS_RAW
FROM @RAW_STAGE/raw/supply_chain/shipments/
FILE_FORMAT = (FORMAT_NAME = 'CSV_PARSE_HEADER_FORMAT')
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE           -- header name -> column with the same name
INCLUDE_METADATA = (_source_file = METADATA$FILENAME,
                    _source_row = METADATA$FILE_ROW_NUMBER,
                    _file_last_modified = METADATA$FILE_LAST_MODIFIED)
PATTERN = '.*supplier_shipments_.*[.]csv';

COPY INTO SUPPLIER_POS_RAW
FROM @RAW_STAGE/raw/supply_chain/purchase_orders/
FILE_FORMAT = (FORMAT_NAME = 'CSV_PARSE_HEADER_FORMAT')
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
INCLUDE_METADATA = (_source_file = METADATA$FILENAME,
                    _source_row = METADATA$FILE_ROW_NUMBER,
                    _file_last_modified = METADATA$FILE_LAST_MODIFIED)
PATTERN = '.*supplier_purchase_orders_.*[.]csv';

-- Did the tables grow new columns?  Look for COMPONENT_ID and CARRIER_NAME at the bottom.
DESCRIBE TABLE SUPPLIER_SHIPMENTS_RAW;

-- Old-layout rows have part_number; new-layout rows have component_id. Nothing was lost.
SELECT _source_file, shipment_ref, status, part_number, component_id, carrier_name
FROM SUPPLIER_SHIPMENTS_RAW
ORDER BY _source_file, _source_row;


-- =====================================================
-- PHASE 4 - PREVENT: a drift check you can run every day (and later automate)
-- Any file whose header differs from the most common layout before it is flagged.
-- =====================================================
WITH headers AS (
    SELECT METADATA$FILENAME AS file_name,
           REGEXP_SUBSTR(METADATA$FILENAME, 'date=([0-9-]+)', 1, 1, 'e') AS file_date,
           SPLIT_PART(METADATA$FILENAME, '/', 3) AS feed,
           $1 AS header_line
    FROM @RAW_STAGE/raw/supply_chain/
         (FILE_FORMAT => 'CSV_LINE_FORMAT', PATTERN => '.*supplier_.*[.]csv')
    WHERE METADATA$FILE_ROW_NUMBER = 1
)
, compared AS (
    SELECT feed, file_date, file_name, header_line,
           LAG(header_line) OVER (PARTITION BY feed ORDER BY file_date) AS previous_header
    FROM headers
)
SELECT feed, file_date, file_name,
       IFF(previous_header IS NOT NULL AND header_line <> previous_header,
           'SCHEMA DRIFT', 'ok')                                      AS drift_status,
       header_line, previous_header
FROM compared
ORDER BY feed, file_date;