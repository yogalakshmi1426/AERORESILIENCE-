-- =====================================================
-- AERORESILIENCE - 00_setup.sql
-- Builds the Snowflake platform. Safe to re-run at any time:
-- every statement is idempotent (IF NOT EXISTS / SET to a fixed value).
-- Contains ONLY platform objects - no exploration queries.
-- =====================================================

-- 1. Admin role (needed to create warehouses, databases and account settings)
USE ROLE ACCOUNTADMIN;

-- 2. Compute
CREATE WAREHOUSE IF NOT EXISTS AERO_WH
  WAREHOUSE_SIZE = 'XSMALL'        -- smallest = cheapest; plenty for our data
  AUTO_SUSPEND = 60                -- switch off after 60 seconds idle
  AUTO_RESUME = TRUE               -- switch on automatically when a query runs
  INITIALLY_SUSPENDED = TRUE       -- don't burn credits until first used
  COMMENT = 'Compute for AERORESILIENCE development';

USE WAREHOUSE AERO_WH;

-- 3. Account time zone: show and calculate all timestamps in UTC
--    (Snowflake's default is America/Los_Angeles; our data is UTC)
ALTER ACCOUNT SET TIMEZONE = 'UTC';

-- 4. Database
CREATE DATABASE IF NOT EXISTS AERORESILIENCE
  COMMENT = 'Airline operations & supply chain control tower';

USE DATABASE AERORESILIENCE;

-- 5. Medallion layers
CREATE SCHEMA IF NOT EXISTS BRONZE COMMENT = 'Raw data as received from sources';
CREATE SCHEMA IF NOT EXISTS SILVER COMMENT = 'Cleaned, deduplicated, standardised data';
CREATE SCHEMA IF NOT EXISTS GOLD   COMMENT = 'Star schema: dimensions and facts';

-- 6. Landing area for files, with a directory table (an index of every file)
CREATE STAGE IF NOT EXISTS AERORESILIENCE.BRONZE.RAW_STAGE
  DIRECTORY = (ENABLE = TRUE)
  COMMENT = 'Landing area for raw source files before loading into BRONZE tables';

-- (for a stage created before this line existed: make sure the directory is on)
ALTER STAGE AERORESILIENCE.BRONZE.RAW_STAGE SET DIRECTORY = (ENABLE = TRUE);

-- 7. File formats: instructions for how to read each kind of file
CREATE FILE FORMAT IF NOT EXISTS AERORESILIENCE.BRONZE.CSV_FORMAT
  TYPE = CSV
  SKIP_HEADER = 1                         -- first row is column names, not data
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'      -- values containing commas are wrapped in quotes
  EMPTY_FIELD_AS_NULL = TRUE              -- empty values become NULL
  COMMENT = 'Standard CSV: header row, quoted fields';

CREATE FILE FORMAT IF NOT EXISTS AERORESILIENCE.BRONZE.JSON_FORMAT
  TYPE = JSON
  COMPRESSION = AUTO                      -- detects .gz and decompresses automatically
  COMMENT = 'JSON Lines, optionally gzip-compressed';

CREATE FILE FORMAT IF NOT EXISTS AERORESILIENCE.BRONZE.JSON_ARRAY_FORMAT
  TYPE = JSON
  STRIP_OUTER_ARRAY = TRUE                -- [ {...}, {...} ] -> one row per element
  COMPRESSION = AUTO
  COMMENT = 'A file containing one JSON array (e.g. the weather service)';

CREATE FILE FORMAT IF NOT EXISTS AERORESILIENCE.BRONZE.CSV_PARSE_HEADER_FORMAT
  TYPE = CSV
  PARSE_HEADER = TRUE                     -- read the header row as COLUMN NAMES (load by name)
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  EMPTY_FIELD_AS_NULL = TRUE
  ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE  -- required for name-based loading
  COMMENT = 'CSV loaded by column NAME - for sources whose layout can change';

CREATE FILE FORMAT IF NOT EXISTS AERORESILIENCE.BRONZE.CSV_LINE_FORMAT
  TYPE = CSV
  FIELD_DELIMITER = NONE                  -- do not split: each whole line is one value ($1)
  SKIP_HEADER = 0                         -- keep the header line (we want to inspect it)
  COMMENT = 'Reads each line of a file as a single text value - for inspecting headers';

-- 8. Verify what exists
SHOW SCHEMAS      IN DATABASE AERORESILIENCE;
SHOW STAGES       IN SCHEMA AERORESILIENCE.BRONZE;
SHOW FILE FORMATS IN SCHEMA AERORESILIENCE.BRONZE;