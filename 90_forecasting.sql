-- =====================================================
-- AERORESILIENCE - 90_forecasting.sql      (V5: forecasting with Snowflake ML)
-- Forecast the next 24 hours of bag-scan volume at the four hubs.
-- Honest limitation: one week of synthetic history -> a demonstration of the method,
-- not a production-grade model (real use needs months of history).
-- Run one section at a time. Role ACCOUNTADMIN, warehouse AERO_WH.
-- =====================================================
USE ROLE ACCOUNTADMIN;
USE WAREHOUSE AERO_WH;

CREATE SCHEMA IF NOT EXISTS AERORESILIENCE.ML;

-- 1. Training data: hourly scans per hub (series = airport)
CREATE OR REPLACE VIEW AERORESILIENCE.ML.V_HOURLY_HUB_SCANS AS
SELECT airport_code                                    AS airport,
       DATE_TRUNC('hour', event_time_utc)::TIMESTAMP_NTZ AS ts,
       COUNT(*)::FLOAT                                  AS scans
FROM AERORESILIENCE.DBT_DEV_INTERMEDIATE.INT_BAG_SCANS
WHERE airport_code IN ('LHR', 'DXB', 'DOH', 'AMS')
GROUP BY 1, 2;

SELECT airport, COUNT(*) AS hours, MIN(ts), MAX(ts) FROM AERORESILIENCE.ML.V_HOURLY_HUB_SCANS GROUP BY 1;

-- 2. Train one model per airport series
CREATE OR REPLACE SNOWFLAKE.ML.FORECAST AERORESILIENCE.ML.HUB_SCAN_FORECAST(
    INPUT_DATA        => TABLE(AERORESILIENCE.ML.V_HOURLY_HUB_SCANS),
    SERIES_COLNAME    => 'airport',
    TIMESTAMP_COLNAME => 'ts',
    TARGET_COLNAME    => 'scans'
);

-- 3. Predict the next 24 hours (with an uncertainty range)
CALL AERORESILIENCE.ML.HUB_SCAN_FORECAST!FORECAST(FORECASTING_PERIODS => 24);

CREATE OR REPLACE TABLE AERORESILIENCE.ML.HUB_SCAN_FORECAST_RESULTS AS
SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

SELECT * FROM AERORESILIENCE.ML.HUB_SCAN_FORECAST_RESULTS ORDER BY series, ts;

-- 4. What drives the model (feature importance)
CALL AERORESILIENCE.ML.HUB_SCAN_FORECAST!EXPLAIN_FEATURE_IMPORTANCE();
ALTER TASK AERORESILIENCE.ORCHESTRATION.T1_LOAD_BRONZE SUSPEND;
ALTER TASK AERORESILIENCE.REALTIME.T_BAG_ALERTS SUSPEND;
SHOW TASKS IN ACCOUNT;      -- every task should say "suspended"