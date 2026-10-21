-- =====================================================
-- AERORESILIENCE - 70_realtime_alerts.sql        (V3: near-real-time events + alerts)
--
-- A live bag-event feed lands in LIVE_BAG_EVENTS one event at a time.
-- A STREAM tracks new rows (change data capture). A TASK checks every minute and,
-- only WHEN the stream has data, turns action-needed events into OPS_ALERTS.
-- Separate from BRONZE on purpose: demo events never disturb the tested pipeline.
-- Run one section at a time. Role ACCOUNTADMIN, warehouse AERO_WH.
-- =====================================================
USE ROLE ACCOUNTADMIN;
USE WAREHOUSE AERO_WH;

-- 1. Schema, live feed table, alerts table
CREATE SCHEMA IF NOT EXISTS AERORESILIENCE.REALTIME;

CREATE TABLE IF NOT EXISTS AERORESILIENCE.REALTIME.LIVE_BAG_EVENTS (
    raw          VARIANT,
    received_at  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
) COMMENT = 'Simulated live bag-scan feed: one row per event as it happens.';

CREATE TABLE IF NOT EXISTS AERORESILIENCE.REALTIME.OPS_ALERTS (
    alert_time    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    alert_type    STRING,
    airport_code  STRING,
    flight_id     STRING,
    bag_tag       STRING,
    detail        STRING
) COMMENT = 'Operational alerts raised automatically from the live feed.';

-- 2. Stream = a bookmark: "which rows are new since I last consumed?"
CREATE OR REPLACE STREAM AERORESILIENCE.REALTIME.LIVE_BAG_EVENTS_STREAM
  ON TABLE AERORESILIENCE.REALTIME.LIVE_BAG_EVENTS
  APPEND_ONLY = TRUE;

-- 3. Task: every minute; skipped for free when the stream is empty (WHEN clause)
CREATE OR REPLACE TASK AERORESILIENCE.REALTIME.T_BAG_ALERTS
    WAREHOUSE = AERO_WH
    SCHEDULE  = '1 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('AERORESILIENCE.REALTIME.LIVE_BAG_EVENTS_STREAM')
AS
INSERT INTO AERORESILIENCE.REALTIME.OPS_ALERTS (alert_type, airport_code, flight_id, bag_tag, detail)
SELECT raw:event_type::STRING,
       raw:location.airport::STRING,
       raw:flight.flight_id::STRING,
       raw:bag.tag::STRING,
       'Action needed: bag ' || raw:bag.tag::STRING || ' ' || raw:event_type::STRING
FROM AERORESILIENCE.REALTIME.LIVE_BAG_EVENTS_STREAM
WHERE raw:event_type::STRING IN ('MISSED_CONNECTION', 'HELD_AWAITING_REBOOKING');
-- Reading the stream inside DML advances its bookmark: every event is processed exactly once.

ALTER TASK AERORESILIENCE.REALTIME.T_BAG_ALERTS RESUME;

-- 4. DEMO: three events arrive "live" (one normal, two needing action)
INSERT INTO AERORESILIENCE.REALTIME.LIVE_BAG_EVENTS (raw)
SELECT PARSE_JSON(column1) FROM VALUES
 ('{"scan_id":"LIVE-1","event_type":"LOADED","event_time_utc":"2026-10-28T08:00:00Z","bag":{"tag":"LIVE001"},"flight":{"flight_id":"DEMO-1"},"location":{"airport":"LHR"}}'),
 ('{"scan_id":"LIVE-2","event_type":"MISSED_CONNECTION","event_time_utc":"2026-10-28T08:05:00Z","bag":{"tag":"LIVE002"},"flight":{"flight_id":"DEMO-2"},"location":{"airport":"DXB"}}'),
 ('{"scan_id":"LIVE-3","event_type":"HELD_AWAITING_REBOOKING","event_time_utc":"2026-10-28T08:06:00Z","bag":{"tag":"LIVE003"},"flight":{"flight_id":"DEMO-2"},"location":{"airport":"DXB"}}');

SELECT SYSTEM$STREAM_HAS_DATA('AERORESILIENCE.REALTIME.LIVE_BAG_EVENTS_STREAM');   -- TRUE until the task runs

-- 5. Wait ~1-2 minutes, then: expect 2 alerts (LIVE002, LIVE003) and the stream empty again
SELECT * FROM AERORESILIENCE.REALTIME.OPS_ALERTS ORDER BY alert_time DESC;
SELECT SYSTEM$STREAM_HAS_DATA('AERORESILIENCE.REALTIME.LIVE_BAG_EVENTS_STREAM');   -- FALSE

-- 6. Save trial credits when finished
 ALTER TASK AERORESILIENCE.REALTIME.T_BAG_ALERTS SUSPEND;




USE ROLE ACCOUNTADMIN;
USE WAREHOUSE AERO_WH;

CREATE TABLE IF NOT EXISTS AERORESILIENCE.REALTIME.LIVE_BAG_EVENTS (
    raw          VARIANT,
    received_at  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS AERORESILIENCE.REALTIME.OPS_ALERTS (
    alert_time    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    alert_type    STRING,
    airport_code  STRING,
    flight_id     STRING,
    bag_tag       STRING,
    detail        STRING
);

CREATE OR REPLACE STREAM AERORESILIENCE.REALTIME.LIVE_BAG_EVENTS_STREAM
  ON TABLE AERORESILIENCE.REALTIME.LIVE_BAG_EVENTS
  APPEND_ONLY = TRUE;

SHOW TABLES IN SCHEMA AERORESILIENCE.REALTIME;