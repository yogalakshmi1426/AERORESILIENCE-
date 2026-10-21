-- =====================================================
-- AERORESILIENCE - 21_silver_tests.sql
-- Data-quality tests for SILVER. Every expected value is known exactly, because the
-- source data is deterministic (same seed -> same data) and every corruption is in the manifest.
-- Run the first query: every row should say PASS.
-- =====================================================

USE WAREHOUSE AERO_WH;
USE SCHEMA AERORESILIENCE.SILVER;

WITH checks AS (
    -- ---------- reference data: one row per key ----------
              SELECT 'airports: rows'                 AS check_name, '10'    AS expected, (SELECT COUNT(*) FROM AIRPORTS)::STRING AS actual
    UNION ALL SELECT 'airlines: rows',                   '4',     (SELECT COUNT(*) FROM AIRLINES)::STRING
    UNION ALL SELECT 'aircraft: rows',                   '20',    (SELECT COUNT(*) FROM AIRCRAFT)::STRING
    UNION ALL SELECT 'routes: rows',                     '52',    (SELECT COUNT(*) FROM ROUTES)::STRING
    UNION ALL SELECT 'min connection times: rows',       '16',    (SELECT COUNT(*) FROM MIN_CONNECTION_TIMES)::STRING
    UNION ALL SELECT 'suppliers: rows',                  '6',     (SELECT COUNT(*) FROM SUPPLIERS)::STRING
    UNION ALL SELECT 'parts: rows',                      '55',    (SELECT COUNT(*) FROM PARTS)::STRING

    -- ---------- flights ----------
    UNION ALL SELECT 'flight schedule: rows',            '472',   (SELECT COUNT(*) FROM FLIGHT_SCHEDULE)::STRING
    UNION ALL SELECT 'flight schedule: number + origin + LOCAL date is unique (business key)', '0',
              (SELECT COUNT(*) FROM (SELECT flight_number, origin_airport_code, DATE(scheduled_departure_local)
                                     FROM FLIGHT_SCHEDULE GROUP BY 1, 2, 3 HAVING COUNT(*) > 1))::STRING
    UNION ALL SELECT 'flight events: rows after BOTH de-dups (1253 raw -> 1227 ids -> 1223 events)',
                                                         '1223',  (SELECT COUNT(*) FROM FLIGHT_EVENTS)::STRING
    UNION ALL SELECT 'flight events: unresolved flight_id', '0',  (SELECT COUNT(*) FROM FLIGHT_EVENTS WHERE flight_id IS NULL)::STRING
    UNION ALL SELECT 'flights: rows',                    '472',   (SELECT COUNT(*) FROM FLIGHTS)::STRING
    UNION ALL SELECT 'flights: cancelled',               '2',     (SELECT COUNT(*) FROM FLIGHTS WHERE flight_status = 'CANCELLED')::STRING
    UNION ALL SELECT 'flights: missing actuals',         '0',     (SELECT COUNT(*) FROM FLIGHTS WHERE flight_status = 'NO_DATA')::STRING
    UNION ALL SELECT 'flights: on-time % matches truth', '83.8',
              (SELECT ROUND(100 * SUM(IFF(flight_status = 'ON_TIME', 1, 0))
                      / NULLIF(SUM(IFF(flight_status IN ('ON_TIME', 'DELAYED'), 1, 0)), 0), 1) FROM FLIGHTS)::STRING
    UNION ALL SELECT 'flight delays: rows',              '165',   (SELECT COUNT(*) FROM FLIGHT_DELAYS)::STRING
    UNION ALL SELECT 'flight delays: codes do not add up to the delay', '0',
              (SELECT COUNT(*) FROM FLIGHTS f
               LEFT JOIN (SELECT flight_id, SUM(delay_minutes) AS m FROM FLIGHT_DELAYS GROUP BY 1) d
                      ON d.flight_id = f.flight_id
               WHERE f.flight_status IN ('ON_TIME', 'DELAYED')
                 AND COALESCE(d.m, 0) <> f.departure_delay_minutes)::STRING

    -- ---------- bag scans ----------
    UNION ALL SELECT 'bag scans: rows (503206 raw - duplicates)', '468261', (SELECT COUNT(*) FROM BAG_SCANS)::STRING
    UNION ALL SELECT 'bag scans: duplicate scan_id',     '0',
              (SELECT COUNT(*) - COUNT(DISTINCT scan_id) FROM BAG_SCANS)::STRING
    UNION ALL SELECT 'bag scans: unparseable times',     '0',     (SELECT COUNT(*) FROM BAG_SCANS WHERE event_time_utc IS NULL)::STRING
    UNION ALL SELECT 'bag scans: local times converted to UTC', '86149',
              (SELECT COUNT(*) FROM BAG_SCANS WHERE time_was_local)::STRING
    UNION ALL SELECT 'bag scans: LOADED after the aircraft left (impossible)', '0',
              (SELECT COUNT(*) FROM BAG_SCANS b JOIN FLIGHTS f ON f.flight_id = b.flight_id
               WHERE b.event_type = 'LOADED' AND b.event_time_utc >= f.actual_departure_utc)::STRING

    -- ---------- maintenance + supply chain ----------
    UNION ALL SELECT 'maintenance: one row per defect',  '15',    (SELECT COUNT(*) FROM MAINTENANCE_EVENTS)::STRING
    UNION ALL SELECT 'maintenance: unrecognised MEL category', '0', (SELECT COUNT(*) FROM MAINTENANCE_EVENTS WHERE mel_category IS NULL)::STRING
    UNION ALL SELECT 'maintenance: station not a known airport', '0',
              (SELECT COUNT(*) FROM MAINTENANCE_EVENTS m LEFT JOIN AIRPORTS a ON a.airport_code = m.airport_code
               WHERE a.airport_code IS NULL)::STRING
    UNION ALL SELECT 'maintenance: missing description', '0',     (SELECT COUNT(*) FROM MAINTENANCE_EVENTS WHERE defect_description IS NULL)::STRING
    -- 6 shipments exist in the truth, but SH-0005 is only dispatched on 30 Oct, AFTER the data
    -- period ends -> the supplier feed has never mentioned it (period-boundary effect)
    UNION ALL SELECT 'supplier shipments: one row per shipment seen in the feed', '5', (SELECT COUNT(*) FROM SUPPLIER_SHIPMENTS)::STRING
    UNION ALL SELECT 'supplier shipments: missing part number (Incident B)', '0',
              (SELECT COUNT(*) FROM SUPPLIER_SHIPMENTS WHERE part_number IS NULL)::STRING
    UNION ALL SELECT 'purchase orders: rows',            '4',     (SELECT COUNT(*) FROM PURCHASE_ORDERS)::STRING
    UNION ALL SELECT 'purchase orders: missing part number', '0', (SELECT COUNT(*) FROM PURCHASE_ORDERS WHERE part_number IS NULL)::STRING
    UNION ALL SELECT 'inventory: rows (resent file removed)', '18480', (SELECT COUNT(*) FROM INVENTORY_SNAPSHOTS)::STRING

    -- ---------- passengers ----------
    UNION ALL SELECT 'passengers: rows',                 '81439', (SELECT COUNT(*) FROM PASSENGERS)::STRING
    UNION ALL SELECT 'passengers: emails not lower case', '0',    (SELECT COUNT(*) FROM PASSENGERS WHERE email <> LOWER(email))::STRING
    UNION ALL SELECT 'passengers: names with extra spaces', '0',
              (SELECT COUNT(*) FROM PASSENGERS WHERE passenger_name LIKE '%  %' OR passenger_name <> TRIM(passenger_name))::STRING
    UNION ALL SELECT 'booking segments: rows',           '99565', (SELECT COUNT(*) FROM BOOKING_SEGMENTS)::STRING
    UNION ALL SELECT 'booking segments: unresolved flight', '0',  (SELECT COUNT(*) FROM BOOKING_SEGMENTS WHERE planned_flight_id IS NULL)::STRING
    UNION ALL SELECT 'boardings: rows',                  '99469', (SELECT COUNT(*) FROM BOARDINGS)::STRING
    UNION ALL SELECT 'boardings: unresolved flight',     '0',     (SELECT COUNT(*) FROM BOARDINGS WHERE flight_id IS NULL)::STRING
    UNION ALL SELECT 'weather alerts: rows',             '4',     (SELECT COUNT(*) FROM WEATHER_ALERTS)::STRING
)
SELECT IFF(expected = actual, 'PASS', 'FAIL') AS result, check_name, expected, actual
FROM checks
ORDER BY result, check_name;      -- any FAIL appears at the top


-- =====================================================
-- WHY THE DUBAI FIX MATTERS: what if we had trusted the 'event_time_utc' label?
-- Counts bags that would appear to be loaded AFTER their flight had already left.
-- =====================================================
SELECT COUNT(*) AS impossible_loads_if_label_trusted
FROM BAG_SCANS b
JOIN FLIGHTS f ON f.flight_id = b.flight_id
WHERE b.time_was_local
  AND b.event_type = 'LOADED'
  AND TRY_TO_TIMESTAMP_NTZ(b.event_time_raw, 'YYYY-MM-DD HH24:MI:SS') >= f.actual_departure_utc;