-- =====================================================
-- AERORESILIENCE - 20_silver.sql
-- SILVER = trustworthy data: typed, de-duplicated, standardised, consistent keys, all times UTC.
-- Built with CREATE OR REPLACE TABLE ... AS SELECT (full rebuild from BRONZE):
--   * idempotent - running it twice gives the same result
--   * late-data-proof - late files and backfills are simply included on the next run
--   * trade-off: recomputes everything each time (incremental models come in V2 with dbt)
-- Run All, top to bottom (later tables depend on earlier ones). Then run 21_silver_tests.sql.
-- =====================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE AERO_WH;
USE DATABASE AERORESILIENCE;
USE SCHEMA SILVER;


-- =====================================================
-- 1. REFERENCE DATA - typed, trimmed, one row per key
-- =====================================================
CREATE OR REPLACE TABLE AIRPORTS AS
SELECT UPPER(TRIM(airport_code))   AS airport_code,
       TRIM(airport_name)          AS airport_name,
       TRIM(city)                  AS city,
       UPPER(TRIM(country_code))   AS country_code,
       TRIM(timezone)              AS timezone
FROM BRONZE.REF_AIRPORTS_RAW
QUALIFY ROW_NUMBER() OVER (PARTITION BY UPPER(TRIM(airport_code)) ORDER BY _loaded_at DESC, _source_row) = 1;

CREATE OR REPLACE TABLE AIRLINES AS
SELECT UPPER(TRIM(airline_code)) AS airline_code, TRIM(airline_name) AS airline_name,
       UPPER(TRIM(country_code)) AS country_code, UPPER(TRIM(hub_airport_code)) AS hub_airport_code
FROM BRONZE.REF_AIRLINES_RAW
QUALIFY ROW_NUMBER() OVER (PARTITION BY UPPER(TRIM(airline_code)) ORDER BY _loaded_at DESC, _source_row) = 1;

CREATE OR REPLACE TABLE AIRCRAFT AS
SELECT UPPER(TRIM(registration)) AS registration, UPPER(TRIM(airline_code)) AS airline_code,
       UPPER(TRIM(icao_type)) AS icao_type, TRIM(manufacturer) AS manufacturer, TRIM(model) AS model,
       UPPER(TRIM(body_type)) AS body_type,
       TRY_TO_NUMBER(seats) AS seats, TRY_TO_NUMBER(year_built) AS year_built
FROM BRONZE.REF_AIRCRAFT_RAW
QUALIFY ROW_NUMBER() OVER (PARTITION BY UPPER(TRIM(registration)) ORDER BY _loaded_at DESC, _source_row) = 1;

CREATE OR REPLACE TABLE ROUTES AS
SELECT TRIM(route_id) AS route_id, UPPER(TRIM(airline_code)) AS airline_code,
       UPPER(TRIM(origin_airport_code)) AS origin_airport_code,
       UPPER(TRIM(destination_airport_code)) AS destination_airport_code,
       TRY_TO_NUMBER(block_time_minutes) AS block_time_minutes,
       UPPER(TRIM(required_body_type)) AS required_body_type
FROM BRONZE.REF_ROUTES_RAW
QUALIFY ROW_NUMBER() OVER (PARTITION BY TRIM(route_id) ORDER BY _loaded_at DESC, _source_row) = 1;

CREATE OR REPLACE TABLE MIN_CONNECTION_TIMES AS
SELECT UPPER(TRIM(airport_code)) AS airport_code, UPPER(TRIM(connection_type)) AS connection_type,
       TRY_TO_NUMBER(min_connection_minutes) AS min_connection_minutes
FROM BRONZE.REF_MIN_CONNECTION_TIMES_RAW
QUALIFY ROW_NUMBER() OVER (PARTITION BY UPPER(TRIM(airport_code)), UPPER(TRIM(connection_type))
                           ORDER BY _loaded_at DESC, _source_row) = 1;          -- composite key

CREATE OR REPLACE TABLE SUPPLIERS AS
SELECT TRIM(supplier_id) AS supplier_id, TRIM(supplier_name) AS supplier_name,
       UPPER(TRIM(country_code)) AS country_code, UPPER(TRIM(ship_from_airport_code)) AS ship_from_airport_code,
       TRY_TO_NUMBER(on_time_rate, 5, 2) AS on_time_rate
FROM BRONZE.REF_SUPPLIERS_RAW
QUALIFY ROW_NUMBER() OVER (PARTITION BY TRIM(supplier_id) ORDER BY _loaded_at DESC, _source_row) = 1;

CREATE OR REPLACE TABLE PARTS AS
SELECT TRIM(part_number) AS part_number, TRIM(description) AS description, TRIM(ata_chapter) AS ata_chapter,
       UPPER(TRIM(icao_type)) AS icao_type, UPPER(TRIM(criticality)) AS criticality,
       TRY_TO_NUMBER(unit_cost_gbp, 12, 2) AS unit_cost_gbp,
       TRY_TO_NUMBER(standard_lead_time_days) AS standard_lead_time_days, TRIM(supplier_id) AS supplier_id
FROM BRONZE.REF_PARTS_RAW
QUALIFY ROW_NUMBER() OVER (PARTITION BY TRIM(part_number) ORDER BY _loaded_at DESC, _source_row) = 1;


-- =====================================================
-- 2. FLIGHT SCHEDULE - the master list of flights (the PLAN)
-- =====================================================
CREATE OR REPLACE TABLE FLIGHT_SCHEDULE AS
SELECT flight_id, flight_number, airline_code, route_id,
       origin_airport_code, destination_airport_code,
       aircraft_registration                                                    AS planned_aircraft_registration,
       TRY_TO_NUMBER(rotation_seq)                                              AS rotation_seq,
       previous_flight_id                                                       AS planned_previous_flight_id,
       TRY_TO_TIMESTAMP_NTZ(scheduled_departure_utc, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS scheduled_departure_utc,
       TRY_TO_TIMESTAMP_NTZ(scheduled_arrival_utc,   'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS scheduled_arrival_utc,
       TRY_TO_TIMESTAMP_NTZ(scheduled_departure_local, 'YYYY-MM-DD HH24:MI')     AS scheduled_departure_local,
       TRY_TO_TIMESTAMP_NTZ(scheduled_arrival_local,   'YYYY-MM-DD HH24:MI')     AS scheduled_arrival_local,
       TRY_TO_NUMBER(block_time_minutes)                                        AS block_time_minutes
FROM BRONZE.SCHED_FLIGHTS_RAW
QUALIFY ROW_NUMBER() OVER (PARTITION BY flight_id ORDER BY _loaded_at DESC, _source_row) = 1;


-- =====================================================
-- 3. FLIGHT EVENTS - operational messages, de-duplicated TWICE, flight key derived
-- =====================================================
CREATE OR REPLACE TABLE FLIGHT_EVENTS AS
WITH parsed AS (
    SELECT raw:message_id::STRING                                                         AS message_id,
           raw:message_type::STRING                                                       AS message_type,
           TRY_TO_TIMESTAMP_NTZ(raw:event_time_utc::STRING, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS event_time_utc,
           TRY_TO_TIMESTAMP_NTZ(raw:sent_at_utc::STRING,    'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS sent_at_utc,
           raw:flight.flight_number::STRING                                               AS flight_number,
           TRY_TO_DATE(raw:flight.flight_date_local::STRING)                              AS flight_date_local,
           raw:flight.origin::STRING                                                      AS origin_airport_code,
           raw:flight.destination::STRING                                                 AS destination_airport_code,
           raw:payload                                                                    AS payload,
           _source_file, _source_row
    FROM BRONZE.FOS_MESSAGES_RAW
),
exact_dedup AS (            -- Incident D: the same message (same id) delivered more than once
    SELECT * FROM parsed
    QUALIFY ROW_NUMBER() OVER (PARTITION BY message_id ORDER BY sent_at_utc, _source_file, _source_row) = 1
),
semantic_dedup AS (         -- same EVENT re-sent with a NEW id: compare business content, not ids
    SELECT * FROM exact_dedup
    QUALIFY ROW_NUMBER() OVER (PARTITION BY flight_number, flight_date_local, origin_airport_code,
                                            message_type, event_time_utc, HASH(payload)
                               ORDER BY sent_at_utc, message_id) = 1
)
SELECT s.flight_id,                                   -- derived: number + origin + LOCAL date -> schedule
       e.message_id, e.message_type, e.event_time_utc, e.sent_at_utc,
       DATEDIFF('minute', e.event_time_utc, e.sent_at_utc) AS delivery_lag_minutes,
       e.flight_number, e.flight_date_local, e.origin_airport_code, e.destination_airport_code,
       e.payload, e._source_file
FROM semantic_dedup e
LEFT JOIN FLIGHT_SCHEDULE s
       ON s.flight_number = e.flight_number
      AND s.origin_airport_code = e.origin_airport_code
      AND DATE(s.scheduled_departure_local) = e.flight_date_local;


-- =====================================================
-- 4. FLIGHTS - one row per flight: plan + what actually happened (built from events)
-- =====================================================
CREATE OR REPLACE TABLE FLIGHTS AS
WITH actuals AS (
    SELECT flight_id,
           MAX(IFF(message_type = 'OUT',
               TRY_TO_TIMESTAMP_NTZ(payload:actual_departure_utc::STRING, 'YYYY-MM-DD"T"HH24:MI:SS"Z"'), NULL)) AS actual_departure_utc,
           MAX(IFF(message_type = 'IN',
               TRY_TO_TIMESTAMP_NTZ(payload:actual_arrival_utc::STRING, 'YYYY-MM-DD"T"HH24:MI:SS"Z"'), NULL))   AS actual_arrival_utc,
           MAX(IFF(message_type = 'OUT', payload:aircraft_registration::STRING, NULL))        AS actual_aircraft_registration,
           BOOLOR_AGG(message_type = 'CANCELLED')                                              AS is_cancelled,
           MAX(IFF(message_type = 'CANCELLED', payload:reason::STRING, NULL))                  AS cancellation_reason,
           BOOLOR_AGG(message_type = 'AIRCRAFT_CHANGE')                                        AS had_aircraft_change
    FROM FLIGHT_EVENTS
    WHERE flight_id IS NOT NULL
    GROUP BY flight_id
),
joined AS (
    SELECT s.*, a.actual_departure_utc, a.actual_arrival_utc, a.actual_aircraft_registration,
           COALESCE(a.is_cancelled, FALSE)        AS is_cancelled,
           a.cancellation_reason,
           COALESCE(a.had_aircraft_change, FALSE) AS had_aircraft_change
    FROM FLIGHT_SCHEDULE s
    LEFT JOIN actuals a ON a.flight_id = s.flight_id
)
SELECT *,
       DATEDIFF('minute', scheduled_departure_utc, actual_departure_utc) AS departure_delay_minutes,
       DATEDIFF('minute', scheduled_arrival_utc,   actual_arrival_utc)   AS arrival_delay_minutes,
       CASE WHEN is_cancelled                     THEN 'CANCELLED'
            WHEN actual_departure_utc IS NULL     THEN 'NO_DATA'
            WHEN DATEDIFF('minute', scheduled_departure_utc, actual_departure_utc) >= 15 THEN 'DELAYED'
            ELSE 'ON_TIME' END                                           AS flight_status,
       -- the flight that ACTUALLY brought this aircraft in (differs from the plan after tail swaps)
       IFF(actual_aircraft_registration IS NULL, NULL,
           LAG(flight_id) OVER (PARTITION BY actual_aircraft_registration
                                ORDER BY actual_departure_utc))          AS actual_previous_flight_id
FROM joined;


-- =====================================================
-- 5. FLIGHT DELAYS - one row per delay cause per flight (one-to-many)
-- =====================================================
CREATE OR REPLACE TABLE FLIGHT_DELAYS AS
SELECT flight_id,
       payload:delay_code::STRING     AS delay_code,
       CASE payload:delay_code::STRING
            WHEN '32' THEN 'Ramp handling: loading/unloading'
            WHEN '41' THEN 'Aircraft defect (technical)'
            WHEN '71' THEN 'Weather at departure airport'
            WHEN '81' THEN 'Air traffic flow restriction (en-route)'
            WHEN '93' THEN 'Reactionary: aircraft late from previous flight'
            ELSE 'Unknown code' END    AS delay_description,
       payload:delay_minutes::NUMBER  AS delay_minutes
FROM FLIGHT_EVENTS
WHERE message_type = 'DELAY' AND flight_id IS NOT NULL;


-- =====================================================
-- 6. BAG SCANS - de-duplicated, every time converted to TRUE UTC
-- =====================================================
CREATE OR REPLACE TABLE BAG_SCANS AS
WITH parsed AS (
    SELECT raw:scan_id::STRING            AS scan_id,
           raw:event_type::STRING         AS event_type,
           raw:event_time_utc::STRING     AS event_time_raw,
           raw:bag.tag::STRING            AS bag_tag,
           raw:bag.booking_ref::STRING    AS booking_ref,
           raw:bag.weight_kg::FLOAT       AS weight_kg,
           raw:flight.flight_id::STRING   AS flight_id,
           raw:location.airport::STRING   AS airport_code,
           raw:location.area::STRING      AS area,
           raw:source_system::STRING      AS source_system,
           TRY_TO_DATE(REGEXP_SUBSTR(_source_file, 'date=([0-9-]+)', 1, 1, 'e')) AS file_date,
           _source_file, _source_row
    FROM BRONZE.BAG_SCANS_RAW
),
dedup AS (                  -- Incident D: each scan repeated 2-5 times -> keep one
    SELECT * FROM parsed
    QUALIFY ROW_NUMBER() OVER (PARTITION BY scan_id ORDER BY _source_file, _source_row) = 1
),
timed AS (
    SELECT d.*,
           -- a real UTC value ends in 'Z'. No 'Z' = the airport's LOCAL clock time (DXB), whatever the label says
           IFF(d.event_time_raw LIKE '%Z',
               TRY_TO_TIMESTAMP_NTZ(d.event_time_raw, 'YYYY-MM-DD"T"HH24:MI:SS"Z"'),
               CONVERT_TIMEZONE(a.timezone, 'UTC', TRY_TO_TIMESTAMP_NTZ(d.event_time_raw, 'YYYY-MM-DD HH24:MI:SS'))
           )                                   AS event_time_utc,
           NOT (d.event_time_raw LIKE '%Z')     AS time_was_local
    FROM dedup d
    LEFT JOIN AIRPORTS a ON a.airport_code = d.airport_code
)
SELECT scan_id, bag_tag, booking_ref, weight_kg, event_type, event_time_utc, airport_code, area,
       flight_id, source_system, time_was_local, event_time_raw,
       file_date > DATE(DATEADD('minute', 5, event_time_utc)) AS received_late,   -- Incident E visibility
       _source_file
FROM timed;


-- =====================================================
-- 7. MAINTENANCE EVENTS - latest version per defect (CDC), codes standardised
-- =====================================================
CREATE OR REPLACE TABLE MAINTENANCE_EVENTS AS
WITH parsed AS (
    SELECT TRIM(fault_ref)                                               AS fault_ref,
           UPPER(TRIM(tail_number))                                      AS aircraft_registration,
           UPPER(TRIM(station))                                          AS airport_code,      -- ' lhr' -> 'LHR'
           TRY_TO_TIMESTAMP_NTZ(reported_at, 'DD/MM/YYYY HH24:MI')       AS reported_at_utc,   -- UK date format
           TRIM(ata_chapter)                                             AS ata_chapter,
           TRIM(part_no)                                                 AS part_number,
           NULLIF(TRIM(defect_description), '')                          AS defect_description_raw,
           CASE REGEXP_REPLACE(UPPER(mel_category), '[^A-Z]', '')         -- 'No-Go','NOGO' -> 'NOGO'
                WHEN 'NOGO' THEN 'NO_GO' WHEN 'GOIF' THEN 'GO_IF' WHEN 'GO' THEN 'GO'
           END                                                           AS mel_category,
           mel_category                                                  AS mel_category_raw,
           UPPER(TRIM(status))                                           AS status,
           NULLIF(TRIM(part_source), '')                                 AS part_source,
           TRY_TO_TIMESTAMP_NTZ(completed_at, 'DD/MM/YYYY HH24:MI')      AS completed_at_utc,
           TRY_TO_TIMESTAMP_NTZ(record_updated_at, 'DD/MM/YYYY HH24:MI') AS record_updated_at_utc,
           _source_file
    FROM BRONZE.MRO_DEFECTS_RAW
)
SELECT p.fault_ref, p.aircraft_registration, p.airport_code, p.reported_at_utc, p.ata_chapter,
       p.part_number,
       COALESCE(p.defect_description_raw, pt.description) AS defect_description,  -- fill gaps from catalogue
       p.mel_category, p.mel_category_raw, p.status, p.part_source, p.completed_at_utc,
       DATEDIFF('minute', p.reported_at_utc, p.completed_at_utc) AS minutes_to_repair,
       p.record_updated_at_utc,
       COUNT(*) OVER (PARTITION BY p.fault_ref)               AS versions_received,
       p._source_file
FROM parsed p
LEFT JOIN PARTS pt ON pt.part_number = p.part_number
QUALIFY ROW_NUMBER() OVER (PARTITION BY p.fault_ref
                           ORDER BY p.record_updated_at_utc DESC, IFF(p.status = 'CLOSED', 0, 1)) = 1;


-- =====================================================
-- 8. SUPPLIER SHIPMENTS + PURCHASE ORDERS - latest version, one part column (Incident B)
-- =====================================================
CREATE OR REPLACE TABLE SUPPLIER_SHIPMENTS AS
SELECT TRIM(shipment_ref)                                         AS shipment_ref,
       NULLIF(TRIM(po_ref), '')                                   AS purchase_order_ref,
       COALESCE(part_number, component_id)                        AS part_number,   -- two names, one meaning
       TRY_TO_NUMBER(quantity)                                    AS quantity,
       TRIM(ship_from)                                            AS ship_from,
       UPPER(TRIM(ship_to))                                       AS ship_to_airport_code,
       UPPER(TRIM(status))                                        AS status,
       TRY_TO_TIMESTAMP_NTZ(dispatched_at, 'DD/MM/YYYY HH24:MI')  AS dispatched_at_utc,
       TRY_TO_TIMESTAMP_NTZ(promised_at,   'DD/MM/YYYY HH24:MI')  AS promised_at_utc,
       TRY_TO_TIMESTAMP_NTZ(delivered_at,  'DD/MM/YYYY HH24:MI')  AS delivered_at_utc,
       DATEDIFF('minute', promised_at_utc, delivered_at_utc)      AS minutes_late,
       carrier_name,                                              -- NULL before 2026-10-25: expected
       TRY_TO_TIMESTAMP_NTZ(record_updated_at, 'DD/MM/YYYY HH24:MI') AS record_updated_at_utc,
       _source_file
FROM BRONZE.SUPPLIER_SHIPMENTS_RAW
QUALIFY ROW_NUMBER() OVER (PARTITION BY TRIM(shipment_ref)
                           ORDER BY TRY_TO_TIMESTAMP_NTZ(record_updated_at, 'DD/MM/YYYY HH24:MI') DESC,
                                    IFF(UPPER(TRIM(status)) = 'DELIVERED', 0, 1)) = 1;

CREATE OR REPLACE TABLE PURCHASE_ORDERS AS
SELECT TRIM(po_ref) AS purchase_order_ref, UPPER(TRIM(order_type)) AS order_type, TRIM(supplier_id) AS supplier_id,
       COALESCE(part_number, component_id) AS part_number, UPPER(TRIM(airline)) AS airline_code,
       UPPER(TRIM(deliver_to)) AS deliver_to_airport_code, TRY_TO_NUMBER(quantity) AS quantity,
       TRY_TO_TIMESTAMP_NTZ(ordered_at,  'DD/MM/YYYY HH24:MI') AS ordered_at_utc,
       TRY_TO_TIMESTAMP_NTZ(promised_at, 'DD/MM/YYYY HH24:MI') AS promised_at_utc,
       _source_file
FROM BRONZE.SUPPLIER_POS_RAW
QUALIFY ROW_NUMBER() OVER (PARTITION BY TRIM(po_ref) ORDER BY _source_file DESC) = 1;


-- =====================================================
-- 9. INVENTORY SNAPSHOTS - one row per (hour, store, part); the resent file is ignored
-- =====================================================
CREATE OR REPLACE TABLE INVENTORY_SNAPSHOTS AS
SELECT TRY_TO_TIMESTAMP_NTZ(snapshot_utc, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS snapshot_utc,
       UPPER(TRIM(airline_code)) AS airline_code, UPPER(TRIM(airport_code)) AS airport_code,
       TRIM(part_number) AS part_number, TRY_TO_NUMBER(quantity_on_hand) AS quantity_on_hand,
       _source_file
FROM BRONZE.INVENTORY_SNAPSHOTS_RAW
QUALIFY ROW_NUMBER() OVER (PARTITION BY snapshot_utc, airline_code, airport_code, part_number
                           ORDER BY IFF(_source_file LIKE '%_resend%', 1, 0), _loaded_at) = 1;


-- =====================================================
-- 10. PASSENGERS, BOOKING SEGMENTS, BOARDINGS - cleaned, flight keys derived
--     (PASSENGERS holds synthetic personal data -> masking policies in V4)
-- =====================================================
CREATE OR REPLACE TABLE PASSENGERS AS
SELECT TRIM(booking_ref)                                   AS booking_ref,
       TRIM(passenger_id)                                  AS passenger_id,
       REGEXP_REPLACE(TRIM(passenger_name), '\\s+', ' ')   AS passenger_name,   -- '  Ana  Silva ' -> 'Ana Silva'
       LOWER(TRIM(email))                                  AS email,
       UPPER(TRIM(frequent_flyer_tier))                    AS frequent_flyer_tier,
       UPPER(TRIM(cabin))                                  AS cabin,
       UPPER(TRIM(airline))                                AS airline_code,
       UPPER(TRIM(origin))                                 AS origin_airport_code,
       UPPER(TRIM(destination))                            AS final_destination_code,
       TRY_TO_BOOLEAN(is_connecting)                       AS is_connecting,
       _source_file
FROM BRONZE.RES_BOOKINGS_RAW
QUALIFY ROW_NUMBER() OVER (PARTITION BY TRIM(booking_ref) ORDER BY _loaded_at DESC, _source_row) = 1;

CREATE OR REPLACE TABLE BOOKING_SEGMENTS AS
SELECT TRIM(r.booking_ref) AS booking_ref, TRY_TO_NUMBER(r.segment_no) AS segment_no,
       s.flight_id         AS planned_flight_id,                 -- derived from number + origin + local date
       r.flight_number, TRY_TO_DATE(r.flight_date_local) AS flight_date_local,
       UPPER(TRIM(r.origin)) AS origin_airport_code, UPPER(TRIM(r.destination)) AS destination_airport_code
FROM BRONZE.RES_SEGMENTS_RAW r
LEFT JOIN FLIGHT_SCHEDULE s
       ON s.flight_number = r.flight_number
      AND s.origin_airport_code = UPPER(TRIM(r.origin))
      AND DATE(s.scheduled_departure_local) = TRY_TO_DATE(r.flight_date_local)
QUALIFY ROW_NUMBER() OVER (PARTITION BY TRIM(r.booking_ref), TRY_TO_NUMBER(r.segment_no)
                           ORDER BY r._loaded_at DESC) = 1;

CREATE OR REPLACE TABLE BOARDINGS AS
SELECT TRIM(b.booking_ref) AS booking_ref,
       s.flight_id         AS flight_id,                         -- the flight they ACTUALLY boarded
       b.flight_number, TRY_TO_DATE(b.flight_date_local) AS flight_date_local,
       UPPER(TRIM(b.origin)) AS origin_airport_code,
       TRY_TO_TIMESTAMP_NTZ(b.boarded_at_utc, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS boarded_at_utc
FROM BRONZE.DCS_BOARDED_RAW b
LEFT JOIN FLIGHT_SCHEDULE s
       ON s.flight_number = b.flight_number
      AND s.origin_airport_code = UPPER(TRIM(b.origin))
      AND DATE(s.scheduled_departure_local) = TRY_TO_DATE(b.flight_date_local)
QUALIFY ROW_NUMBER() OVER (PARTITION BY TRIM(b.booking_ref), s.flight_id ORDER BY b._loaded_at DESC) = 1;


-- =====================================================
-- 11. WEATHER ALERTS - epoch seconds -> UTC timestamps, numeric severity -> label
-- =====================================================
CREATE OR REPLACE TABLE WEATHER_ALERTS AS
SELECT raw:alert_id::STRING                                   AS weather_alert_id,
       UPPER(raw:station_iata::STRING)                        AS airport_code,
       CASE raw:severity_level::NUMBER WHEN 2 THEN 'MODERATE' WHEN 3 THEN 'SEVERE'
            ELSE 'UNKNOWN' END                                AS severity,
       TO_TIMESTAMP_NTZ(raw:valid_from_epoch::NUMBER)         AS valid_from_utc,
       TO_TIMESTAMP_NTZ(raw:valid_to_epoch::NUMBER)           AS valid_to_utc
FROM BRONZE.WEATHER_ALERTS_RAW
QUALIFY ROW_NUMBER() OVER (PARTITION BY raw:alert_id::STRING ORDER BY _loaded_at DESC) = 1;