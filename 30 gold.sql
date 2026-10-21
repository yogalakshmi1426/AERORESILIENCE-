-- =====================================================
-- AERORESILIENCE - 30_gold.sql
-- GOLD = the star schema: dimensions (who/what/where/when) + facts (what happened, measures).
-- Organised around BUSINESS QUESTIONS, not sources. Read by Power BI, analysts and ML.
--   * surrogate keys = MD5(natural key): stable across rebuilds, source-independent
--   * date_key = YYYYMMDD integer (UTC date)
--   * three fact types: transaction, periodic snapshot, accumulating snapshot
-- Run All after 20_silver.sql has passed 21_silver_tests.sql.
-- =====================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE AERO_WH;
USE DATABASE AERORESILIENCE;
USE SCHEMA GOLD;


-- =====================================================
-- DIMENSIONS
-- =====================================================

-- Calendar: one row per day (generated, not loaded)
CREATE OR REPLACE TABLE DIM_DATE AS
WITH days AS (
    SELECT DATEADD('day', ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1, '2026-10-01'::DATE) AS d
    FROM TABLE(GENERATOR(ROWCOUNT => 61))
)
SELECT TO_NUMBER(TO_CHAR(d, 'YYYYMMDD')) AS date_key,
       d                                  AS calendar_date,
       DAYNAME(d)                         AS day_name,
       DAYOFWEEKISO(d)                    AS day_of_week_iso,
       DAYOFWEEKISO(d) IN (6, 7)          AS is_weekend,
       WEEKISO(d)                         AS iso_week,
       MONTH(d)                           AS month_number,
       MONTHNAME(d)                       AS month_name,
       YEAR(d)                            AS year_number
FROM days;

CREATE OR REPLACE TABLE DIM_AIRPORT AS
SELECT MD5(a.airport_code)                                  AS airport_key,
       a.airport_code, a.airport_name, a.city, a.country_code, a.timezone,
       h.hub_for_airlines IS NOT NULL                       AS is_hub,
       h.hub_for_airlines,
       m.min_connection_minutes                             AS mct_intl_intl_minutes
FROM SILVER.AIRPORTS a
LEFT JOIN (SELECT hub_airport_code, LISTAGG(airline_code, ', ') AS hub_for_airlines
           FROM SILVER.AIRLINES GROUP BY 1) h ON h.hub_airport_code = a.airport_code
LEFT JOIN SILVER.MIN_CONNECTION_TIMES m
       ON m.airport_code = a.airport_code AND m.connection_type = 'INTL_INTL';

CREATE OR REPLACE TABLE DIM_AIRLINE AS
SELECT MD5(airline_code) AS airline_key, airline_code, airline_name, country_code, hub_airport_code
FROM SILVER.AIRLINES;

-- SCD Type 2-ready: if an aircraft's attributes changed, a NEW row would be added and the old one closed
CREATE OR REPLACE TABLE DIM_AIRCRAFT AS
SELECT MD5(ac.registration)                                     AS aircraft_key,
       ac.registration, ac.airline_code, ac.icao_type, ac.manufacturer, ac.model, ac.body_type,
       ac.seats, ac.year_built,
       2026 - ac.year_built                                     AS age_years,
       ac.registration NOT IN (SELECT DISTINCT planned_aircraft_registration
                               FROM SILVER.FLIGHT_SCHEDULE)     AS is_standby,
       '2026-01-01'::DATE                                       AS effective_from,
       '9999-12-31'::DATE                                       AS effective_to,
       TRUE                                                     AS is_current
FROM SILVER.AIRCRAFT ac;

CREATE OR REPLACE TABLE DIM_ROUTE AS
SELECT MD5(route_id) AS route_key, route_id, airline_code, origin_airport_code, destination_airport_code,
       block_time_minutes, required_body_type,
       IFF(block_time_minutes < 240, 'SHORT_HAUL', 'LONG_HAUL') AS haul_type
FROM SILVER.ROUTES;

CREATE OR REPLACE TABLE DIM_SUPPLIER AS
SELECT MD5(supplier_id) AS supplier_key, supplier_id, supplier_name, country_code,
       ship_from_airport_code, on_time_rate AS contracted_on_time_rate
FROM SILVER.SUPPLIERS;

-- Denormalised: supplier name copied in, so part reports need no extra join
CREATE OR REPLACE TABLE DIM_PART AS
SELECT MD5(p.part_number) AS part_key, p.part_number, p.description, p.ata_chapter, p.icao_type,
       p.criticality, p.unit_cost_gbp, p.standard_lead_time_days, p.supplier_id, s.supplier_name
FROM SILVER.PARTS p
LEFT JOIN SILVER.SUPPLIERS s ON s.supplier_id = p.supplier_id;

CREATE OR REPLACE TABLE DIM_DELAY_CODE AS
SELECT column1 AS delay_code, column2 AS delay_description, column3 AS delay_group,
       column4 AS is_reactionary
FROM VALUES
    ('32', 'Ramp handling: loading/unloading',               'GROUND_HANDLING', FALSE),
    ('41', 'Aircraft defect (technical)',                    'TECHNICAL',       FALSE),
    ('71', 'Weather at departure airport',                   'WEATHER',         FALSE),
    ('81', 'Air traffic flow restriction (en-route)',        'AIR_TRAFFIC',     FALSE),
    ('93', 'Reactionary: aircraft late from previous flight','REACTIONARY',     TRUE);

-- Contains (synthetic) personal data -> masking policies in V4
CREATE OR REPLACE TABLE DIM_PASSENGER AS
SELECT MD5(passenger_id) AS passenger_key, passenger_id, passenger_name, email, frequent_flyer_tier
FROM SILVER.PASSENGERS;


-- =====================================================
-- FACTS
-- =====================================================

-- FACT_FLIGHT - grain: one scheduled flight
CREATE OR REPLACE TABLE FACT_FLIGHT AS
WITH pax AS (
    SELECT flight_id, COUNT(*) AS passengers_boarded FROM SILVER.BOARDINGS GROUP BY 1
),
bags AS (
    SELECT flight_id, COUNT(DISTINCT bag_tag) AS bags_loaded
    FROM SILVER.BAG_SCANS WHERE event_type = 'LOADED' GROUP BY 1
),
dl AS (
    SELECT flight_id,
           SUM(IFF(delay_code = '93', delay_minutes, 0))  AS reactionary_delay_minutes,
           SUM(IFF(delay_code <> '93', delay_minutes, 0)) AS primary_delay_minutes,
           SUM(IFF(delay_code = '41', delay_minutes, 0))  AS technical_delay_minutes,
           SUM(IFF(delay_code = '71', delay_minutes, 0))  AS weather_delay_minutes
    FROM SILVER.FLIGHT_DELAYS GROUP BY 1
)
SELECT f.flight_id,                                                       -- degenerate dimension
       f.flight_number,
       TO_NUMBER(TO_CHAR(f.scheduled_departure_utc, 'YYYYMMDD'))           AS date_key,
       MD5(f.airline_code)                                                AS airline_key,
       MD5(f.route_id)                                                    AS route_key,
       MD5(f.origin_airport_code)                                         AS origin_airport_key,
       MD5(f.destination_airport_code)                                    AS destination_airport_key,
       MD5(f.planned_aircraft_registration)                               AS planned_aircraft_key,
       IFF(f.actual_aircraft_registration IS NULL, NULL,
           MD5(f.actual_aircraft_registration))                           AS actual_aircraft_key,
       f.scheduled_departure_utc, f.scheduled_arrival_utc,
       f.actual_departure_utc, f.actual_arrival_utc,
       f.flight_status,
       f.is_cancelled,
       f.flight_status = 'DELAYED'                                        AS is_delayed_15,
       f.departure_delay_minutes,
       f.arrival_delay_minutes,
       COALESCE(dl.primary_delay_minutes, 0)                              AS primary_delay_minutes,
       COALESCE(dl.reactionary_delay_minutes, 0)                          AS reactionary_delay_minutes,
       COALESCE(dl.technical_delay_minutes, 0)                            AS technical_delay_minutes,
       COALESCE(dl.weather_delay_minutes, 0)                              AS weather_delay_minutes,
       f.had_aircraft_change,
       ac.seats,
       COALESCE(pax.passengers_boarded, 0)                                AS passengers_boarded,
       ROUND(COALESCE(pax.passengers_boarded, 0) / NULLIF(ac.seats, 0), 3) AS load_factor,
       COALESCE(bags.bags_loaded, 0)                                      AS bags_loaded,
       f.planned_previous_flight_id,
       f.actual_previous_flight_id                                        -- the dependency-graph edge
FROM SILVER.FLIGHTS f
LEFT JOIN SILVER.AIRCRAFT ac
       ON ac.registration = COALESCE(f.actual_aircraft_registration, f.planned_aircraft_registration)
LEFT JOIN pax  ON pax.flight_id  = f.flight_id
LEFT JOIN bags ON bags.flight_id = f.flight_id
LEFT JOIN dl   ON dl.flight_id   = f.flight_id;


-- FACT_FLIGHT_DELAY - grain: one delay cause for one flight (transaction fact)
CREATE OR REPLACE TABLE FACT_FLIGHT_DELAY AS
SELECT d.flight_id,
       TO_NUMBER(TO_CHAR(f.scheduled_departure_utc, 'YYYYMMDD')) AS date_key,
       MD5(f.airline_code)                                      AS airline_key,
       MD5(f.origin_airport_code)                               AS origin_airport_key,
       d.delay_code,                                            -- joins to DIM_DELAY_CODE
       d.delay_minutes
FROM SILVER.FLIGHT_DELAYS d
JOIN SILVER.FLIGHTS f ON f.flight_id = d.flight_id;


-- FACT_BAG_SCAN - grain: one scan event (transaction fact, ~468k rows)
CREATE OR REPLACE TABLE FACT_BAG_SCAN AS
SELECT scan_id, bag_tag, flight_id,
       TO_NUMBER(TO_CHAR(event_time_utc, 'YYYYMMDD')) AS date_key,
       MD5(airport_code)                              AS airport_key,
       event_type, event_time_utc, area, time_was_local, received_late
FROM SILVER.BAG_SCANS;


-- FACT_BAG_JOURNEY - grain: one bag (ACCUMULATING snapshot: milestones filled in as the bag moves)
CREATE OR REPLACE TABLE FACT_BAG_JOURNEY AS
WITH milestones AS (
    SELECT bag_tag,
           ANY_VALUE(booking_ref)                                                   AS booking_ref,
           MAX(weight_kg)                                                           AS weight_kg,
           MIN(IFF(event_type = 'CHECK_IN', event_time_utc, NULL))                  AS checked_in_utc,
           MIN(IFF(event_type = 'LOADED',   event_time_utc, NULL))                  AS first_loaded_utc,
           MAX(IFF(event_type IN ('ARRIVED_RECLAIM', 'DELIVERED_TO_PASSENGER'),
                   event_time_utc, NULL))                                           AS delivered_utc,
           BOOLOR_AGG(event_type = 'MISSED_CONNECTION')                             AS was_misconnected,
           BOOLOR_AGG(event_type = 'DELIVERED_TO_PASSENGER')                        AS delivered_by_courier,
           BOOLOR_AGG(event_type = 'HELD_AWAITING_REBOOKING')                       AS held_at_period_end,
           BOOLOR_AGG(event_type = 'ARRIVED_RECLAIM')                               AS reached_reclaim,
           COUNT(*)                                                                 AS scans_received,
           COUNT(DISTINCT flight_id)                                                AS flights_used
    FROM SILVER.BAG_SCANS
    GROUP BY bag_tag
)
SELECT m.bag_tag, m.booking_ref,
       MD5(p.passenger_id)                 AS passenger_key,
       MD5(p.airline_code)                 AS airline_key,
       MD5(p.origin_airport_code)          AS origin_airport_key,
       MD5(p.final_destination_code)       AS destination_airport_key,
       TO_NUMBER(TO_CHAR(m.checked_in_utc, 'YYYYMMDD')) AS date_key,
       m.weight_kg, m.checked_in_utc, m.first_loaded_utc, m.delivered_utc,
       DATEDIFF('minute', m.checked_in_utc, m.delivered_utc) AS minutes_check_in_to_delivery,
       m.was_misconnected, m.scans_received, m.flights_used,
       CASE WHEN m.delivered_by_courier                     THEN 'MISHANDLED_DELIVERED_LATE'
            WHEN m.was_misconnected                         THEN 'MISHANDLED_IN_TRANSIT_AT_PERIOD_END'
            WHEN m.held_at_period_end                       THEN 'IN_TRANSIT_AT_PERIOD_END'
            WHEN m.reached_reclaim                          THEN 'DELIVERED_WITH_PASSENGER'
            ELSE 'UNKNOWN_NO_FINAL_SCAN' END                AS bag_status,   -- data lost upstream (Incident C)
       m.was_misconnected OR m.delivered_by_courier         AS is_mishandled
FROM milestones m
LEFT JOIN SILVER.PASSENGERS p ON p.booking_ref = m.booking_ref;


-- FACT_PASSENGER_JOURNEY - grain: one booking (accumulating snapshot: planned vs actual journey)
CREATE OR REPLACE TABLE FACT_PASSENGER_JOURNEY AS
WITH planned_last AS (
    SELECT bs.booking_ref, f.scheduled_arrival_utc AS planned_arrival_utc
    FROM SILVER.BOOKING_SEGMENTS bs
    JOIN SILVER.FLIGHTS f ON f.flight_id = bs.planned_flight_id
    QUALIFY ROW_NUMBER() OVER (PARTITION BY bs.booking_ref ORDER BY bs.segment_no DESC) = 1
),
boarded_last AS (
    SELECT b.booking_ref, f.destination_airport_code AS reached_airport_code,
           f.actual_arrival_utc
    FROM SILVER.BOARDINGS b
    JOIN SILVER.FLIGHTS f ON f.flight_id = b.flight_id
    QUALIFY ROW_NUMBER() OVER (PARTITION BY b.booking_ref ORDER BY b.boarded_at_utc DESC) = 1
),
changed AS (                         -- any planned flight the passenger did NOT board = rebooked
    SELECT bs.booking_ref, COUNT(*) AS planned_flights_missed
    FROM SILVER.BOOKING_SEGMENTS bs
    LEFT JOIN SILVER.BOARDINGS b
           ON b.booking_ref = bs.booking_ref AND b.flight_id = bs.planned_flight_id
    WHERE b.flight_id IS NULL
    GROUP BY 1
)
SELECT p.booking_ref,
       MD5(p.passenger_id)                    AS passenger_key,
       MD5(p.airline_code)                    AS airline_key,
       MD5(p.origin_airport_code)             AS origin_airport_key,
       MD5(p.final_destination_code)          AS destination_airport_key,
       TO_NUMBER(TO_CHAR(pl.planned_arrival_utc, 'YYYYMMDD')) AS date_key,
       p.cabin, p.is_connecting,
       pl.planned_arrival_utc,
       IFF(bl.reached_airport_code = p.final_destination_code, bl.actual_arrival_utc, NULL) AS actual_arrival_utc,
       IFF(bl.reached_airport_code = p.final_destination_code,
           DATEDIFF('minute', pl.planned_arrival_utc, bl.actual_arrival_utc), NULL)          AS arrival_delay_minutes,
       CASE WHEN bl.reached_airport_code IS DISTINCT FROM p.final_destination_code THEN 'INCOMPLETE_AT_PERIOD_END'
            WHEN COALESCE(c.planned_flights_missed, 0) > 0                         THEN 'REBOOKED'
            ELSE 'AS_PLANNED' END                                                    AS journey_status
FROM SILVER.PASSENGERS p
LEFT JOIN planned_last pl ON pl.booking_ref = p.booking_ref
LEFT JOIN boarded_last bl ON bl.booking_ref = p.booking_ref
LEFT JOIN changed      c  ON c.booking_ref  = p.booking_ref;


-- FACT_MAINTENANCE_EVENT - grain: one defect
CREATE OR REPLACE TABLE FACT_MAINTENANCE_EVENT AS
SELECT m.fault_ref,
       MD5(m.aircraft_registration)                         AS aircraft_key,
       MD5(m.airport_code)                                  AS airport_key,
       MD5(m.part_number)                                   AS part_key,
       TO_NUMBER(TO_CHAR(m.reported_at_utc, 'YYYYMMDD'))    AS date_key,
       m.mel_category, m.status, m.part_source,
       m.reported_at_utc, m.completed_at_utc, m.minutes_to_repair,
       m.mel_category = 'NO_GO'                             AS grounded_aircraft,
       IFF(m.mel_category = 'NO_GO', m.minutes_to_repair, NULL) AS aog_minutes,
       p.unit_cost_gbp                                      AS part_cost_gbp
FROM SILVER.MAINTENANCE_EVENTS m
LEFT JOIN SILVER.PARTS p ON p.part_number = m.part_number;


-- FACT_INVENTORY_SNAPSHOT - grain: one part, one store, one hour (PERIODIC snapshot)
CREATE OR REPLACE TABLE FACT_INVENTORY_SNAPSHOT AS
SELECT snapshot_utc,
       TO_NUMBER(TO_CHAR(snapshot_utc, 'YYYYMMDD')) AS date_key,
       HOUR(snapshot_utc)                           AS snapshot_hour,
       MD5(airline_code)                            AS airline_key,
       MD5(airport_code)                            AS airport_key,
       MD5(part_number)                             AS part_key,
       quantity_on_hand,
       quantity_on_hand = 0                         AS is_stockout
FROM SILVER.INVENTORY_SNAPSHOTS;


-- FACT_SHIPMENT - grain: one shipment (latest status)
CREATE OR REPLACE TABLE FACT_SHIPMENT AS
SELECT shipment_ref, purchase_order_ref,
       MD5(part_number)                                     AS part_key,
       MD5(ship_to_airport_code)                            AS destination_airport_key,
       TO_NUMBER(TO_CHAR(dispatched_at_utc, 'YYYYMMDD'))    AS date_key,
       ship_from, status, carrier_name,
       dispatched_at_utc, promised_at_utc, delivered_at_utc, minutes_late,
       IFF(delivered_at_utc IS NULL, NULL, delivered_at_utc <= promised_at_utc) AS delivered_on_time
FROM SILVER.SUPPLIER_SHIPMENTS;