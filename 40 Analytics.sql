-- =====================================================
-- AERORESILIENCE - 40_analytics.sql
-- The business questions, answered from the GOLD star schema.
-- Run ONE query at a time (cursor inside the query -> Run).
-- Each query states: the business question, and the SQL technique it demonstrates.
-- =====================================================

USE WAREHOUSE AERO_WH;
USE SCHEMA AERORESILIENCE.GOLD;


-- =====================================================
-- Q1. CONTROL-TOWER HEADLINE: how did the network perform this week?
-- Technique: conditional aggregation (SUM(IFF(...))), scalar subqueries across facts
-- =====================================================
SELECT COUNT(*)                                                              AS flights_scheduled,
       SUM(IFF(is_cancelled, 1, 0))                                          AS flights_cancelled,
       ROUND(100 * SUM(IFF(flight_status = 'ON_TIME', 1, 0))
             / NULLIF(SUM(IFF(NOT is_cancelled, 1, 0)), 0), 1)               AS on_time_pct,
       ROUND(AVG(IFF(is_delayed_15, departure_delay_minutes, NULL)), 0)      AS avg_delay_when_late_min,
       SUM(passengers_boarded)                                               AS passengers_flown,
       ROUND(100 * AVG(IFF(NOT is_cancelled, load_factor, NULL)), 1)         AS avg_load_factor_pct,
       (SELECT ROUND(1000 * SUM(IFF(is_mishandled, 1, 0))
               / (SELECT COUNT(*) FROM FACT_PASSENGER_JOURNEY), 1)
        FROM FACT_BAG_JOURNEY)                                               AS mishandled_bags_per_1000_pax,
       (SELECT ROUND(100 * SUM(IFF(arrival_delay_minutes >= 15, 1, 0)) / COUNT(*), 1)
        FROM FACT_PASSENGER_JOURNEY)                                         AS pct_journeys_arriving_late,
       (SELECT ROUND(SUM(aog_minutes) / 60, 1) FROM FACT_MAINTENANCE_EVENT)  AS aircraft_on_ground_hours
FROM FACT_FLIGHT;


-- =====================================================
-- Q2. WHAT CAUSES OUR DELAYS? (primary causes vs knock-on effects)
-- Technique: star-schema join to a dimension + RATIO_TO_REPORT window function (share of total)
-- =====================================================
SELECT d.delay_group,
       d.is_reactionary,
       COUNT(DISTINCT f.flight_id)                                     AS flights_affected,
       SUM(f.delay_minutes)                                            AS delay_minutes,
       ROUND(100 * RATIO_TO_REPORT(SUM(f.delay_minutes)) OVER (), 1)   AS pct_of_all_delay
FROM FACT_FLIGHT_DELAY f
JOIN DIM_DELAY_CODE d ON d.delay_code = f.delay_code
GROUP BY d.delay_group, d.is_reactionary
ORDER BY delay_minutes DESC;


-- =====================================================
-- Q3. WHICH AIRPORTS ARE OUR WEAKEST POINTS?
-- Technique: RANK() window function over an aggregate
-- =====================================================
SELECT a.airport_code,
       a.city,
       a.is_hub,
       COUNT(*)                                                                   AS departures,
       ROUND(100 * SUM(IFF(f.flight_status = 'ON_TIME', 1, 0))
             / NULLIF(SUM(IFF(NOT f.is_cancelled, 1, 0)), 0), 1)                  AS on_time_pct,
       SUM(f.departure_delay_minutes)                                             AS total_delay_min,
       RANK() OVER (ORDER BY SUM(f.departure_delay_minutes) DESC)                 AS delay_rank
FROM FACT_FLIGHT f
JOIN DIM_AIRPORT a ON a.airport_key = f.origin_airport_key
GROUP BY a.airport_code, a.city, a.is_hub
ORDER BY delay_rank;


-- =====================================================
-- Q4. DAY BY DAY: when did the network struggle, and was weather to blame?
-- Technique: date dimension + LEFT JOIN to an event table on a time window
-- =====================================================
SELECT d.calendar_date,
       d.day_name,
       COUNT(f.flight_id)                                                         AS flights,
       ROUND(100 * SUM(IFF(f.flight_status = 'ON_TIME', 1, 0))
             / NULLIF(SUM(IFF(NOT f.is_cancelled, 1, 0)), 0), 1)                  AS on_time_pct,
       SUM(f.weather_delay_minutes)                                               AS weather_delay_min,
       SUM(f.reactionary_delay_minutes)                                           AS knock_on_delay_min,
       (SELECT LISTAGG(w.airport_code || ' ' || w.severity, ', ')
        FROM AERORESILIENCE.SILVER.WEATHER_ALERTS w
        WHERE DATE(w.valid_from_utc) = d.calendar_date)                           AS weather_alerts
FROM DIM_DATE d
JOIN FACT_FLIGHT f ON f.date_key = d.date_key
GROUP BY d.calendar_date, d.day_name
ORDER BY d.calendar_date;


-- =====================================================
-- Q5. THE DISRUPTION DEPENDENCY GRAPH: which single delays spread the furthest?
-- Walks aircraft rotations: flight -> the next flight flown by the SAME aircraft
-- (actual_previous_flight_id), as long as the delay keeps being passed on.
-- Technique: RECURSIVE CTE (graph traversal in SQL)
-- =====================================================
WITH RECURSIVE cascade (root_flight_id, flight_id, depth, reactionary_delay_minutes, passengers_boarded) AS (
    -- anchor: every flight that STARTED a problem (primary delay of an hour or more)
    SELECT flight_id, flight_id, 0, reactionary_delay_minutes, passengers_boarded
    FROM FACT_FLIGHT
    WHERE primary_delay_minutes >= 60
    UNION ALL
    -- recursive step: the next flight of the same aircraft, if it inherited delay
    SELECT c.root_flight_id, n.flight_id, c.depth + 1, n.reactionary_delay_minutes, n.passengers_boarded
    FROM cascade c
    JOIN FACT_FLIGHT n ON n.actual_previous_flight_id = c.flight_id
    WHERE n.reactionary_delay_minutes > 0
      AND c.depth < 30                                     -- safety stop
)
SELECT c.root_flight_id,
       r.primary_delay_minutes                                  AS original_delay_min,
       MAX(c.depth)                                             AS later_flights_affected,
       SUM(IFF(c.depth > 0, c.reactionary_delay_minutes, 0))    AS knock_on_delay_min_caused,
       SUM(IFF(c.depth > 0, c.passengers_boarded, 0))           AS knock_on_passengers_delayed
FROM cascade c
JOIN FACT_FLIGHT r ON r.flight_id = c.root_flight_id
GROUP BY c.root_flight_id, r.primary_delay_minutes
ORDER BY later_flights_affected DESC, knock_on_delay_min_caused DESC
LIMIT 10;


-- =====================================================
-- Q6. "FLIGHT DELAY -> PASSENGERS -> BAGS -> CONNECTIONS"  (the project-guide impact chain)
-- For each inbound flight: how many connecting passengers missed their onward flight,
-- how many bags were mishandled, how many onward flights were affected?
-- Technique: multi-CTE pipeline, anti-join (planned flight NOT boarded), pivoting rows to columns
-- =====================================================
WITH connections AS (          -- connecting bookings: planned first and second flight
    SELECT booking_ref,
           MAX(IFF(segment_no = 1, planned_flight_id, NULL)) AS inbound_flight_id,
           MAX(IFF(segment_no = 2, planned_flight_id, NULL)) AS onward_flight_id
    FROM AERORESILIENCE.SILVER.BOOKING_SEGMENTS
    GROUP BY booking_ref
    HAVING onward_flight_id IS NOT NULL
),
misconnected AS (              -- flew the inbound flight, did NOT board the planned onward flight
    SELECT c.*
    FROM connections c
    JOIN AERORESILIENCE.SILVER.BOARDINGS b1
      ON b1.booking_ref = c.booking_ref AND b1.flight_id = c.inbound_flight_id
    LEFT JOIN AERORESILIENCE.SILVER.BOARDINGS b2
      ON b2.booking_ref = c.booking_ref AND b2.flight_id = c.onward_flight_id
    JOIN FACT_FLIGHT onward ON onward.flight_id = c.onward_flight_id
    WHERE b2.flight_id IS NULL                 -- anti-join: no boarding record
      AND NOT onward.is_cancelled              -- (cancellations are a different story)
),
bags AS (
    SELECT c.inbound_flight_id, COUNT(*) AS bags_mishandled
    FROM FACT_BAG_JOURNEY j
    JOIN connections c ON c.booking_ref = j.booking_ref
    WHERE j.is_mishandled
    GROUP BY 1
)
SELECT m.inbound_flight_id,
       f.departure_delay_minutes                   AS inbound_dep_delay_min,
       f.arrival_delay_minutes                     AS inbound_arr_delay_min,
       COUNT(*)                                    AS passengers_misconnected,
       COALESCE(MAX(b.bags_mishandled), 0)         AS bags_mishandled,
       COUNT(DISTINCT m.onward_flight_id)          AS onward_flights_affected
FROM misconnected m
JOIN FACT_FLIGHT f ON f.flight_id = m.inbound_flight_id
LEFT JOIN bags b   ON b.inbound_flight_id = m.inbound_flight_id
GROUP BY m.inbound_flight_id, f.departure_delay_minutes, f.arrival_delay_minutes
ORDER BY passengers_misconnected DESC
LIMIT 10;


-- =====================================================
-- Q7. "IF THIS PART DOESN'T ARRIVE, WHAT DOES IT AFFECT?"
-- For every aircraft grounded by a NO_GO fault: the part, where it came from, how long the
-- aircraft was stuck, and the chain of flights, minutes and passengers it disrupted.
-- Technique: QUALIFY to pick the first affected flight + RECURSIVE CTE for the knock-on chain
-- =====================================================
WITH RECURSIVE groundings AS (
    SELECT m.fault_ref, a.registration, a.aircraft_key, ap.airport_code AS grounded_at,
           p.part_number, p.description AS part, m.part_source,
           m.reported_at_utc, m.completed_at_utc, ROUND(m.aog_minutes / 60, 1) AS hours_on_ground
    FROM FACT_MAINTENANCE_EVENT m
    JOIN DIM_AIRCRAFT a ON a.aircraft_key = m.aircraft_key
    JOIN DIM_AIRPORT ap ON ap.airport_key = m.airport_key
    JOIN DIM_PART p     ON p.part_key     = m.part_key
    WHERE m.grounded_aircraft
),
first_hit AS (                 -- the first flight that aircraft operated after the fault
    SELECT g.fault_ref, f.flight_id
    FROM groundings g
    JOIN FACT_FLIGHT f
      ON f.actual_aircraft_key = g.aircraft_key AND f.actual_departure_utc > g.reported_at_utc
    QUALIFY ROW_NUMBER() OVER (PARTITION BY g.fault_ref ORDER BY f.actual_departure_utc) = 1
),
chain (fault_ref, flight_id, depth, delay_minutes, passengers) AS (
    SELECT h.fault_ref, f.flight_id, 0, f.departure_delay_minutes, f.passengers_boarded
    FROM first_hit h JOIN FACT_FLIGHT f ON f.flight_id = h.flight_id
    UNION ALL
    SELECT c.fault_ref, n.flight_id, c.depth + 1, n.departure_delay_minutes, n.passengers_boarded
    FROM chain c
    JOIN FACT_FLIGHT n ON n.actual_previous_flight_id = c.flight_id
    WHERE n.reactionary_delay_minutes > 0 AND c.depth < 30
)
SELECT g.fault_ref, g.registration, g.grounded_at, g.part, g.part_source, g.hours_on_ground,
       COUNT(c.flight_id)      AS flights_disrupted,
       SUM(c.delay_minutes)    AS total_delay_minutes,
       SUM(c.passengers)       AS passengers_on_disrupted_flights,
       LISTAGG(c.flight_id, ' -> ') WITHIN GROUP (ORDER BY c.depth) AS disruption_chain
FROM groundings g
LEFT JOIN chain c ON c.fault_ref = g.fault_ref
GROUP BY g.fault_ref, g.registration, g.grounded_at, g.part, g.part_source, g.hours_on_ground
ORDER BY total_delay_minutes DESC;


-- =====================================================
-- Q8. WHERE ARE WE EXPOSED RIGHT NOW? Safety-critical (NO_GO) parts with ZERO stock
-- at the hub, and how many aircraft depend on them.
-- Technique: QUALIFY with MAX() OVER () to pick the latest periodic snapshot
-- =====================================================
WITH latest AS (
    SELECT * FROM FACT_INVENTORY_SNAPSHOT
    QUALIFY snapshot_utc = MAX(snapshot_utc) OVER ()
),
fleet AS (
    SELECT airline_code, icao_type, COUNT(*) AS aircraft_count
    FROM DIM_AIRCRAFT GROUP BY 1, 2
)
SELECT al.airline_code, ap.airport_code AS hub_store, p.part_number, p.description,
       p.icao_type, fl.aircraft_count AS aircraft_exposed,
       p.unit_cost_gbp, p.standard_lead_time_days, p.supplier_name
FROM latest l
JOIN DIM_PART p     ON p.part_key     = l.part_key
JOIN DIM_AIRLINE al ON al.airline_key = l.airline_key
JOIN DIM_AIRPORT ap ON ap.airport_key = l.airport_key
JOIN fleet fl       ON fl.airline_code = al.airline_code AND fl.icao_type = p.icao_type
WHERE l.quantity_on_hand = 0
  AND p.criticality = 'NO_GO'
ORDER BY aircraft_exposed DESC, p.standard_lead_time_days DESC;


-- =====================================================
-- Q9. SUPPLY CHAIN: did parts arrive when promised?
-- Technique: simple fact analysis with NULL-aware logic (undelivered shipments)
-- =====================================================
SELECT shipment_ref, ship_from, status, carrier_name,
       promised_at_utc, delivered_at_utc, minutes_late,
       CASE WHEN delivered_at_utc IS NULL THEN 'NOT YET DELIVERED'
            WHEN delivered_on_time        THEN 'ON TIME'
            ELSE 'LATE' END AS delivery_result
FROM FACT_SHIPMENT
ORDER BY dispatched_at_utc;


-- =====================================================
-- Q10. BAGGAGE: which airlines lose the most bags, per 1,000 passengers?
-- Technique: two aggregates at different grains joined on a shared dimension key
-- =====================================================
WITH pax AS (
    SELECT airline_key, COUNT(*) AS passengers FROM FACT_PASSENGER_JOURNEY GROUP BY 1
),
bag AS (
    SELECT airline_key,
           COUNT(*)                                   AS bags,
           SUM(IFF(is_mishandled, 1, 0))              AS mishandled,
           SUM(IFF(bag_status = 'UNKNOWN_NO_FINAL_SCAN', 1, 0)) AS unknown_outcome
    FROM FACT_BAG_JOURNEY GROUP BY 1
)
SELECT al.airline_code, al.airline_name, p.passengers, b.bags, b.mishandled,
       ROUND(1000 * b.mishandled / p.passengers, 1) AS mishandled_per_1000_pax,
       b.unknown_outcome                            AS bags_with_no_final_scan
FROM pax p
JOIN bag b          ON b.airline_key  = p.airline_key
JOIN DIM_AIRLINE al ON al.airline_key = p.airline_key
ORDER BY mishandled_per_1000_pax DESC;