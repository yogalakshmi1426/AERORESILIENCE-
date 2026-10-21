-- =====================================================
-- Parallel-run reconciliation: does dbt reproduce V1 SILVER exactly, for EVERY table?
-- Run in a normal SQL file AFTER `dbt build` passes.
--
-- HASH_AGG(columns) = one fingerprint for a whole set of rows (order does not matter).
-- Same rows + same values  ->  same fingerprint.  One value different -> different fingerprint.
-- Every row must say MATCH.
-- =====================================================
USE WAREHOUSE AERO_WH;

WITH pairs AS (
              SELECT 'airports' AS tbl,
                     (SELECT COUNT(*) FROM AERORESILIENCE.SILVER.AIRPORTS) AS v1_rows,
                     (SELECT COUNT(*) FROM AERORESILIENCE.DBT_DEV_STAGING.STG_AIRPORTS) AS dbt_rows,
                     (SELECT HASH_AGG(airport_code, timezone) FROM AERORESILIENCE.SILVER.AIRPORTS) AS v1_hash,
                     (SELECT HASH_AGG(airport_code, timezone) FROM AERORESILIENCE.DBT_DEV_STAGING.STG_AIRPORTS) AS dbt_hash
    UNION ALL SELECT 'airlines',
                     (SELECT COUNT(*) FROM AERORESILIENCE.SILVER.AIRLINES),
                     (SELECT COUNT(*) FROM AERORESILIENCE.DBT_DEV_STAGING.STG_AIRLINES),
                     (SELECT HASH_AGG(airline_code, airline_name, hub_airport_code) FROM AERORESILIENCE.SILVER.AIRLINES),
                     (SELECT HASH_AGG(airline_code, airline_name, hub_airport_code) FROM AERORESILIENCE.DBT_DEV_STAGING.STG_AIRLINES)
    UNION ALL SELECT 'aircraft',
                     (SELECT COUNT(*) FROM AERORESILIENCE.SILVER.AIRCRAFT),
                     (SELECT COUNT(*) FROM AERORESILIENCE.DBT_DEV_STAGING.STG_AIRCRAFT),
                     (SELECT HASH_AGG(registration, icao_type, seats, year_built) FROM AERORESILIENCE.SILVER.AIRCRAFT),
                     (SELECT HASH_AGG(registration, icao_type, seats, year_built) FROM AERORESILIENCE.DBT_DEV_STAGING.STG_AIRCRAFT)
    UNION ALL SELECT 'routes',
                     (SELECT COUNT(*) FROM AERORESILIENCE.SILVER.ROUTES),
                     (SELECT COUNT(*) FROM AERORESILIENCE.DBT_DEV_STAGING.STG_ROUTES),
                     (SELECT HASH_AGG(route_id, block_time_minutes) FROM AERORESILIENCE.SILVER.ROUTES),
                     (SELECT HASH_AGG(route_id, block_time_minutes) FROM AERORESILIENCE.DBT_DEV_STAGING.STG_ROUTES)
    UNION ALL SELECT 'min_connection_times',
                     (SELECT COUNT(*) FROM AERORESILIENCE.SILVER.MIN_CONNECTION_TIMES),
                     (SELECT COUNT(*) FROM AERORESILIENCE.DBT_DEV_STAGING.STG_MIN_CONNECTION_TIMES),
                     (SELECT HASH_AGG(airport_code, connection_type, min_connection_minutes) FROM AERORESILIENCE.SILVER.MIN_CONNECTION_TIMES),
                     (SELECT HASH_AGG(airport_code, connection_type, min_connection_minutes) FROM AERORESILIENCE.DBT_DEV_STAGING.STG_MIN_CONNECTION_TIMES)
    UNION ALL SELECT 'suppliers',
                     (SELECT COUNT(*) FROM AERORESILIENCE.SILVER.SUPPLIERS),
                     (SELECT COUNT(*) FROM AERORESILIENCE.DBT_DEV_STAGING.STG_SUPPLIERS),
                     (SELECT HASH_AGG(supplier_id, on_time_rate) FROM AERORESILIENCE.SILVER.SUPPLIERS),
                     (SELECT HASH_AGG(supplier_id, on_time_rate) FROM AERORESILIENCE.DBT_DEV_STAGING.STG_SUPPLIERS)
    UNION ALL SELECT 'parts',
                     (SELECT COUNT(*) FROM AERORESILIENCE.SILVER.PARTS),
                     (SELECT COUNT(*) FROM AERORESILIENCE.DBT_DEV_STAGING.STG_PARTS),
                     (SELECT HASH_AGG(part_number, criticality, unit_cost_gbp) FROM AERORESILIENCE.SILVER.PARTS),
                     (SELECT HASH_AGG(part_number, criticality, unit_cost_gbp) FROM AERORESILIENCE.DBT_DEV_STAGING.STG_PARTS)
    UNION ALL SELECT 'flights',
                     (SELECT COUNT(*) FROM AERORESILIENCE.SILVER.FLIGHTS),
                     (SELECT COUNT(*) FROM AERORESILIENCE.DBT_DEV_INTERMEDIATE.INT_FLIGHTS),
                     (SELECT HASH_AGG(flight_id, flight_status, actual_departure_utc, actual_arrival_utc,
                                      actual_aircraft_registration, actual_previous_flight_id) FROM AERORESILIENCE.SILVER.FLIGHTS),
                     (SELECT HASH_AGG(flight_id, flight_status, actual_departure_utc, actual_arrival_utc,
                                      actual_aircraft_registration, actual_previous_flight_id) FROM AERORESILIENCE.DBT_DEV_INTERMEDIATE.INT_FLIGHTS)
    UNION ALL SELECT 'flight_delays',
                     (SELECT COUNT(*) FROM AERORESILIENCE.SILVER.FLIGHT_DELAYS),
                     (SELECT COUNT(*) FROM AERORESILIENCE.DBT_DEV_INTERMEDIATE.INT_FLIGHT_DELAYS),
                     (SELECT HASH_AGG(flight_id, delay_code, delay_minutes) FROM AERORESILIENCE.SILVER.FLIGHT_DELAYS),
                     (SELECT HASH_AGG(flight_id, delay_code, delay_minutes) FROM AERORESILIENCE.DBT_DEV_INTERMEDIATE.INT_FLIGHT_DELAYS)
    UNION ALL SELECT 'bag_scans',
                     (SELECT COUNT(*) FROM AERORESILIENCE.SILVER.BAG_SCANS),
                     (SELECT COUNT(*) FROM AERORESILIENCE.DBT_DEV_INTERMEDIATE.INT_BAG_SCANS),
                     (SELECT HASH_AGG(scan_id, event_type, event_time_utc, airport_code, flight_id) FROM AERORESILIENCE.SILVER.BAG_SCANS),
                     (SELECT HASH_AGG(scan_id, event_type, event_time_utc, airport_code, flight_id) FROM AERORESILIENCE.DBT_DEV_INTERMEDIATE.INT_BAG_SCANS)
    UNION ALL SELECT 'maintenance_events',
                     (SELECT COUNT(*) FROM AERORESILIENCE.SILVER.MAINTENANCE_EVENTS),
                     (SELECT COUNT(*) FROM AERORESILIENCE.DBT_DEV_INTERMEDIATE.INT_MAINTENANCE_EVENTS),
                     (SELECT HASH_AGG(fault_ref, airport_code, mel_category, status, completed_at_utc, defect_description) FROM AERORESILIENCE.SILVER.MAINTENANCE_EVENTS),
                     (SELECT HASH_AGG(fault_ref, airport_code, mel_category, status, completed_at_utc, defect_description) FROM AERORESILIENCE.DBT_DEV_INTERMEDIATE.INT_MAINTENANCE_EVENTS)
    UNION ALL SELECT 'supplier_shipments',
                     (SELECT COUNT(*) FROM AERORESILIENCE.SILVER.SUPPLIER_SHIPMENTS),
                     (SELECT COUNT(*) FROM AERORESILIENCE.DBT_DEV_STAGING.STG_SUPPLIER_SHIPMENTS),
                     (SELECT HASH_AGG(shipment_ref, part_number, status, delivered_at_utc) FROM AERORESILIENCE.SILVER.SUPPLIER_SHIPMENTS),
                     (SELECT HASH_AGG(shipment_ref, part_number, status, delivered_at_utc) FROM AERORESILIENCE.DBT_DEV_STAGING.STG_SUPPLIER_SHIPMENTS)
    UNION ALL SELECT 'purchase_orders',
                     (SELECT COUNT(*) FROM AERORESILIENCE.SILVER.PURCHASE_ORDERS),
                     (SELECT COUNT(*) FROM AERORESILIENCE.DBT_DEV_STAGING.STG_PURCHASE_ORDERS),
                     (SELECT HASH_AGG(purchase_order_ref, part_number) FROM AERORESILIENCE.SILVER.PURCHASE_ORDERS),
                     (SELECT HASH_AGG(purchase_order_ref, part_number) FROM AERORESILIENCE.DBT_DEV_STAGING.STG_PURCHASE_ORDERS)
    UNION ALL SELECT 'inventory_snapshots',
                     (SELECT COUNT(*) FROM AERORESILIENCE.SILVER.INVENTORY_SNAPSHOTS),
                     (SELECT COUNT(*) FROM AERORESILIENCE.DBT_DEV_STAGING.STG_INVENTORY_SNAPSHOTS),
                     (SELECT HASH_AGG(snapshot_utc, airline_code, airport_code, part_number, quantity_on_hand) FROM AERORESILIENCE.SILVER.INVENTORY_SNAPSHOTS),
                     (SELECT HASH_AGG(snapshot_utc, airline_code, airport_code, part_number, quantity_on_hand) FROM AERORESILIENCE.DBT_DEV_STAGING.STG_INVENTORY_SNAPSHOTS)
    UNION ALL SELECT 'passengers',
                     (SELECT COUNT(*) FROM AERORESILIENCE.SILVER.PASSENGERS),
                     (SELECT COUNT(*) FROM AERORESILIENCE.DBT_DEV_STAGING.STG_PASSENGERS),
                     (SELECT HASH_AGG(booking_ref, passenger_name, email, is_connecting) FROM AERORESILIENCE.SILVER.PASSENGERS),
                     (SELECT HASH_AGG(booking_ref, passenger_name, email, is_connecting) FROM AERORESILIENCE.DBT_DEV_STAGING.STG_PASSENGERS)
    UNION ALL SELECT 'booking_segments',
                     (SELECT COUNT(*) FROM AERORESILIENCE.SILVER.BOOKING_SEGMENTS),
                     (SELECT COUNT(*) FROM AERORESILIENCE.DBT_DEV_INTERMEDIATE.INT_BOOKING_SEGMENTS),
                     (SELECT HASH_AGG(booking_ref, segment_no, planned_flight_id) FROM AERORESILIENCE.SILVER.BOOKING_SEGMENTS),
                     (SELECT HASH_AGG(booking_ref, segment_no, planned_flight_id) FROM AERORESILIENCE.DBT_DEV_INTERMEDIATE.INT_BOOKING_SEGMENTS)
    UNION ALL SELECT 'boardings',
                     (SELECT COUNT(*) FROM AERORESILIENCE.SILVER.BOARDINGS),
                     (SELECT COUNT(*) FROM AERORESILIENCE.DBT_DEV_INTERMEDIATE.INT_BOARDINGS),
                     (SELECT HASH_AGG(booking_ref, flight_id, boarded_at_utc) FROM AERORESILIENCE.SILVER.BOARDINGS),
                     (SELECT HASH_AGG(booking_ref, flight_id, boarded_at_utc) FROM AERORESILIENCE.DBT_DEV_INTERMEDIATE.INT_BOARDINGS)
    UNION ALL SELECT 'weather_alerts',
                     (SELECT COUNT(*) FROM AERORESILIENCE.SILVER.WEATHER_ALERTS),
                     (SELECT COUNT(*) FROM AERORESILIENCE.DBT_DEV_STAGING.STG_WEATHER_ALERTS),
                     (SELECT HASH_AGG(weather_alert_id, severity, valid_from_utc) FROM AERORESILIENCE.SILVER.WEATHER_ALERTS),
                     (SELECT HASH_AGG(weather_alert_id, severity, valid_from_utc) FROM AERORESILIENCE.DBT_DEV_STAGING.STG_WEATHER_ALERTS)
)
SELECT IFF(v1_rows = dbt_rows AND v1_hash = dbt_hash, 'MATCH', 'DIFFERENT') AS result,
       tbl, v1_rows, dbt_rows
FROM pairs
ORDER BY result, tbl;          -- any DIFFERENT row appears first