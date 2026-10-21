-- =====================================================
-- AERORESILIENCE - 31_gold_tests.sql
-- GOLD must (1) match the truth for business numbers and (2) have no broken star-schema links.
-- Every row should say PASS.
-- =====================================================

USE WAREHOUSE AERO_WH;
USE SCHEMA AERORESILIENCE.GOLD;

WITH checks AS (
    -- ---------- business numbers must match the original truth ----------
              SELECT 'flights: rows'                          AS check_name, '472' AS expected, (SELECT COUNT(*) FROM FACT_FLIGHT)::STRING AS actual
    UNION ALL SELECT 'flights: on-time % (truth 83.8)',      '83.8',
              (SELECT ROUND(100 * SUM(IFF(flight_status = 'ON_TIME', 1, 0))
                      / NULLIF(SUM(IFF(NOT is_cancelled, 1, 0)), 0), 1) FROM FACT_FLIGHT)::STRING
    UNION ALL SELECT 'flights: load factor never above 100%', '0',  (SELECT COUNT(*) FROM FACT_FLIGHT WHERE load_factor > 1)::STRING
    UNION ALL SELECT 'delay causes: rows',                     '165', (SELECT COUNT(*) FROM FACT_FLIGHT_DELAY)::STRING
    UNION ALL SELECT 'passenger journeys: rows',               '81439', (SELECT COUNT(*) FROM FACT_PASSENGER_JOURNEY)::STRING
    UNION ALL SELECT 'passenger journeys: AS_PLANNED (truth)', '80384', (SELECT COUNT(*) FROM FACT_PASSENGER_JOURNEY WHERE journey_status = 'AS_PLANNED')::STRING
    UNION ALL SELECT 'passenger journeys: REBOOKED (truth)',   '959',   (SELECT COUNT(*) FROM FACT_PASSENGER_JOURNEY WHERE journey_status = 'REBOOKED')::STRING
    UNION ALL SELECT 'passenger journeys: INCOMPLETE (truth)', '96',    (SELECT COUNT(*) FROM FACT_PASSENGER_JOURNEY WHERE journey_status = 'INCOMPLETE_AT_PERIOD_END')::STRING
    UNION ALL SELECT 'passenger journeys: 15+ min late (truth)', '22209',
              (SELECT COUNT(*) FROM FACT_PASSENGER_JOURNEY WHERE arrival_delay_minutes >= 15)::STRING
    UNION ALL SELECT 'bags: rows',                             '69370', (SELECT COUNT(*) FROM FACT_BAG_JOURNEY)::STRING
    UNION ALL SELECT 'bags: mishandled, delivered late (truth)', '454', (SELECT COUNT(*) FROM FACT_BAG_JOURNEY WHERE bag_status = 'MISHANDLED_DELIVERED_LATE')::STRING
    UNION ALL SELECT 'bags: no final scan (Incident C data loss)', '317', (SELECT COUNT(*) FROM FACT_BAG_JOURNEY WHERE bag_status = 'UNKNOWN_NO_FINAL_SCAN')::STRING
    UNION ALL SELECT 'maintenance: defects',                   '15',  (SELECT COUNT(*) FROM FACT_MAINTENANCE_EVENT)::STRING
    UNION ALL SELECT 'maintenance: aircraft-on-ground hours (truth 23.0)', '23.0',
              (SELECT ROUND(SUM(aog_minutes) / 60, 1) FROM FACT_MAINTENANCE_EVENT)::STRING
    UNION ALL SELECT 'inventory snapshots: rows',              '18480', (SELECT COUNT(*) FROM FACT_INVENTORY_SNAPSHOT)::STRING

    -- ---------- star-schema integrity: every fact key must exist in its dimension ----------
    UNION ALL SELECT 'FACT_FLIGHT -> DIM_AIRLINE orphans', '0',
              (SELECT COUNT(*) FROM FACT_FLIGHT f LEFT JOIN DIM_AIRLINE d ON d.airline_key = f.airline_key WHERE d.airline_key IS NULL)::STRING
    UNION ALL SELECT 'FACT_FLIGHT -> DIM_ROUTE orphans', '0',
              (SELECT COUNT(*) FROM FACT_FLIGHT f LEFT JOIN DIM_ROUTE d ON d.route_key = f.route_key WHERE d.route_key IS NULL)::STRING
    UNION ALL SELECT 'FACT_FLIGHT -> DIM_AIRPORT orphans (origin)', '0',
              (SELECT COUNT(*) FROM FACT_FLIGHT f LEFT JOIN DIM_AIRPORT d ON d.airport_key = f.origin_airport_key WHERE d.airport_key IS NULL)::STRING
    UNION ALL SELECT 'FACT_FLIGHT -> DIM_AIRCRAFT orphans (actual)', '0',
              (SELECT COUNT(*) FROM FACT_FLIGHT f LEFT JOIN DIM_AIRCRAFT d ON d.aircraft_key = f.actual_aircraft_key
               WHERE f.actual_aircraft_key IS NOT NULL AND d.aircraft_key IS NULL)::STRING
    UNION ALL SELECT 'FACT_FLIGHT -> DIM_DATE orphans', '0',
              (SELECT COUNT(*) FROM FACT_FLIGHT f LEFT JOIN DIM_DATE d ON d.date_key = f.date_key WHERE d.date_key IS NULL)::STRING
    UNION ALL SELECT 'FACT_FLIGHT_DELAY -> DIM_DELAY_CODE orphans', '0',
              (SELECT COUNT(*) FROM FACT_FLIGHT_DELAY f LEFT JOIN DIM_DELAY_CODE d ON d.delay_code = f.delay_code WHERE d.delay_code IS NULL)::STRING
    UNION ALL SELECT 'FACT_MAINTENANCE_EVENT -> DIM_PART orphans', '0',
              (SELECT COUNT(*) FROM FACT_MAINTENANCE_EVENT f LEFT JOIN DIM_PART d ON d.part_key = f.part_key WHERE d.part_key IS NULL)::STRING
    UNION ALL SELECT 'FACT_PASSENGER_JOURNEY -> DIM_PASSENGER orphans', '0',
              (SELECT COUNT(*) FROM FACT_PASSENGER_JOURNEY f LEFT JOIN DIM_PASSENGER d ON d.passenger_key = f.passenger_key WHERE d.passenger_key IS NULL)::STRING
    UNION ALL SELECT 'dimension keys unique (airport, aircraft, part, passenger)', '0',
              ((SELECT COUNT(*) - COUNT(DISTINCT airport_key)   FROM DIM_AIRPORT)
             + (SELECT COUNT(*) - COUNT(DISTINCT aircraft_key)  FROM DIM_AIRCRAFT)
             + (SELECT COUNT(*) - COUNT(DISTINCT part_key)      FROM DIM_PART)
             + (SELECT COUNT(*) - COUNT(DISTINCT passenger_key) FROM DIM_PASSENGER))::STRING
)
SELECT IFF(expected = actual, 'PASS', 'FAIL') AS result, check_name, expected, actual
FROM checks
ORDER BY result, check_name;