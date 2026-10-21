-- =====================================================
-- AERORESILIENCE - 80_governance.sql   (V4: access control, PII masking, volume monitoring)
-- Run ONE SECTION AT A TIME (select the section's lines, then Run).
-- Role ACCOUNTADMIN, warehouse AERO_WH. Close the search box before running.
-- =====================================================
USE ROLE ACCOUNTADMIN;
USE WAREHOUSE AERO_WH;

-- -----------------------------------------------------
-- 1. ROLE-BASED ACCESS (DONE): analyst role that can read ONLY the star schema (dbt marts)
-- -----------------------------------------------------
CREATE ROLE IF NOT EXISTS AERO_ANALYST COMMENT = 'Read-only access to the GOLD star schema (dbt marts)';
GRANT USAGE ON WAREHOUSE AERO_WH TO ROLE AERO_ANALYST;
GRANT USAGE ON DATABASE AERORESILIENCE TO ROLE AERO_ANALYST;
GRANT USAGE ON SCHEMA AERORESILIENCE.DBT_DEV_MARTS TO ROLE AERO_ANALYST;
GRANT SELECT ON ALL TABLES IN SCHEMA AERORESILIENCE.DBT_DEV_MARTS TO ROLE AERO_ANALYST;   -- 17 tables
-- dbt re-creates the mart tables on every build, so dbt_project.yml re-applies this grant (+grants).
-- The user name is stored in lowercase, so it must be in double quotes:
GRANT ROLE AERO_ANALYST TO USER "yogalakshmishanmugajothi";


-- -----------------------------------------------------
-- 2. PII MASKING POLICY
--    CURRENT_ROLE() = only the role you switched to. (IS_ROLE_IN_SESSION would also count the
--    background "secondary roles", so an admin user would never see the masked view when testing.)
--    IF NOT EXISTS + ALTER: a policy that is attached to columns cannot be CREATE OR REPLACEd.
-- -----------------------------------------------------
CREATE SCHEMA IF NOT EXISTS AERORESILIENCE.GOVERNANCE;

CREATE MASKING POLICY IF NOT EXISTS AERORESILIENCE.GOVERNANCE.PII_MASK AS (val STRING)
RETURNS STRING -> '***MASKED***'
COMMENT = 'Masks personal data for every role except ACCOUNTADMIN';

ALTER MASKING POLICY AERORESILIENCE.GOVERNANCE.PII_MASK SET BODY ->
    CASE WHEN CURRENT_ROLE() = 'ACCOUNTADMIN' THEN val
         ELSE '***MASKED***'
    END;


-- -----------------------------------------------------
-- 3. ATTACH THE POLICY + CHECK IT IS ATTACHED
--    dbt's post_hook (models/marts/dim_passenger.sql) re-attaches it after every build.
--    These two lines attach it now as well; FORCE = replace any policy already there (safe to re-run).
-- -----------------------------------------------------
ALTER TABLE AERORESILIENCE.DBT_DEV_MARTS.DIM_PASSENGER MODIFY COLUMN email
    SET MASKING POLICY AERORESILIENCE.GOVERNANCE.PII_MASK FORCE;
ALTER TABLE AERORESILIENCE.DBT_DEV_MARTS.DIM_PASSENGER MODIFY COLUMN passenger_name
    SET MASKING POLICY AERORESILIENCE.GOVERNANCE.PII_MASK FORCE;

-- Expected: 2 rows (EMAIL, PASSENGER_NAME) with policy PII_MASK
SELECT ref_column_name, policy_name, policy_kind
FROM TABLE(AERORESILIENCE.INFORMATION_SCHEMA.POLICY_REFERENCES(
       REF_ENTITY_NAME   => 'AERORESILIENCE.DBT_DEV_MARTS.DIM_PASSENGER',
       REF_ENTITY_DOMAIN => 'table'));


-- -----------------------------------------------------
-- 4. TEST: run these ONE LINE AT A TIME, in order
-- -----------------------------------------------------
USE ROLE AERO_ANALYST;
SELECT passenger_name, email, frequent_flyer_tier
FROM AERORESILIENCE.DBT_DEV_MARTS.DIM_PASSENGER LIMIT 5;     -- expect ***MASKED*** for name + email

USE SECONDARY ROLES NONE;                                    -- switch off background admin powers
SELECT CURRENT_ROLE(), CURRENT_SECONDARY_ROLES();             -- expect AERO_ANALYST, no secondary roles
SELECT COUNT(*) FROM AERORESILIENCE.SILVER.PASSENGERS;       -- expect ERROR: not authorized (by design)
-- If SILVER is still readable, the Workspace keeps secondary roles on: a true access test
-- needs a separate least-privilege test user. Record that as a known limitation.

USE ROLE ACCOUNTADMIN;                                       -- ALWAYS finish with these 2 lines
USE SECONDARY ROLES ALL;                                     -- dbt needs secondary roles ON
SELECT passenger_name, email FROM AERORESILIENCE.DBT_DEV_MARTS.DIM_PASSENGER LIMIT 5;   -- real values



--    Reconciliation cannot catch a source that HONESTLY sends too little, so compare each
--    airport-day with that airport's usual volume. Operating week only: edge days are partial.
-- -----------------------------------------------------
CREATE OR REPLACE VIEW AERORESILIENCE.GOVERNANCE.V_BAG_FEED_VOLUME AS
WITH daily AS (
    SELECT raw:location.airport::STRING                                             AS airport_code,
           TRY_TO_DATE(REGEXP_SUBSTR(_source_file, 'date=([0-9-]+)', 1, 1, 'e'))     AS file_date,
           COUNT(*)                                                                  AS scans
    FROM AERORESILIENCE.BRONZE.BAG_SCANS_RAW
    GROUP BY 1, 2
)
SELECT airport_code, file_date, scans,
       MEDIAN(scans) OVER (PARTITION BY airport_code)                               AS typical_scans,
       ROUND(scans / NULLIF(typical_scans, 0), 2)                                   AS ratio_to_typical,
       IFF(ratio_to_typical < 0.30, 'VOLUME ANOMALY', 'ok')                         AS volume_status
FROM daily
WHERE file_date BETWEEN '2026-10-21' AND '2026-10-27';

SELECT * FROM AERORESILIENCE.GOVERNANCE.V_BAG_FEED_VOLUME
ORDER BY volume_status DESC, airport_code, file_date;        -- expect FRA 2026-10-24 flagged


-- -----------------------------------------------------
-- 6. RE-DEPLOY dbt after ANY dbt file change (DONE for V4), so the task graph runs the new version
-- ----------------------------------SQL compilation error: syntax error line 76 at position 0 unexpected '5.'.-------------------
-- USE WAREHOUSE AERO_WH;
-- ALTER DBT PROJECT AERORESILIENCE.ORCHESTRATION.AERORESILIENCE_DBT
--   ADD VERSION FROM $$snow://workspace/"USER$yogalakshmishanmugajothi".PUBLIC.DEFAULT$/versions/live/aeroresilience_dbt$$;


-- -----------------------------------------------------
-- 7. LINEAGE (no code): dbt panel -> DAG tab = model lineage; Catalog -> any table -> Lineage tab
-- -----------------------------------------------------