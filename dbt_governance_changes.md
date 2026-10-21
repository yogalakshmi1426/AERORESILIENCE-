# dbt changes for V4 governance (then Build, then re-Deploy if you use the task graph)

## 1. dbt_project.yml - under `marts:` add one line (keep everything else)
```yaml
    marts:
      +materialized: table
      +schema: marts
      +grants:
        select: ['AERO_ANALYST']        # re-applied on every build
      +persist_docs:
        relation: true
        columns: true
```

## 2. models/marts/dim_passenger.sql - add this as the FIRST lines of the file
```sql
{{ config(post_hook=[
    "alter table {{ this }} modify column email set masking policy AERORESILIENCE.GOVERNANCE.PII_MASK",
    "alter table {{ this }} modify column passenger_name set masking policy AERORESILIENCE.GOVERNANCE.PII_MASK"
]) }}
```
