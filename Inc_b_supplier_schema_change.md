# Incident Report - INC-B: Supplier portal schema change

| | |
|---|---|
| **Severity** | Medium - data at risk of silent loss / misplacement; no operational decision affected |
| **Source system** | Supplier portal (shipments and purchase-order feeds) |
| **Detected** | During BRONZE load design, by a header-inspection query |
| **Status** | Resolved; prevention check in place |

## 1. Summary
On 2026-10-25 the supplier portal changed the layout of its daily CSV files without notice:
the column `part_number` was renamed `component_id`, and a new column `carrier_name` was added
at the end. Our standard BRONZE loads read CSV columns **by position**, which would have silently
dropped `carrier_name` and relied on luck for `component_id` landing in the right column.
We switched the supplier feeds to **name-based loading with schema evolution**, so every value
lands in the correctly named column and new columns are captured automatically.
No data was lost.

## 2. Timeline (UTC)
| Date | Event |
|---|---|
| 2026-10-22 / 24 | Supplier files arrive in the original 11-column layout |
| 2026-10-25 | First **shipments** file in the new 12-column layout (`component_id`, `carrier_name`) |
| 2026-10-26 | First **purchase-orders** file in the new layout (no PO file was sent on the 25th) |
| Investigation | Header-inspection query shows two distinct layouts across the 9 supplier files |
| Fix | Supplier tables recreated with `ENABLE_SCHEMA_EVOLUTION`, loaded `MATCH_BY_COLUMN_NAME` |
| Verification | Reconciliation: 9 files declared / 9 loaded; 14 records declared / 14 loaded; 0 mismatches |

## 3. Detection
- Read line 1 of every supplier file as plain text (`CSV_LINE_FORMAT`, `FIELD_DELIMITER = NONE`)
  and grouped identical headers. Result: **two schema signatures**, the second starting 2026-10-25.
- A positional load would **not** have raised an error: the extra column would simply have been
  ignored. This is a **silent failure** - the reason a dedicated check is needed.

## 4. Impact
| Feed | Files in new layout | Records in new layout |
|---|---|---|
| Shipments | 3 of 5 (25, 26, 27 Oct) | 6 of 10 |
| Purchase orders | 2 of 4 (26, 27 Oct) | 2 of 4 |
| **Total** | **5 of 9 files** | **8 of 14 records (57%)** |

Had the positional load been used:
- `carrier_name` would have been **lost** for all 8 records.
- `component_id` values would have landed in `part_number` **only because the column kept the
  same position**. A reordering would have put values in the wrong columns with a green load status.

## 5. Root cause
- **Trigger:** an unannounced change by an external system (the supplier upgraded its portal).
- **Vulnerability:** our loads assumed a **fixed column order** and had **no check on file structure**.
  There was no data contract with the supplier defining the layout or requiring advance notice.

## 6. Resolution
- New file format `CSV_PARSE_HEADER_FORMAT` (`PARSE_HEADER = TRUE`): header row read as column names.
- `COPY INTO ... MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE`: each value goes to the column with
  the same **name**, regardless of position.
- `ENABLE_SCHEMA_EVOLUTION = TRUE` on `SUPPLIER_SHIPMENTS_RAW` and `SUPPLIER_POS_RAW`: Snowflake
  **added `COMPONENT_ID` and `CARRIER_NAME` automatically** when the new files arrived.
- `INCLUDE_METADATA` keeps lineage columns (source file, row, file timestamp) under name-based loading.

**Why schema evolution (option B) over manually widening the table (option A):** BRONZE's job is to
keep everything the source sent. Evolution means a future change is captured, not dropped, even
before anyone reacts. The risk - changes slipping in unnoticed - is covered by the drift check below.

## 7. Prevention
- **Daily drift check** (Phase 4 query): compares each file's header with the previous file of the
  same feed and flags `SCHEMA DRIFT`. To be scheduled and alerted on in V2 (Airflow) / V4 (observability).
- **Data contract** (planned): agree the supplier file layout in writing - columns, types, and a
  notice period for changes - and validate every file against it before loading.
- **Reconciliation** against the delivery log stays in place: it proves no rows were lost in loading.

## 8. Follow-up for SILVER
- One business column, two source names: `part_number = COALESCE(part_number, component_id)`.
- `carrier_name` is NULL for files before 2026-10-25 - expected, not a data-quality failure.
- Keep `_source_file` so any SILVER value can be traced back to the exact file and line.

## 9. Lessons learned
1. A load that succeeds is not the same as a load that is correct.
2. Position-based loading is only safe for sources whose layout is guaranteed.
3. Detection must look at **structure** (headers), not only at **row counts** - reconciliation
   passed throughout, because the row counts were never wrong.