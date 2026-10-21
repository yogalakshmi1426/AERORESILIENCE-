"""
AERORESILIENCE - Synthetic data generator (Snowflake)
04 RAW SOURCE FEEDS  (the corruption layer)

Turns the clean TRUTH into the messy files that real, separate source systems would deliver.

Reads  : @AERORESILIENCE.BRONZE.RAW_STAGE/reference/airports.csv
         @AERORESILIENCE.BRONZE.RAW_STAGE/truth/   (all truth files from steps 02 and 03)
Writes : @AERORESILIENCE.BRONZE.RAW_STAGE/raw/     (date-partitioned landing zone)
           flight_ops/date=YYYY-MM-DD/fos_YYYYMMDD_HH.jsonl            hourly operational messages
           baggage/date=YYYY-MM-DD/bag_scans_<APT>_YYYYMMDD.jsonl.gz    one file per airport per day
           maintenance/date=YYYY-MM-DD/mro_defects_YYYYMMDD.csv         change records (CDC-style)
           supply_chain/shipments|purchase_orders|inventory/date=.../   supplier portal + stock system
           reservations/date=YYYY-MM-DD/bookings_|segments_YYYYMMDD.csv  bookings (synthetic PII)
           departure_control/date=YYYY-MM-DD/boarded_YYYYMMDD.csv       who actually boarded what
           external/weather/date=YYYY-MM-DD/weather_alerts_YYYYMMDD.json JSON array, epoch times
           _control/delivery_log.csv                                     when each file "arrived"
         @AERORESILIENCE.BRONZE.RAW_STAGE/truth/corruption_manifest.csv  every corruption introduced

Raw systems do NOT know our flight_id. They identify flights by flight number + LOCAL date + origin.
"""

import csv
import gzip
import io
import json
import random
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo
from snowflake.snowpark.context import get_active_session

# =====================================================
# SETTINGS - how messy each source system is
# =====================================================
SEED = 42
FOS_DUPLICATE_RATE = 0.03            # same message (same id) delivered twice
FOS_RESENT_NEW_ID_RATE = 0.005       # same event re-sent with a NEW message id (harder to catch)
FOS_LATE_RATE = 0.02                 # message delivered hours or days after the event
FOS_LATE_MINUTES = (120, 1800)
FOS_OUTAGE_HOUR = datetime(2026, 10, 23, 14, tzinfo=timezone.utc)   # Incident A: lost hour...
FOS_BACKFILL_DELIVERED = datetime(2026, 10, 24, 9, 0, tzinfo=timezone.utc)   # ...re-sent next morning

BAG_DUPLICATE_RATE = 0.03            # Incident D: scan repeated 2-5 times in total
BAG_DUPLICATE_EXTRA = (1, 4)
BAG_DROP_RATE = 0.01                 # scanner missed a non-critical scan
BAG_DROPPABLE = {"SCREENED", "SORTED", "TRANSFER_SORTED"}
BAG_LATE_RATE = 0.01                 # scan delivered a day late
BAG_LOCAL_TIME_AIRPORTS = {"DXB"}    # these systems send LOCAL time with no offset
BAG_COLLAPSE = ("FRA", "2026-10-24") # Incident C: feed collapses for this airport-day...
BAG_COLLAPSE_KEEP = 0.02             # ...only 2% of scans get through (the rest are lost)

MRO_DIRTY_STATION_RATE = 0.20
MRO_BLANK_DESCRIPTION_RATE = 0.05
MEL_SPELLINGS = {"NO_GO": ["NO_GO", "No-Go", "NOGO"], "GO_IF": ["GO_IF", "Go If", "GOIF"], "GO": ["GO", "Go"]}

SCHEMA_CHANGE_DATE = "2026-10-25"    # Incident B: supplier portal renames part_number -> component_id
CARRIERS = ["AeroCourier Express", "SkyFreight Logistics"]          # fictional
INVENTORY_RESENT_DATE = "2026-10-23" # this day's inventory file is delivered twice

EMAIL_UPPER_RATE = 0.10
NAME_SPACES_RATE = 0.05

REF_STAGE = "@AERORESILIENCE.BRONZE.RAW_STAGE/reference/"
TRUTH_STAGE = "@AERORESILIENCE.BRONZE.RAW_STAGE/truth/"
RAW_STAGE = "@AERORESILIENCE.BRONZE.RAW_STAGE/raw/"
IN_LOCAL = Path("/tmp/aeroresilience/p5_in")
RAW_LOCAL = Path("/tmp/aeroresilience/raw")
UTC = timezone.utc


# =====================================================
# HELPERS
# =====================================================
def get_file(session, stage, name):
    session.file.get(stage + name, str(IN_LOCAL))
    return IN_LOCAL / name


def load_csv(session, stage, name):
    with open(get_file(session, stage, name), newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    print(f"IN  {name:<26} {len(rows):>7} rows")
    return rows


def load_jsonl_gz(session, stage, name):
    with gzip.open(get_file(session, stage, name), "rt", encoding="utf-8") as f:
        rows = [json.loads(line) for line in f]
    print(f"IN  {name:<26} {len(rows):>7} rows")
    return rows


def parse_utc(s):
    return datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC) if s else None


def iso(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def uk(dt):
    return dt.strftime("%d/%m/%Y %H:%M") if dt else ""


class RawWriter:
    """Writes files into the local landing zone and records each one in the delivery log."""

    def __init__(self):
        self.delivery_log = []

    def _path(self, rel):
        p = RAW_LOCAL / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        return p

    def _log(self, rel, system, delivered, n):
        self.delivery_log.append({"file_path": rel, "source_system": system,
                                  "delivered_utc": iso(delivered), "record_count": n})

    def csv(self, rel, system, delivered, columns, rows):
        with open(self._path(rel), "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(columns)
            for r in rows:
                w.writerow([r.get(c, "") for c in columns])
        self._log(rel, system, delivered, len(rows))

    def jsonl(self, rel, system, delivered, rows, compress=False):
        text = "".join(json.dumps(r) + "\n" for r in rows)
        if compress:   # mtime=0: identical data gives byte-identical .gz files (reproducible)
            with gzip.GzipFile(self._path(rel), "wb", mtime=0) as f:
                f.write(text.encode("utf-8"))
        else:
            with open(self._path(rel), "w", encoding="utf-8") as f:
                f.write(text)
        self._log(rel, system, delivered, len(rows))

    def json_array(self, rel, system, delivered, rows):
        with open(self._path(rel), "w", encoding="utf-8") as f:
            json.dump(rows, f, indent=2)
        self._log(rel, system, delivered, len(rows))


def end_of_day(day_str, hour=23, minute=59):
    return datetime.strptime(day_str, "%Y-%m-%d").replace(hour=hour, minute=minute, tzinfo=UTC)


# =====================================================
# 1) FLIGHT OPERATIONS SYSTEM - hourly JSON message files
# =====================================================
def flight_ops(rng, writer, manifest, schedule, actual_by_id, delays_by_flight, tz_of):
    messages = []

    def flight_ref(s):
        dep_local = parse_utc(s["scheduled_departure_utc"]).astimezone(ZoneInfo(tz_of[s["origin_airport_code"]]))
        return {"flight_number": s["flight_number"], "flight_date_local": dep_local.strftime("%Y-%m-%d"),
                "origin": s["origin_airport_code"], "destination": s["destination_airport_code"]}

    def add(msg_type, event_time, s, payload):
        messages.append({"message_id": f"FOS-{len(messages) + 1:07d}", "message_type": msg_type,
                         "event_time_utc": iso(event_time), "flight": flight_ref(s), "payload": payload,
                         "_event": event_time, "_truth_flight": s["flight_id"]})

    for s in schedule:
        a = actual_by_id[s["flight_id"]]
        sched_dep = parse_utc(s["scheduled_departure_utc"])
        if a["flight_status"] == "CANCELLED":
            add("CANCELLED", sched_dep - timedelta(minutes=90), s, {"reason": a["cancellation_reason"]})
            continue
        if a["actual_aircraft_registration"] != s["aircraft_registration"]:
            add("AIRCRAFT_CHANGE", sched_dep - timedelta(minutes=45), s,
                {"planned_registration": s["aircraft_registration"],
                 "new_registration": a["actual_aircraft_registration"]})
        dep, arr = parse_utc(a["actual_departure_utc"]), parse_utc(a["actual_arrival_utc"])
        add("OUT", dep, s, {"actual_departure_utc": iso(dep),
                             "aircraft_registration": a["actual_aircraft_registration"]})
        for d in delays_by_flight[s["flight_id"]]:
            add("DELAY", dep, s, {"delay_code": d["delay_code"], "delay_minutes": int(d["delay_minutes"])})
        add("IN", arr, s, {"actual_arrival_utc": iso(arr)})

    # ---- corruption: when each message is SENT, plus duplicates ----
    sent = []
    for m in messages:
        t = m["_event"] + timedelta(minutes=rng.randint(0, 3))
        if rng.random() < FOS_LATE_RATE:
            t = m["_event"] + timedelta(minutes=rng.randint(*FOS_LATE_MINUTES))
            manifest.append(("flight_ops", m["message_id"], "LATE_ARRIVAL", f"sent {iso(t)}"))
        sent.append((t, m))
        if rng.random() < FOS_DUPLICATE_RATE:
            sent.append((t + timedelta(minutes=rng.randint(0, 10)), m))
            manifest.append(("flight_ops", m["message_id"], "DUPLICATE_EXACT", "same message_id twice"))
        if rng.random() < FOS_RESENT_NEW_ID_RATE:
            copy = dict(m, message_id=f"FOS-R{len(sent):07d}")
            sent.append((t + timedelta(minutes=rng.randint(30, 120)), copy))
            manifest.append(("flight_ops", m["message_id"], "DUPLICATE_NEW_ID",
                             f"re-sent as {copy['message_id']}"))

    # ---- write one file per SENT hour; the outage hour becomes a next-day backfill file ----
    by_hour = defaultdict(list)
    for t, m in sent:
        by_hour[t.replace(minute=0, second=0)].append((t, m))
    for hour, items in sorted(by_hour.items()):
        rows = [dict({k: v for k, v in m.items() if not k.startswith("_")}, sent_at_utc=iso(t))
                for t, m in sorted(items, key=lambda x: (x[0], x[1]["message_id"]))]
        folder = f"flight_ops/date={hour:%Y-%m-%d}"
        if hour == FOS_OUTAGE_HOUR:
            writer.jsonl(f"{folder}/fos_{hour:%Y%m%d_%H}_backfill.jsonl", "flight_ops",
                         FOS_BACKFILL_DELIVERED, rows)
            for r in rows:
                manifest.append(("flight_ops", r["message_id"], "MISSING_HOUR_THEN_BACKFILLED",
                                 f"hour {hour:%Y-%m-%d %H}:00 delivered {iso(FOS_BACKFILL_DELIVERED)}"))
        else:
            writer.jsonl(f"{folder}/fos_{hour:%Y%m%d_%H}.jsonl", "flight_ops",
                         hour + timedelta(hours=1, minutes=5), rows)
    return messages, sent


# =====================================================
# 2) BAGGAGE SYSTEMS - one gzip JSON Lines file per airport per day
# =====================================================
def baggage(rng, writer, manifest, scans, tz_of):
    files = defaultdict(list)            # (airport, sent_date) -> [(sent_time, record)]
    collapse_apt, collapse_day = BAG_COLLAPSE
    for s in scans:
        event = parse_utc(s["event_time_utc"])
        airport = s["location"]["airport"]
        if s["event_type"] in BAG_DROPPABLE and rng.random() < BAG_DROP_RATE:
            manifest.append(("baggage", s["scan_id"], "DROPPED_SCAN", s["event_type"]))
            continue
        t = event + timedelta(minutes=rng.randint(0, 2))
        if rng.random() < BAG_LATE_RATE:
            t = event + timedelta(hours=rng.randint(24, 48))
            manifest.append(("baggage", s["scan_id"], "LATE_ARRIVAL", f"sent {iso(t)}"))
        day = t.strftime("%Y-%m-%d")
        if airport == collapse_apt and day == collapse_day and rng.random() > BAG_COLLAPSE_KEEP:
            manifest.append(("baggage", s["scan_id"], "VOLUME_COLLAPSE_LOST", f"{airport} feed {day}"))
            continue
        record = json.loads(json.dumps(s))                    # independent copy
        if airport in BAG_LOCAL_TIME_AIRPORTS:                 # local clock time, no offset, wrong label
            record["event_time_utc"] = event.astimezone(ZoneInfo(tz_of[airport])).strftime("%Y-%m-%d %H:%M:%S")
        copies = 1
        if rng.random() < BAG_DUPLICATE_RATE:
            copies += rng.randint(*BAG_DUPLICATE_EXTRA)
            manifest.append(("baggage", s["scan_id"], "DUPLICATE_EXACT", f"{copies} copies"))
        for i in range(copies):
            files[(airport, day)].append((t + timedelta(seconds=i * 7), record))

    for (airport, day), items in sorted(files.items()):
        rows = [r for _, r in sorted(items, key=lambda x: (x[0], x[1]["scan_id"]))]
        rel = f"baggage/date={day}/bag_scans_{airport}_{day.replace('-', '')}.jsonl.gz"
        writer.jsonl(rel, f"baggage_{airport}", end_of_day(day) + timedelta(hours=2, minutes=1), rows,
                     compress=True)
        if airport in BAG_LOCAL_TIME_AIRPORTS:
            manifest.append(("baggage", rel, "LOCAL_TIME_NO_OFFSET",
                             f"event_time_utc is {tz_of[airport]} local time"))
    return files


# =====================================================
# 3) MAINTENANCE (MRO) - daily CSV of changed defect records
# =====================================================
def maintenance(rng, writer, manifest, events, period_end):
    columns = ["fault_ref", "tail_number", "station", "reported_at", "ata_chapter", "part_no",
               "defect_description", "mel_category", "status", "part_source", "completed_at",
               "record_updated_at"]
    by_day = defaultdict(list)

    def dirty_station(code, key):
        if rng.random() < MRO_DIRTY_STATION_RATE:
            variant = rng.choice([f" {code.lower()}", code.capitalize(), f"{code} ", code.lower()])
            manifest.append(("maintenance", key, "DIRTY_CODE", f"station '{variant}'"))
            return variant
        return code

    for e in events:
        detected = parse_utc(e["detected_utc"])
        completed = parse_utc(e["repair_completed_utc"])
        desc = e["part_description"]
        if rng.random() < MRO_BLANK_DESCRIPTION_RATE:
            desc = ""
            manifest.append(("maintenance", e["fault_id"], "NULL_VALUE", "defect_description blank"))
        mel = rng.choice(MEL_SPELLINGS[e["criticality"]])
        if mel != e["criticality"]:
            manifest.append(("maintenance", e["fault_id"], "DIRTY_CODE", f"mel_category '{mel}'"))
        base = {"fault_ref": e["fault_id"], "tail_number": e["aircraft_registration"],
                "reported_at": uk(detected), "ata_chapter": e["ata_chapter"], "part_no": e["part_number"],
                "defect_description": desc, "mel_category": mel}
        opened = dict(base, station=dirty_station(e["airport_code"], e["fault_id"]), status="OPEN",
                      part_source="", completed_at="", record_updated_at=uk(detected))
        by_day[detected.strftime("%Y-%m-%d")].append(opened)
        if completed and completed <= period_end:          # second version of the same record
            closed = dict(base, station=dirty_station(e["airport_code"], e["fault_id"]), status="CLOSED",
                          part_source=e["part_source"], completed_at=uk(completed),
                          record_updated_at=uk(completed))
            by_day[completed.strftime("%Y-%m-%d")].append(closed)

    for day, rows in sorted(by_day.items()):
        writer.csv(f"maintenance/date={day}/mro_defects_{day.replace('-', '')}.csv", "maintenance",
                   end_of_day(day), columns, rows)


# =====================================================
# 4) SUPPLIER PORTAL + INVENTORY SYSTEM
# =====================================================
def supply_chain(rng, writer, manifest, shipments, pos, snapshots, period_end):
    def columns_for(day, old_cols):
        if day >= SCHEMA_CHANGE_DATE:                       # Incident B: renamed + new column
            return [("component_id" if c == "part_number" else c) for c in old_cols] + ["carrier_name"]
        return old_cols

    def write_daily(kind, by_day, old_cols):
        change_logged = False
        for day, rows in sorted(by_day.items()):
            cols = columns_for(day, old_cols)
            if day >= SCHEMA_CHANGE_DATE:
                for r in rows:
                    r["component_id"] = r["part_number"]
                    r["carrier_name"] = rng.choice(CARRIERS)
            rel = f"supply_chain/{kind}/date={day}/supplier_{kind}_{day.replace('-', '')}.csv"
            writer.csv(rel, "supplier_portal", end_of_day(day), cols, rows)
            if day >= SCHEMA_CHANGE_DATE and not change_logged:      # first file in the new layout
                manifest.append(("supplier_portal", rel, "SCHEMA_CHANGE",
                                 "part_number renamed to component_id; new column carrier_name"))
                change_logged = True

    ship_cols = ["shipment_ref", "po_ref", "part_number", "quantity", "ship_from", "ship_to", "status",
                 "dispatched_at", "promised_at", "delivered_at", "record_updated_at"]
    by_day = defaultdict(list)
    for s in shipments:
        dispatched, planned, actual = (parse_utc(s["dispatched_utc"]), parse_utc(s["planned_arrival_utc"]),
                                       parse_utc(s["actual_arrival_utc"]))
        base = {"shipment_ref": s["shipment_id"], "po_ref": s["purchase_order_id"],
                "part_number": s["part_number"], "quantity": s["quantity"], "ship_from": s["from_location"],
                "ship_to": s["to_airport_code"], "dispatched_at": uk(dispatched), "promised_at": uk(planned)}
        if dispatched <= period_end:
            by_day[dispatched.strftime("%Y-%m-%d")].append(
                dict(base, status="DISPATCHED", delivered_at="", record_updated_at=uk(dispatched)))
        if actual <= period_end:
            by_day[actual.strftime("%Y-%m-%d")].append(
                dict(base, status="DELIVERED", delivered_at=uk(actual), record_updated_at=uk(actual)))
    write_daily("shipments", by_day, ship_cols)

    po_cols = ["po_ref", "order_type", "supplier_id", "part_number", "airline", "deliver_to", "quantity",
               "ordered_at", "promised_at"]
    by_day = defaultdict(list)
    for p in pos:
        ordered = parse_utc(p["ordered_utc"])
        by_day[ordered.strftime("%Y-%m-%d")].append({
            "po_ref": p["purchase_order_id"], "order_type": p["order_type"], "supplier_id": p["supplier_id"],
            "part_number": p["part_number"], "airline": p["airline_code"],
            "deliver_to": p["deliver_to_airport_code"], "quantity": p["quantity"],
            "ordered_at": uk(ordered), "promised_at": uk(parse_utc(p["promised_delivery_utc"]))})
    write_daily("purchase_orders", by_day, po_cols)

    inv_cols = ["snapshot_utc", "airline_code", "airport_code", "part_number", "quantity_on_hand"]
    by_day = defaultdict(list)
    for r in snapshots:
        by_day[r["snapshot_utc"][:10]].append(r)
    for day, rows in sorted(by_day.items()):
        folder = f"supply_chain/inventory/date={day}"
        writer.csv(f"{folder}/inventory_{day.replace('-', '')}.csv", "inventory_system",
                   end_of_day(day), inv_cols, rows)
        if day == INVENTORY_RESENT_DATE:                      # whole file delivered a second time
            rel = f"{folder}/inventory_{day.replace('-', '')}_resend.csv"
            writer.csv(rel, "inventory_system", end_of_day(day) + timedelta(days=2), inv_cols, rows)
            manifest.append(("inventory_system", rel, "FILE_RESENT", f"duplicate of inventory_{day}"))


# =====================================================
# 5) RESERVATIONS + DEPARTURE CONTROL (synthetic PII)
# =====================================================
def passengers(rng, writer, manifest, pax, segments, schedule_by_id, actual_by_id, tz_of):
    def local_ref(fid):
        s = schedule_by_id[fid]
        dep_local = parse_utc(s["scheduled_departure_utc"]).astimezone(ZoneInfo(tz_of[s["origin_airport_code"]]))
        return s, dep_local.strftime("%Y-%m-%d")

    segs_by_ref = defaultdict(list)
    for sg in segments:
        segs_by_ref[sg["booking_ref"]].append(sg)

    book_cols = ["booking_ref", "passenger_id", "passenger_name", "email", "frequent_flyer_tier", "cabin",
                 "airline", "origin", "destination", "is_connecting"]
    seg_cols = ["booking_ref", "segment_no", "flight_number", "flight_date_local", "origin", "destination"]
    board_cols = ["booking_ref", "flight_number", "flight_date_local", "origin", "boarded_at_utc"]
    bookings, seg_rows, boarded = defaultdict(list), defaultdict(list), defaultdict(list)

    for p in pax:
        segs = sorted(segs_by_ref[p["booking_ref"]], key=lambda x: int(x["segment_no"]))
        s0, day0 = local_ref(segs[0]["planned_flight_id"])
        name, email = p["passenger_name"], p["email"]
        if rng.random() < EMAIL_UPPER_RATE:
            email = email.upper()
            manifest.append(("reservations", p["booking_ref"], "DIRTY_VALUE", "email upper case"))
        if rng.random() < NAME_SPACES_RATE:
            name = "  " + name.replace(" ", "  ") + " "
            manifest.append(("reservations", p["booking_ref"], "DIRTY_VALUE", "extra spaces in name"))
        bookings[day0].append({"booking_ref": p["booking_ref"], "passenger_id": p["passenger_id"],
                               "passenger_name": name, "email": email,
                               "frequent_flyer_tier": p["frequent_flyer_tier"], "cabin": p["cabin"],
                               "airline": p["airline_code"], "origin": p["origin_airport_code"],
                               "destination": p["final_destination_code"],
                               "is_connecting": p["is_connecting"]})
        for sg in segs:
            s, day = local_ref(sg["planned_flight_id"])
            seg_rows[day0].append({"booking_ref": p["booking_ref"], "segment_no": sg["segment_no"],
                                   "flight_number": s["flight_number"], "flight_date_local": day,
                                   "origin": s["origin_airport_code"],
                                   "destination": s["destination_airport_code"]})
            if sg["actual_flight_id"]:
                s_act, day_act = local_ref(sg["actual_flight_id"])
                dep = parse_utc(actual_by_id[sg["actual_flight_id"]]["actual_departure_utc"])
                boarded[day_act].append({"booking_ref": p["booking_ref"], "flight_number": s_act["flight_number"],
                                         "flight_date_local": day_act, "origin": s_act["origin_airport_code"],
                                         "boarded_at_utc": iso(dep - timedelta(minutes=rng.randint(15, 40)))})

    for day in sorted(bookings):
        writer.csv(f"reservations/date={day}/bookings_{day.replace('-', '')}.csv", "reservations",
                   end_of_day(day), book_cols, bookings[day])
        writer.csv(f"reservations/date={day}/segments_{day.replace('-', '')}.csv", "reservations",
                   end_of_day(day), seg_cols, seg_rows[day])
    for day in sorted(boarded):
        writer.csv(f"departure_control/date={day}/boarded_{day.replace('-', '')}.csv", "departure_control",
                   end_of_day(day), board_cols, boarded[day])


# =====================================================
# 6) EXTERNAL WEATHER SERVICE - JSON array, Unix epoch seconds
# =====================================================
def weather(writer, events):
    by_day = defaultdict(list)
    for e in events:
        start, end = parse_utc(e["start_utc"]), parse_utc(e["end_utc"])
        by_day[start.strftime("%Y-%m-%d")].append({
            "alert_id": e["weather_event_id"], "station_iata": e["airport_code"],
            "severity_level": {"MODERATE": 2, "SEVERE": 3}[e["severity"]],
            "valid_from_epoch": int(start.timestamp()), "valid_to_epoch": int(end.timestamp())})
    for day, rows in sorted(by_day.items()):
        first_alert = min(datetime.fromtimestamp(r["valid_from_epoch"], UTC) for r in rows)
        writer.json_array(f"external/weather/date={day}/weather_alerts_{day.replace('-', '')}.json",
                          "external_weather", first_alert - timedelta(hours=2), rows)   # issued as a forecast


# =====================================================
# RECONCILIATION - every truth record is accounted for
# =====================================================
def reconcile(fos_messages, fos_sent, scans, bag_files, manifest):
    problems = []
    kinds = Counter((m[0], m[2]) for m in manifest)

    # flight ops: every original message delivered at least once; row count explained by duplicates
    delivered_ids = {m["message_id"] for _, m in fos_sent}
    missing = [m["message_id"] for m in fos_messages if m["message_id"] not in delivered_ids]
    if missing:
        problems.append(f"flight_ops: {len(missing)} messages never delivered")
    expected = (len(fos_messages) + kinds[("flight_ops", "DUPLICATE_EXACT")]
                + kinds[("flight_ops", "DUPLICATE_NEW_ID")])
    if len(fos_sent) != expected:
        problems.append(f"flight_ops: {len(fos_sent)} rows, expected {expected}")

    # baggage: raw rows = truth - dropped - lost + extra duplicate copies
    raw_rows = sum(len(v) for v in bag_files.values())
    extra = sum(int(m[3].split()[0]) - 1 for m in manifest if m[0] == "baggage" and m[2] == "DUPLICATE_EXACT")
    expected = (len(scans) - kinds[("baggage", "DROPPED_SCAN")]
                - kinds[("baggage", "VOLUME_COLLAPSE_LOST")] + extra)
    if raw_rows != expected:
        problems.append(f"baggage: {raw_rows} rows, expected {expected}")

    if problems:
        raise ValueError("Raw feed reconciliation failed:\n  " + "\n  ".join(problems))
    print("RECONCILIATION PASSED (every truth record is delivered or recorded in the manifest)")


# =====================================================
# MAIN
# =====================================================
def upload_tree(session, writer):
    """One PUT per folder (PUT does not recurse into sub-folders)."""
    folders = sorted({str(Path(f["file_path"]).parent) for f in writer.delivery_log} | {"_control"})
    for folder in folders:
        session.file.put(str(RAW_LOCAL / folder / "*"), f"{RAW_STAGE}{folder}/",
                         auto_compress=False, overwrite=True)
    print(f"UP  {len(writer.delivery_log)} raw files in {len(folders)} folders -> {RAW_STAGE}")


def main():
    session = get_active_session()
    rng = random.Random(SEED)
    IN_LOCAL.mkdir(parents=True, exist_ok=True)
    if RAW_LOCAL.exists():
        import shutil
        shutil.rmtree(RAW_LOCAL)
    RAW_LOCAL.mkdir(parents=True)

    airports = load_csv(session, REF_STAGE, "airports.csv")
    schedule = load_csv(session, TRUTH_STAGE, "flights_schedule.csv")
    actuals = load_csv(session, TRUTH_STAGE, "flights_actual.csv")
    delays = load_csv(session, TRUTH_STAGE, "flight_delays.csv")
    weather_events = load_csv(session, TRUTH_STAGE, "weather_events.csv")
    mro_events = load_csv(session, TRUTH_STAGE, "maintenance_events.csv")
    shipments = load_csv(session, TRUTH_STAGE, "shipments.csv")
    pos = load_csv(session, TRUTH_STAGE, "purchase_orders.csv")
    snapshots = load_csv(session, TRUTH_STAGE, "inventory_snapshots.csv")
    pax = load_csv(session, TRUTH_STAGE, "passengers.csv")
    segments = load_csv(session, TRUTH_STAGE, "passenger_segments.csv")
    scans = load_jsonl_gz(session, TRUTH_STAGE, "bag_scans.jsonl.gz")

    tz_of = {a["airport_code"]: a["timezone"] for a in airports}
    actual_by_id = {a["flight_id"]: a for a in actuals}
    schedule_by_id = {s["flight_id"]: s for s in schedule}
    delays_by_flight = defaultdict(list)
    for d in delays:
        delays_by_flight[d["flight_id"]].append(d)
    period_end = max(parse_utc(s["scheduled_departure_utc"]) for s in schedule) + timedelta(days=1)

    writer, manifest = RawWriter(), []
    fos_messages, fos_sent = flight_ops(rng, writer, manifest, schedule, actual_by_id, delays_by_flight, tz_of)
    bag_files = baggage(rng, writer, manifest, scans, tz_of)
    maintenance(rng, writer, manifest, mro_events, period_end)
    supply_chain(rng, writer, manifest, shipments, pos, snapshots, period_end)
    passengers(rng, writer, manifest, pax, segments, schedule_by_id, actual_by_id, tz_of)
    weather(writer, weather_events)
    reconcile(fos_messages, fos_sent, scans, bag_files, manifest)

    # ---- control file + manifest ----
    writer.delivery_log.sort(key=lambda f: (f["delivered_utc"], f["file_path"]))
    ctrl = RAW_LOCAL / "_control" / "delivery_log.csv"
    ctrl.parent.mkdir(parents=True, exist_ok=True)
    with open(ctrl, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["file_path", "source_system", "delivered_utc", "record_count"])
        w.writeheader()
        w.writerows(writer.delivery_log)

    man_path = IN_LOCAL / "corruption_manifest.csv"
    with open(man_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["manifest_id", "source_system", "record_or_file", "corruption_type", "detail"])
        for i, (system, key, kind, detail) in enumerate(manifest, start=1):
            w.writerow([f"CM-{i:06d}", system, key, kind, detail])

    # ---- summary ----
    print("\nRAW FILES PER SOURCE SYSTEM:")
    files_by_sys, rows_by_sys = Counter(), Counter()
    for f in writer.delivery_log:
        sys_name = "baggage" if f["source_system"].startswith("baggage_") else f["source_system"]
        files_by_sys[sys_name] += 1
        rows_by_sys[sys_name] += f["record_count"]
    for sys_name in sorted(files_by_sys):
        print(f"  {sys_name:<18} {files_by_sys[sys_name]:>4} files  {rows_by_sys[sys_name]:>8} records")
    print("\nCORRUPTIONS INTRODUCED (see truth/corruption_manifest.csv):")
    for (system, kind), n in sorted(Counter((m[0], m[2]) for m in manifest).items()):
        print(f"  {system:<18} {kind:<30} {n:>6}")

    # ---- upload: wipe the old landing zone first so re-runs never leave stale files ----
    session.sql(f"REMOVE {RAW_STAGE}").collect()
    upload_tree(session, writer)
    session.file.put(str(man_path), TRUTH_STAGE, auto_compress=False, overwrite=True)
    print(f"UP  corruption_manifest.csv  {len(manifest)} rows -> {TRUTH_STAGE}")


main()