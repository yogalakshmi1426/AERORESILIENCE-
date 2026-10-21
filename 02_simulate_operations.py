"""
AERORESILIENCE - Operations simulator (Snowflake)
02  3a  FLIGHT SCHEDULE  (the plan: aircraft rotations)
    3b  ACTUALS          (what happened: delays, propagation, cancellations)
    3b.1 CALIBRATION     (realistic recovery: hub ground time, tail swaps,
                          weather capacity queues, proactive cancellations)
    3c  MAINTENANCE & SUPPLY CHAIN (faults, MEL, spare-part stock, AOG shipments,
                          purchase orders, hourly inventory snapshots)

Reads  : @AERORESILIENCE.BRONZE.RAW_STAGE/reference/   (airports, airlines, aircraft, routes,
                                                         parts, suppliers)
Writes : @AERORESILIENCE.BRONZE.RAW_STAGE/truth/
           flights_schedule.csv  grain: one scheduled flight (flight number + UTC departure date)
           flights_actual.csv    grain: one flight's actual outcome
           flight_delays.csv     grain: one delay cause for one flight (one-to-many)
           weather_events.csv    grain: one weather disruption at one airport
           tail_swaps.csv        grain: one aircraft-swap decision
           maintenance_events.csv   grain: one fault found on one aircraft
           shipments.csv            grain: one movement of parts to an airport
           purchase_orders.csv      grain: one order placed with a supplier
           inventory_snapshots.csv  grain: one part, one airline store, one hour (periodic snapshot)

Rules:
  * All calculations are done in UTC. Local times are added for display only.
  * The simulated week includes the European clock change (25 Oct 2026).
  * The schedule holds the PLANNED aircraft and links; flights_actual holds the
    ACTUAL aircraft and links (they differ after tail swaps).
  * Delay codes are based on IATA standard delay codes. Probabilities are simulation settings.
"""

import csv
import random
from collections import Counter, defaultdict
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo
from snowflake.snowpark.context import get_active_session

# =====================================================
# SETTINGS - schedule (3a)
# =====================================================
START_DATE = date(2026, 10, 21)          # week includes the 25 Oct clock change
NUM_DAYS = 7
SCHEDULED_TURNAROUND_MINUTES = {         # PLANNED ground time
    "HUB":        {"NARROW": 45, "WIDE": 180},   # long-haul rests at the hub: absorbs delays
    "OUTSTATION": {"NARROW": 45, "WIDE": 90},
}
OPS_START_LOCAL = time(6, 0)
OPS_END_LOCAL = time(22, 0)
AIRCRAFT_STAGGER_MINUTES = 25

# =====================================================
# SETTINGS - actuals (3b)
# =====================================================
SEED = 42
MIN_TURNAROUND_MINUTES = {"NARROW": 35, "WIDE": 75}   # fastest possible turn on a late day
ON_TIME_THRESHOLD_MINUTES = 15                         # industry standard: < 15 min late = on time
CANCEL_AFTER_MINUTES = 180                             # narrow-body round trip cancelled beyond this

NUM_WEATHER_EVENTS = 4
HOLDING_MINUTES = (10, 30)               # extra airborne time arriving into bad weather

EUROPEAN_AIRPORTS = {"LHR", "MAN", "AMS", "CDG", "FRA"}
ATC_PROBABILITY = {"EUROPE": 0.08, "OTHER": 0.03}       # busy European airspace = more restrictions
ATC_MINUTES = (5, 40)
RAMP_PROBABILITY = 0.05
RAMP_MINUTES = (5, 25)
AIRBORNE_VARIATION = {"NARROW": (-8, 12), "WIDE": (-15, 20)}   # winds, routing

# =====================================================
# SETTINGS - recovery & capacity (3b.1)
# =====================================================
WEATHER_CAPACITY = {"MODERATE": 0.80, "SEVERE": 0.50}  # share of normal runway capacity still available
QUEUE_DRAIN_RATE = 0.25                  # after weather clears: wait shrinks 0.25 min per minute (spare capacity)
PROACTIVE_CANCEL_RATE = 0.30             # share of short-haul round trips cancelled in advance (severe, hub)
STANDBY_AIRCRAFT = {"BA": "NARROW", "KL": "NARROW"}   # airline -> body type of ONE spare kept at the hub
SWAP_CONSIDER_MINUTES = 30               # only look for a swap if the aircraft is this late
SWAP_MIN_SAVING_MINUTES = 30             # only swap if it saves at least this much delay in total

# =====================================================
# SETTINGS - maintenance & supply chain (3c)
# =====================================================
BASE_FAULT_PROBABILITY = 0.02           # chance a fault is found after a flight (new aircraft)
AGE_FACTOR_YEARS = 15                    # +100% fault chance per 15 years of aircraft age
SIM_YEAR = 2026
CRITICALITY_WEIGHTS = {"GO": 0.50, "GO_IF": 0.38, "NO_GO": 0.12}   # how often each kind of fault occurs
REPAIR_HOURS = {"NO_GO": (2, 5), "GO_IF": (2, 4), "GO": (1, 2)}
MEL_GO_IF_DEADLINE_HOURS = 72            # a GO_IF defect must be fixed within this time
AOG_HANDLING_HOURS = 2                   # pick, pack and hand over a part from our own store
AOG_SUPPLIER_PROCESSING_HOURS = (4, 12)  # supplier's AOG desk: time until the part is dispatched
INTERNAL_ON_TIME_RATE = 0.95             # our own courier transfers
INTERNAL_LATE_HOURS = (3, 10)
SUPPLIER_AOG_LATE_HOURS = (6, 24)        # when a supplier misses its AOG promise
SUPPLIER_REPLENISH_LATE_DAYS = (1, 5)    # when a supplier misses a normal delivery date
COURIER_HANDLING_HOURS = 4              # "next flight out": flight time + handling/customs
NO_DIRECT_FLIGHT_HOURS = 20              # if no route links the two airports (connection needed)

DELAY_CODES = {
    "32": "Ramp handling: loading/unloading",
    "41": "Aircraft defect (technical)",
    "71": "Weather at departure airport",
    "81": "Air traffic flow restriction (en-route)",
    "93": "Reactionary: aircraft late from previous flight",
}

# =====================================================
# LOCATIONS
# =====================================================
REF_STAGE = "@AERORESILIENCE.BRONZE.RAW_STAGE/reference/"
OUT_STAGE = "@AERORESILIENCE.BRONZE.RAW_STAGE/truth/"
REF_LOCAL = Path("/tmp/aeroresilience/reference_in")
OUT_LOCAL = Path("/tmp/aeroresilience/truth")
UTC = timezone.utc

SCHEDULE_COLUMNS = [
    "flight_id", "flight_number", "airline_code", "route_id",
    "origin_airport_code", "destination_airport_code", "aircraft_registration",
    "rotation_seq", "previous_flight_id",
    "scheduled_departure_utc", "scheduled_arrival_utc",
    "scheduled_departure_local", "scheduled_arrival_local",
    "block_time_minutes",
]
ACTUAL_COLUMNS = [
    "flight_id", "flight_status", "actual_aircraft_registration", "actual_previous_flight_id",
    "actual_departure_utc", "actual_arrival_utc",
    "departure_delay_minutes", "arrival_delay_minutes", "cancellation_reason",
]
DELAY_COLUMNS = ["flight_id", "delay_code", "delay_description", "delay_minutes"]
WEATHER_COLUMNS = ["weather_event_id", "airport_code", "severity", "start_utc", "end_utc"]
MAINTENANCE_COLUMNS = [
    "fault_id", "aircraft_registration", "airline_code", "detected_after_flight_id", "airport_code",
    "detected_utc", "part_number", "part_description", "ata_chapter", "criticality", "action",
    "mel_deadline_utc", "part_source", "part_available_utc", "repair_completed_utc", "status",
]
SHIPMENT_COLUMNS = [
    "shipment_id", "shipment_type", "part_number", "quantity", "from_location", "to_airport_code",
    "airline_code", "fault_id", "purchase_order_id",
    "dispatched_utc", "planned_arrival_utc", "actual_arrival_utc",
]
PO_COLUMNS = [
    "purchase_order_id", "order_type", "supplier_id", "part_number", "airline_code",
    "deliver_to_airport_code", "quantity", "ordered_utc", "promised_delivery_utc", "actual_delivery_utc",
]
SNAPSHOT_COLUMNS = ["snapshot_utc", "airline_code", "airport_code", "part_number", "quantity_on_hand"]
SWAP_COLUMNS = ["swap_id", "swap_type", "airport_code", "swap_time_utc", "flight_id", "late_aircraft",
                "replacement_aircraft", "other_flight_id", "minutes_saved"]


# =====================================================
# READ REFERENCE DATA FROM THE STAGE (output of step 01)
# =====================================================
def load_reference(session, filename):
    session.file.get(REF_STAGE + filename, str(REF_LOCAL))
    with open(REF_LOCAL / filename, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    print(f"IN  {filename:<14} {len(rows):>4} rows")
    return rows


# =====================================================
# TIME HELPERS
# =====================================================
def local_to_utc(day, clock_time, tz_name):
    return datetime.combine(day, clock_time, tzinfo=ZoneInfo(tz_name)).astimezone(UTC)


def round_up_5(dt):
    extra = timedelta(minutes=dt.minute % 5, seconds=dt.second, microseconds=dt.microsecond)
    return dt if extra == timedelta(0) else dt - extra + timedelta(minutes=5)


def within_operating_hours(dep_utc, tz_name):
    local = dep_utc.astimezone(ZoneInfo(tz_name))
    if local.time() < OPS_START_LOCAL:
        return local_to_utc(local.date(), OPS_START_LOCAL, tz_name)
    if local.time() > OPS_END_LOCAL:
        return local_to_utc(local.date() + timedelta(days=1), OPS_START_LOCAL, tz_name)
    return dep_utc


def fmt_utc(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def fmt_local(dt, tz_name):
    return dt.astimezone(ZoneInfo(tz_name)).strftime("%Y-%m-%d %H:%M")


def minutes_between(later, earlier):
    return int((later - earlier).total_seconds() // 60)


# =====================================================
# 3a  SCHEDULE
# =====================================================
def pick_standby(aircraft):
    """The LAST aircraft of the chosen body type for each airline is kept as a spare (no schedule)."""
    standby = set()
    for airline_code, body in STANDBY_AIRCRAFT.items():
        matching = [a["registration"] for a in aircraft
                    if a["airline_code"] == airline_code and a["body_type"] == body]
        if len(matching) > 1:
            standby.add(matching[-1])
    return standby


def assign_flight_number(airline_code, start_number, dep_utc, used_ids, origin, origin_tz):
    """
    A flight number must be unique per UTC date (our flight_id) AND per origin + LOCAL date
    (how airlines and their operational systems identify a flight). Extra frequencies get +200.
    """
    local_day = dep_utc.astimezone(ZoneInfo(origin_tz)).strftime("%Y%m%d")

    def keys(n):
        return (f"{airline_code}{n}-{dep_utc:%Y%m%d}", f"{airline_code}{n}-{origin}-L{local_day}")

    number = start_number
    while any(k in used_ids for k in keys(number)):
        number += 200
    used_ids.update(keys(number))
    return number


def make_flight(airline_code, number, route, aircraft, dep, arr, seq, prev_id, tz_of):
    origin = route["origin_airport_code"]
    dest = route["destination_airport_code"]
    flight_number = f"{airline_code}{number}"
    return {
        "flight_id": f"{flight_number}-{dep:%Y%m%d}",
        "flight_number": flight_number,
        "airline_code": airline_code,
        "route_id": route["route_id"],
        "origin_airport_code": origin,
        "destination_airport_code": dest,
        "aircraft_registration": aircraft["registration"],
        "rotation_seq": seq,
        "previous_flight_id": prev_id,
        "scheduled_departure_utc": fmt_utc(dep),
        "scheduled_arrival_utc": fmt_utc(arr),
        "scheduled_departure_local": fmt_local(dep, tz_of[origin]),
        "scheduled_arrival_local": fmt_local(arr, tz_of[dest]),
        "block_time_minutes": int(route["block_time_minutes"]),
        "_dep": dep, "_arr": arr, "_body": aircraft["body_type"],
    }


def build_schedule(airports, airlines, aircraft, routes):
    tz_of = {a["airport_code"]: a["timezone"] for a in airports}
    route_by_id = {r["route_id"]: r for r in routes}
    flights, used_ids = [], set()

    for airline in airlines:
        code, hub = airline["airline_code"], airline["hub_airport_code"]
        hub_tz = tz_of[hub]
        horizon_start = local_to_utc(START_DATE, OPS_START_LOCAL, hub_tz)
        horizon_end = local_to_utc(START_DATE + timedelta(days=NUM_DAYS), time(0, 0), hub_tz)

        outbound = sorted(
            (r for r in routes if r["airline_code"] == code and r["origin_airport_code"] == hub),
            key=lambda r: r["route_id"],
        )
        base_number = {r["route_id"]: 100 + 2 * i for i, r in enumerate(outbound)}
        by_body = defaultdict(list)
        for r in outbound:
            by_body[r["required_body_type"]].append(r)
        next_route = defaultdict(int)

        standby = pick_standby(aircraft)
        fleet = [a for a in aircraft if a["airline_code"] == code and a["registration"] not in standby]
        for k, ac in enumerate(fleet):
            body = ac["body_type"]
            candidates = by_body.get(body, [])
            if not candidates:
                continue
            hub_turn = timedelta(minutes=SCHEDULED_TURNAROUND_MINUTES["HUB"][body])
            out_turn = timedelta(minutes=SCHEDULED_TURNAROUND_MINUTES["OUTSTATION"][body])
            t = horizon_start + timedelta(minutes=AIRCRAFT_STAGGER_MINUTES * k)
            seq, prev_id = 0, ""

            while True:
                dep = round_up_5(t)
                if body == "NARROW":
                    dep = round_up_5(within_operating_hours(dep, hub_tz))
                if dep >= horizon_end:
                    break

                out_route = candidates[next_route[body] % len(candidates)]
                next_route[body] += 1
                spoke = out_route["destination_airport_code"]
                ret_route = route_by_id[f"{code}-{spoke}-{hub}"]

                out_arr = dep + timedelta(minutes=int(out_route["block_time_minutes"]))
                out_num = assign_flight_number(code, base_number[out_route["route_id"]], dep, used_ids,
                                               hub, tz_of[hub])
                seq += 1
                f_out = make_flight(code, out_num, out_route, ac, dep, out_arr, seq, prev_id, tz_of)
                flights.append(f_out)
                prev_id = f_out["flight_id"]

                ret_dep = round_up_5(out_arr + out_turn)
                ret_arr = ret_dep + timedelta(minutes=int(ret_route["block_time_minutes"]))
                ret_num = assign_flight_number(code, out_num + 1, ret_dep, used_ids,
                                               spoke, tz_of[spoke])
                seq += 1
                f_ret = make_flight(code, ret_num, ret_route, ac, ret_dep, ret_arr, seq, prev_id, tz_of)
                flights.append(f_ret)
                prev_id = f_ret["flight_id"]

                t = ret_arr + hub_turn

    return flights


def validate_schedule(flights):
    problems = []
    ids = [f["flight_id"] for f in flights]
    if len(ids) != len(set(ids)):
        problems.append("Duplicate flight_id found")

    local_keys = [(f["flight_number"], f["origin_airport_code"], f["scheduled_departure_local"][:10])
                  for f in flights]
    if len(local_keys) != len(set(local_keys)):
        problems.append("Flight number + origin + LOCAL date is not unique (operational systems cannot "
                        "tell these flights apart)")

    by_tail = defaultdict(list)
    for f in flights:
        if f["_arr"] <= f["_dep"]:
            problems.append(f"{f['flight_id']}: arrives before it departs")
        by_tail[f["aircraft_registration"]].append(f)

    for reg, legs in by_tail.items():
        legs.sort(key=lambda f: f["rotation_seq"])
        for prev, cur in zip(legs, legs[1:]):
            where = "HUB" if cur["rotation_seq"] % 2 == 1 else "OUTSTATION"   # odd legs leave the hub
            if cur["origin_airport_code"] != prev["destination_airport_code"]:
                problems.append(f"{reg}: TELEPORT at {cur['flight_id']}")
            if cur["_dep"] < prev["_arr"] + timedelta(minutes=SCHEDULED_TURNAROUND_MINUTES[where][cur["_body"]]):
                problems.append(f"{reg}: turnaround too short before {cur['flight_id']}")
            if cur["previous_flight_id"] != prev["flight_id"]:
                problems.append(f"{reg}: broken previous_flight_id link at {cur['flight_id']}")

    if problems:
        raise ValueError("Schedule validation failed:\n  " + "\n  ".join(problems[:20]))
    print(f"SCHEDULE VALIDATION PASSED ({len(flights)} flights)")


# =====================================================
# 3b  WEATHER EVENTS
# =====================================================
def generate_weather_events(rng, airports, airlines):
    hubs = [a["hub_airport_code"] for a in airlines]
    all_airports = [a["airport_code"] for a in airports]
    week_start = datetime.combine(START_DATE, time(0, 0), tzinfo=UTC)
    week_minutes = NUM_DAYS * 24 * 60

    events = []
    for i in range(NUM_WEATHER_EVENTS):
        # first event: guaranteed SEVERE at a hub, so there is a real disruption to analyse
        airport = rng.choice(hubs) if i == 0 else rng.choice(all_airports)
        severity = "SEVERE" if i == 0 else rng.choice(["MODERATE", "MODERATE", "SEVERE"])
        start = week_start + timedelta(minutes=rng.randrange(6 * 60, week_minutes - 12 * 60, 30))
        end = start + timedelta(hours=rng.randint(3, 8))
        events.append({
            "weather_event_id": f"WX-{i + 1:03d}",
            "airport_code": airport,
            "severity": severity,
            "start_utc": fmt_utc(start),
            "end_utc": fmt_utc(end),
            "_start": start, "_end": end,
        })
    return events


def weather_at(events_by_airport, airport, moment):
    for e in events_by_airport.get(airport, []):
        if e["_start"] <= moment < e["_end"]:
            return e
    return None


def weather_queue_wait(events_by_airport, airport, moment, hubs):
    """
    Fluid queue model of an airport in bad weather.
      During the event : demand > capacity, so the queue (and the wait) GROWS linearly.
      After the event  : spare capacity DRAINS the queue gradually.
    Proactive cancellations at a hub reduce demand, so the queue grows more slowly.
    Returns the wait in minutes for a flight ready to depart at `moment`.
    """
    worst = 0.0
    for e in events_by_airport.get(airport, []):
        if moment < e["_start"]:
            continue
        capacity = WEATHER_CAPACITY[e["severity"]]
        demand = 1 - PROACTIVE_CANCEL_RATE if (e["severity"] == "SEVERE" and airport in hubs) else 1.0
        growth = max(0.0, demand - capacity) / capacity          # minutes of wait added per minute
        if moment < e["_end"]:
            wait = growth * minutes_between(moment, e["_start"])
        else:
            peak = growth * minutes_between(e["_end"], e["_start"])
            wait = max(0.0, peak - QUEUE_DRAIN_RATE * minutes_between(moment, e["_end"]))
        worst = max(worst, wait)
    return worst



# =====================================================
# 3c  SUPPLY CHAIN - spare-part stock kept as a LEDGER of +/- movements
# =====================================================
def build_courier_times(routes):
    """Shortest flight time between two airports (either direction), from the route network."""
    best = {}
    for r in routes:
        a, b, mins = r["origin_airport_code"], r["destination_airport_code"], int(r["block_time_minutes"])
        best[(a, b)] = min(best.get((a, b), mins), mins)
    return best


COURIER_MINUTES = {}       # filled in main() from routes.csv


def transit_hours(from_airport, to_airport):
    """Next-flight-out courier: real flight time + handling; longer if no direct flight exists."""
    if from_airport == to_airport:
        return 0
    mins = COURIER_MINUTES.get((from_airport, to_airport))
    if mins is None:
        return NO_DIRECT_FLIGHT_HOURS
    return round(mins / 60) + COURIER_HANDLING_HOURS


class SupplyChain:
    """
    Stock is owned per (airline, airport, part). Instead of one changing number, every movement
    is recorded in a ledger: (time, +qty / -qty, reason). Stock at any moment = sum of movements
    up to that moment. This lets us answer "what was in stock at 14:00 on Thursday?" exactly.
    """

    def __init__(self, rng, parts, suppliers, airlines, aircraft, week_start):
        self.rng = rng
        self.part = {p["part_number"]: p for p in parts}
        self.supplier = {s["supplier_id"]: s for s in suppliers}
        self.hub_of = {a["airline_code"]: a["hub_airport_code"] for a in airlines}
        self.ledger = defaultdict(list)          # key -> [(time, delta, reason)]
        self.initial, self.reorder_point = {}, {}
        self.shipments, self.purchase_orders = [], []

        fleet_types = defaultdict(set)
        for a in aircraft:
            fleet_types[a["airline_code"]].add(a["icao_type"])

        # Each airline stocks parts for ITS OWN aircraft types at ITS OWN hub. Outstations hold nothing.
        for airline_code in sorted(self.hub_of):
            hub = self.hub_of[airline_code]
            for p in parts:
                if p["icao_type"] not in fleet_types[airline_code]:
                    continue
                key = (airline_code, hub, p["part_number"])
                qty = self.initial_quantity(p)
                self.initial[key] = qty
                self.reorder_point[key] = 1 if qty >= 1 else 0
                if qty:
                    self.ledger[key].append((week_start - timedelta(days=1), qty, "OPENING_STOCK"))

    def initial_quantity(self, p):
        cost, crit = float(p["unit_cost_gbp"]), p["criticality"]
        if crit == "GO":
            return self.rng.randint(2, 5)                  # cheap cabin items: keep plenty
        if crit == "NO_GO":
            return self.rng.randint(2, 4) if cost < 20000 else self.rng.randint(0, 2)
        return self.rng.randint(1, 3) if cost < 30000 else self.rng.randint(0, 1)

    # ---- ledger maths ----
    def _sorted(self, key):
        return sorted(self.ledger[key], key=lambda m: (m[0], -m[1]))   # receipts before issues at same time

    def available_at(self, key, moment):
        """Units we can take at `moment` without making stock negative at ANY later time."""
        movements = self._sorted(key)
        level = sum(d for t, d, _ in movements if t <= moment)
        available, running = level, level
        for t, d, _ in movements:
            if t > moment:
                running += d
                available = min(available, running)
        return available

    def inventory_position(self, key):
        """On hand + on order (all movements, including deliveries not yet arrived)."""
        return sum(d for _, d, _ in self.ledger[key])

    def level_at(self, key, moment):
        return sum(d for t, d, _ in self.ledger[key] if t <= moment)

    def move(self, key, moment, delta, reason):
        self.ledger[key].append((moment, delta, reason))

    # ---- documents ----
    def add_po(self, order_type, supplier_id, part_no, airline, deliver_to, qty, ordered, promised, actual):
        po = {
            "purchase_order_id": f"PO-{len(self.purchase_orders) + 1:04d}", "order_type": order_type,
            "supplier_id": supplier_id, "part_number": part_no, "airline_code": airline,
            "deliver_to_airport_code": deliver_to, "quantity": qty,
            "ordered_utc": fmt_utc(ordered), "promised_delivery_utc": fmt_utc(promised),
            "actual_delivery_utc": fmt_utc(actual),
            "_ordered": ordered, "_actual": actual,
        }
        self.purchase_orders.append(po)
        return po

    def add_shipment(self, shipment_type, part_no, qty, from_loc, to_airport, airline, fault_id, po_id,
                     dispatched, planned, actual):
        sh = {
            "shipment_id": f"SH-{len(self.shipments) + 1:04d}", "shipment_type": shipment_type,
            "part_number": part_no, "quantity": qty, "from_location": from_loc,
            "to_airport_code": to_airport, "airline_code": airline, "fault_id": fault_id,
            "purchase_order_id": po_id, "dispatched_utc": fmt_utc(dispatched),
            "planned_arrival_utc": fmt_utc(planned), "actual_arrival_utc": fmt_utc(actual),
            "_dispatched": dispatched, "_planned": planned, "_actual": actual,
        }
        self.shipments.append(sh)
        return sh

    # ---- replenishment: keep hub stores topped up ----
    def check_reorder(self, key, moment):
        if self.initial.get(key, 0) == 0:
            return
        position = self.inventory_position(key)
        if position >= self.reorder_point[key]:
            return
        airline, airport, part_no = key
        qty = self.initial[key] - position                          # order back up to the opening level
        p = self.part[part_no]
        sup = self.supplier[p["supplier_id"]]
        promised = moment + timedelta(days=int(p["standard_lead_time_days"]))
        actual = promised
        if self.rng.random() > float(sup["on_time_rate"]):
            actual = promised + timedelta(days=self.rng.randint(*SUPPLIER_REPLENISH_LATE_DAYS))
        po = self.add_po("REPLENISHMENT", sup["supplier_id"], part_no, airline, airport, qty,
                         moment, promised, actual)
        self.add_shipment("REPLENISHMENT", part_no, qty, sup["supplier_id"], airport, airline, "",
                          po["purchase_order_id"], actual - timedelta(hours=transit_hours(
                              sup["ship_from_airport_code"], airport)), promised, actual)
        self.move(key, actual, +qty, f"RECEIVED {po['purchase_order_id']}")

    # ---- order a part for a DEFERRED defect so it reaches the hub before the MEL deadline ----
    def order_for_deferred_defect(self, airline, part_no, moment, fault_id, urgent):
        hub = self.hub_of[airline]
        key = (airline, hub, part_no)
        if self.available_at(key, moment) >= 1:
            return None                                             # hub already has one
        p = self.part[part_no]
        sup = self.supplier[p["supplier_id"]]
        if urgent:      # GO_IF: use the supplier's expedite service so it beats the deadline
            dispatch = moment + timedelta(hours=self.rng.randint(*AOG_SUPPLIER_PROCESSING_HOURS))
            promised = dispatch + timedelta(hours=transit_hours(sup["ship_from_airport_code"], hub))
            late = timedelta(hours=self.rng.randint(*SUPPLIER_AOG_LATE_HOURS))
        else:           # GO: normal lead time is fine
            promised = moment + timedelta(days=int(p["standard_lead_time_days"]))
            dispatch = promised - timedelta(hours=transit_hours(sup["ship_from_airport_code"], hub))
            late = timedelta(days=self.rng.randint(*SUPPLIER_REPLENISH_LATE_DAYS))
        actual = promised + (late if self.rng.random() > float(sup["on_time_rate"]) else timedelta(0))
        order_type = "DEFERRED_DEFECT_URGENT" if urgent else "DEFERRED_DEFECT"
        po = self.add_po(order_type, sup["supplier_id"], part_no, airline, hub, 1, moment, promised, actual)
        self.add_shipment(order_type, part_no, 1, sup["supplier_id"], hub, airline, fault_id,
                          po["purchase_order_id"], dispatch, promised, actual)
        self.move(key, actual, +1, f"RECEIVED {po['purchase_order_id']} for {fault_id}")
        return actual

    # ---- get one part to an aircraft as fast as possible ----
    def obtain_part(self, airline, station, part_no, moment, fault_id):
        """Returns (time the part is in the engineer's hands, source description, action)."""
        local = (airline, station, part_no)
        if self.available_at(local, moment) >= 1:
            self.move(local, moment, -1, f"ISSUED {fault_id}")
            self.check_reorder(local, moment)
            return moment, f"STOCK:{station}", "REPAIRED_FROM_STOCK"

        options = []
        hub = self.hub_of[airline]
        if station != hub:                                          # option A: our own hub store
            hub_key = (airline, hub, part_no)
            dispatch = moment + timedelta(hours=AOG_HANDLING_HOURS)
            if self.available_at(hub_key, dispatch) >= 1:
                planned = dispatch + timedelta(hours=transit_hours(hub, station))
                options.append({"planned": planned, "kind": "HUB", "key": hub_key, "dispatch": dispatch})
        sup = self.supplier[self.part[part_no]["supplier_id"]]        # option B: supplier AOG desk
        dispatch = moment + timedelta(hours=self.rng.randint(*AOG_SUPPLIER_PROCESSING_HOURS))
        planned = dispatch + timedelta(hours=transit_hours(sup["ship_from_airport_code"], station))
        options.append({"planned": planned, "kind": "SUPPLIER", "sup": sup, "dispatch": dispatch})

        best = min(options, key=lambda o: o["planned"])            # fastest PLANNED arrival wins
        if best["kind"] == "HUB":
            late = self.rng.random() > INTERNAL_ON_TIME_RATE
            actual = best["planned"] + (timedelta(hours=self.rng.randint(*INTERNAL_LATE_HOURS))
                                        if late else timedelta(0))
            self.move(best["key"], best["dispatch"], -1, f"AOG TRANSFER {fault_id}")
            self.check_reorder(best["key"], best["dispatch"])
            self.add_shipment("AOG_TRANSFER", part_no, 1, hub, station, airline, fault_id, "",
                              best["dispatch"], best["planned"], actual)
            source, action = f"TRANSFER:{hub}", "AOG_TRANSFER"
        else:
            s = best["sup"]
            late = self.rng.random() > float(s["on_time_rate"])
            actual = best["planned"] + (timedelta(hours=self.rng.randint(*SUPPLIER_AOG_LATE_HOURS))
                                        if late else timedelta(0))
            po = self.add_po("AOG", s["supplier_id"], part_no, airline, station, 1,
                             moment, best["planned"], actual)
            self.add_shipment("AOG_SUPPLIER", part_no, 1, s["supplier_id"], station, airline, fault_id,
                              po["purchase_order_id"], best["dispatch"], best["planned"], actual)
            source, action = f"SUPPLIER:{s['supplier_id']}", "AOG_SUPPLIER"

        self.move(local, actual, +1, f"RECEIVED for {fault_id}")       # arrives at the station...
        self.move(local, actual, -1, f"ISSUED {fault_id}")             # ...and goes straight on the aircraft
        return actual, source, action

    def snapshots(self, week_start, hours):
        """Periodic snapshot: stock on hand for every hub store, every hour."""
        rows = []
        keys = sorted(set(self.initial) | {k for k in self.ledger if k[1] == self.hub_of[k[0]]})
        for h in range(hours):
            moment = week_start + timedelta(hours=h)
            for key in keys:
                rows.append({"snapshot_utc": fmt_utc(moment), "airline_code": key[0],
                             "airport_code": key[1], "part_number": key[2],
                             "quantity_on_hand": self.level_at(key, moment)})
        return rows


# =====================================================
# 3c  MAINTENANCE CONTROL - faults, MEL decisions, deferred defects
# =====================================================
class MaintenanceControl:
    def __init__(self, rng, parts, aircraft, supply):
        self.rng, self.supply = rng, supply
        self.parts_by_type = defaultdict(list)
        for p in parts:
            self.parts_by_type[p["icao_type"]].append(p)
        self.aircraft = {a["registration"]: a for a in aircraft}
        self.open_defects = defaultdict(list)          # tail -> deferred defects not yet repaired
        self.events = []

    def fault_probability(self, reg):
        age = SIM_YEAR - int(self.aircraft[reg]["year_built"])
        return BASE_FAULT_PROBABILITY * (1 + age / AGE_FACTOR_YEARS)

    def pick_part(self, icao_type):
        candidates = self.parts_by_type[icao_type]
        per_crit = Counter(p["criticality"] for p in candidates)
        weights = [CRITICALITY_WEIGHTS[p["criticality"]] / per_crit[p["criticality"]] for p in candidates]
        return self.rng.choices(candidates, weights=weights, k=1)[0]

    def repair_done(self, start, criticality):
        return start + timedelta(hours=self.rng.randint(*REPAIR_HOURS[criticality]))

    def after_arrival(self, flight_id, reg, airline, station, arrival, hub):
        """
        Called every time an aircraft lands. Returns the time the aircraft is released after
        technical work that STOPS it flying (or None if nothing grounds it).
        """
        grounded_until = []

        # 1) existing deferred defects: fix at the hub if stock exists, escalate if MEL deadline passed
        for d in list(self.open_defects[reg]):
            key = (airline, station, d["part_number"])
            if station == hub and self.supply.available_at(key, arrival) >= 1:
                self.supply.move(key, arrival, -1, f"ISSUED {d['fault_id']}")
                self.supply.check_reorder(key, arrival)
                d.update(action="DEFERRED_THEN_REPAIRED", part_source=f"STOCK:{station}",
                         part_available_utc=fmt_utc(arrival),
                         repair_completed_utc=fmt_utc(self.repair_done(arrival, d["criticality"])),
                         status="CLOSED")
                self.open_defects[reg].remove(d)          # done during scheduled ground time
            elif d["criticality"] == "GO_IF" and arrival >= d["_deadline"]:
                part_time, source, action = self.supply.obtain_part(
                    airline, station, d["part_number"], arrival, d["fault_id"])
                done = self.repair_done(part_time, "NO_GO")
                d.update(action=f"MEL_EXPIRED_{action}", part_source=source,
                         part_available_utc=fmt_utc(part_time), repair_completed_utc=fmt_utc(done),
                         status="CLOSED", _grounded_from=arrival, _grounded_at=station,
                         _grounded_until=done)
                self.open_defects[reg].remove(d)
                grounded_until.append(done)

        # 2) a new fault found during the post-flight inspection?
        if self.rng.random() < self.fault_probability(reg):
            p = self.pick_part(self.aircraft[reg]["icao_type"])
            fault = {
                "fault_id": f"FLT-{len(self.events) + 1:04d}", "aircraft_registration": reg,
                "airline_code": airline, "detected_after_flight_id": flight_id, "airport_code": station,
                "detected_utc": fmt_utc(arrival), "part_number": p["part_number"],
                "part_description": p["description"], "ata_chapter": p["ata_chapter"],
                "criticality": p["criticality"], "action": "", "mel_deadline_utc": "",
                "part_source": "", "part_available_utc": "", "repair_completed_utc": "", "status": "OPEN",
                "_detected": arrival, "_deadline": None,
                "_grounded_from": None, "_grounded_at": None, "_grounded_until": None,
            }
            self.events.append(fault)
            if p["criticality"] == "NO_GO":                 # cannot fly until fixed
                part_time, source, action = self.supply.obtain_part(
                    airline, station, p["part_number"], arrival, fault["fault_id"])
                done = self.repair_done(part_time, "NO_GO")
                fault.update(action=action, part_source=source, part_available_utc=fmt_utc(part_time),
                             repair_completed_utc=fmt_utc(done), status="CLOSED",
                             _grounded_from=arrival, _grounded_at=station, _grounded_until=done)
                grounded_until.append(done)
            else:                                            # MEL allows flying; fix later
                if p["criticality"] == "GO_IF":
                    fault["_deadline"] = arrival + timedelta(hours=MEL_GO_IF_DEADLINE_HOURS)
                    fault["mel_deadline_utc"] = fmt_utc(fault["_deadline"])
                fault["action"] = "DEFERRED"
                self.open_defects[reg].append(fault)
                # maintenance control orders the part NOW, so it is waiting at the hub
                self.supply.order_for_deferred_defect(airline, p["part_number"], arrival,
                                                      fault["fault_id"], urgent=p["criticality"] == "GO_IF")

        return max(grounded_until) if grounded_until else None

# =====================================================
# 3b.1  PROACTIVE CANCELLATIONS (decided before the day of operation)
# =====================================================
def plan_proactive_cancellations(rng, flights, weather_events, hubs):
    """Severe weather at a hub: cancel a share of short-haul round trips in advance."""
    next_leg_of = {f["previous_flight_id"]: f for f in flights if f["previous_flight_id"]}
    cancelled = {}
    reason = "71: Proactive cancellation - severe weather capacity reduction"
    for e in weather_events:
        if e["severity"] != "SEVERE" or e["airport_code"] not in hubs:
            continue
        for f in sorted(flights, key=lambda f: (f["_dep"], f["flight_id"])):
            is_hub_outbound = f["rotation_seq"] % 2 == 1 and f["origin_airport_code"] == e["airport_code"]
            if (f["_body"] == "NARROW" and is_hub_outbound
                    and e["_start"] <= f["_dep"] < e["_end"]
                    and rng.random() < PROACTIVE_CANCEL_RATE):
                cancelled[f["flight_id"]] = reason
                ret = next_leg_of.get(f["flight_id"])
                if ret:
                    cancelled[ret["flight_id"]] = reason
    return cancelled


# =====================================================
# 3b  ACTUALS - discrete-event simulation in global time order
# =====================================================
def simulate_actuals(rng, flights, weather_events, hubs, aircraft, airlines, maintenance):
    events_by_airport = defaultdict(list)
    for e in weather_events:
        events_by_airport[e["airport_code"]].append(e)

    # A "line" = the planned sequence of flights for one tail. Swaps change which aircraft flies a line.
    lines = defaultdict(list)
    for f in flights:
        lines[f["aircraft_registration"]].append(f)
    for legs in lines.values():
        legs.sort(key=lambda f: f["rotation_seq"])
    position = {f["flight_id"]: i for legs in lines.values() for i, f in enumerate(legs)}
    next_idx = {line: 0 for line in lines}                      # next unprocessed leg per line
    line_aircraft = {line: line for line in lines}               # which tail currently flies each line
    line_airline = {line: legs[0]["airline_code"] for line, legs in lines.items()}
    line_body = {line: legs[0]["_body"] for line, legs in lines.items()}

    # every aircraft starts the week parked and ready at its hub
    ready = {line: legs[0]["_dep"] - timedelta(hours=12) for line, legs in lines.items()}
    last_flight = {}           # tail -> last flight it actually operated

    # standby aircraft wait at their airline's hub: (airline, body) -> set of tails
    hub_of = {a["airline_code"]: a["hub_airport_code"] for a in airlines}
    body_of = {a["registration"]: a["body_type"] for a in aircraft}
    airline_of = {a["registration"]: a["airline_code"] for a in aircraft}
    standby_pool = defaultdict(set)
    week_start = min(f["_dep"] for f in flights)
    for reg in pick_standby(aircraft):
        standby_pool[(airline_of[reg], body_of[reg])].add(reg)
        ready[reg] = week_start - timedelta(hours=12)
    # normal_ready = when the aircraft WOULD be ready without technical problems
    # (the gap between ready and normal_ready is technical delay, code 41)
    normal_ready = dict(ready)

    cancelled_reason = plan_proactive_cancellations(rng, flights, weather_events, hubs)
    actuals, delay_rows, swaps = {}, [], []

    for f in sorted(flights, key=lambda f: (f["_dep"], f["flight_id"])):
        fid, line, body = f["flight_id"], f["aircraft_registration"], f["_body"]
        origin, dest = f["origin_airport_code"], f["destination_airport_code"]
        sched_dep, sched_arr = f["_dep"], f["_arr"]
        next_idx[line] = position[fid] + 1

        if fid in cancelled_reason:
            actuals[fid] = cancelled_record(fid, cancelled_reason[fid])
            continue

        reg = line_aircraft[line]

        # ---- TAIL SWAP at the hub: replace a late aircraft (standby first, then another line) ----
        if f["rotation_seq"] % 2 == 1:
            late_by = minutes_between(ready[reg], sched_dep)
            if late_by >= SWAP_CONSIDER_MINUTES:
                best = None
                # option 1: a standby aircraft waiting at this hub
                pool = standby_pool[(line_airline[line], body)]
                for spare in sorted(pool):
                    saved = late_by - max(0, minutes_between(ready[spare], sched_dep))
                    if saved >= SWAP_MIN_SAVING_MINUTES and (best is None or saved > best[0]):
                        best = (saved, "STANDBY", None, None, spare)
                # option 2: swap with another line whose aircraft is idle here until a later flight
                for other, legs in lines.items():
                    if other == line or line_airline[other] != line_airline[line] or line_body[other] != body:
                        continue
                    i = next_idx[other]
                    if i >= len(legs):
                        continue
                    nxt = legs[i]
                    if (nxt["rotation_seq"] % 2 != 1 or nxt["origin_airport_code"] != origin
                            or nxt["flight_id"] in cancelled_reason):
                        continue
                    other_reg = line_aircraft[other]
                    before = (max(0, minutes_between(ready[reg], sched_dep))
                              + max(0, minutes_between(ready[other_reg], nxt["_dep"])))
                    after = (max(0, minutes_between(ready[other_reg], sched_dep))
                             + max(0, minutes_between(ready[reg], nxt["_dep"])))
                    saved = before - after
                    if saved >= SWAP_MIN_SAVING_MINUTES and (best is None or saved > best[0]):
                        best = (saved, "LINE_SWAP", other, nxt, other_reg)
                if best:
                    saved, swap_type, other, nxt, new_reg = best
                    if swap_type == "STANDBY":
                        pool.discard(new_reg)
                        pool.add(reg)                      # the late aircraft becomes the spare
                        line_aircraft[line] = new_reg
                    else:
                        line_aircraft[line], line_aircraft[other] = new_reg, reg
                    swaps.append({
                        "swap_id": f"SW-{len(swaps) + 1:03d}", "swap_type": swap_type,
                        "airport_code": origin, "swap_time_utc": fmt_utc(sched_dep), "flight_id": fid,
                        "late_aircraft": reg, "replacement_aircraft": new_reg,
                        "other_flight_id": nxt["flight_id"] if nxt else "", "minutes_saved": saved,
                    })
                    reg = new_reg

        # ---- primary delays that are random per flight ----
        primary = []
        atc_chance = ATC_PROBABILITY["EUROPE" if origin in EUROPEAN_AIRPORTS else "OTHER"]
        if rng.random() < atc_chance:
            primary.append(("81", rng.randint(*ATC_MINUTES)))
        if rng.random() < RAMP_PROBABILITY:
            primary.append(("32", rng.randint(*RAMP_MINUTES)))
        primary_total = sum(m for _, m in primary)

        # ---- when could it go, ignoring weather? ----
        base = sched_dep + timedelta(minutes=primary_total)
        could_go = max(base, ready[reg])
        aircraft_delay = minutes_between(could_go, base)                        # caused by the aircraft
        reactionary = max(0, minutes_between(min(normal_ready[reg], could_go), base))  # late inbound
        technical = aircraft_delay - reactionary                                 # grounded for repairs

        # ---- weather: reduced runway capacity -> queue for departure ----
        base_wait = weather_queue_wait(events_by_airport, origin, could_go, hubs)
        weather_wait = round(base_wait * rng.uniform(0.8, 1.2)) if base_wait > 0 else 0
        actual_dep = could_go + timedelta(minutes=weather_wait)

        dep_delay = minutes_between(actual_dep, sched_dep)
        causes = (primary
                  + ([("71", weather_wait)] if weather_wait > 0 else [])
                  + ([("41", technical)] if technical > 0 else [])
                  + ([("93", reactionary)] if reactionary > 0 else []))

        # ---- reactive recovery: cancel a badly delayed short-haul round trip ----
        if body == "NARROW" and f["rotation_seq"] % 2 == 1 and dep_delay > CANCEL_AFTER_MINUTES:
            main_code = max(causes, key=lambda c: c[1])[0]
            reason = f"{main_code}: Reactive cancellation - {DELAY_CODES[main_code]}"
            actuals[fid] = cancelled_record(fid, reason)
            nxt_in_line = lines[line][position[fid] + 1] if position[fid] + 1 < len(lines[line]) else None
            if nxt_in_line:
                cancelled_reason[nxt_in_line["flight_id"]] = reason
            continue   # aircraft stays at the hub; its ready time does not change

        # ---- arrival: flight time varies; holding if destination weather is bad ----
        lo, hi = AIRBORNE_VARIATION[body]
        block = f["block_time_minutes"] + rng.randint(lo, hi)
        if weather_at(events_by_airport, dest, actual_dep + timedelta(minutes=block)):
            block += rng.randint(*HOLDING_MINUTES)
        actual_arr = actual_dep + timedelta(minutes=block)

        actuals[fid] = {
            "flight_id": fid,
            "flight_status": "DELAYED" if dep_delay >= ON_TIME_THRESHOLD_MINUTES else "ON_TIME",
            "actual_aircraft_registration": reg,
            "actual_previous_flight_id": last_flight.get(reg, ""),
            "actual_departure_utc": fmt_utc(actual_dep),
            "actual_arrival_utc": fmt_utc(actual_arr),
            "departure_delay_minutes": dep_delay,
            "arrival_delay_minutes": minutes_between(actual_arr, sched_arr),   # negative = early
            "cancellation_reason": "",
            "_dep": actual_dep, "_arr": actual_arr,
            "_primary": primary_total + weather_wait + technical, "_reactionary": reactionary,
        }
        for code, minutes in causes:
            delay_rows.append({
                "flight_id": fid, "delay_code": code,
                "delay_description": DELAY_CODES[code], "delay_minutes": minutes,
            })

        # ---- post-flight inspection: may find a fault that grounds the aircraft ----
        normal_ready[reg] = actual_arr + timedelta(minutes=MIN_TURNAROUND_MINUTES[body])
        grounded_until = maintenance.after_arrival(fid, reg, line_airline[line], dest, actual_arr,
                                                   hub_of[line_airline[line]])
        ready[reg] = max(normal_ready[reg], grounded_until) if grounded_until else normal_ready[reg]
        last_flight[reg] = fid

    return actuals, delay_rows, swaps


def cancelled_record(fid, reason):
    return {
        "flight_id": fid, "flight_status": "CANCELLED",
        "actual_aircraft_registration": "", "actual_previous_flight_id": "",
        "actual_departure_utc": "", "actual_arrival_utc": "",
        "departure_delay_minutes": "", "arrival_delay_minutes": "",
        "cancellation_reason": reason,
        "_dep": None, "_arr": None, "_primary": 0, "_reactionary": 0,
    }


def validate_actuals(flights, actuals, delay_rows):
    problems = []
    if set(actuals) != {f["flight_id"] for f in flights}:
        problems.append("Not every scheduled flight has exactly one actual record")

    # delay codes must add up to the departure delay (reconciliation)
    code_sum = defaultdict(int)
    for d in delay_rows:
        code_sum[d["flight_id"]] += d["delay_minutes"]
    for fid, a in actuals.items():
        if a["flight_status"] != "CANCELLED" and code_sum[fid] != a["departure_delay_minutes"]:
            problems.append(f"{fid}: delay codes sum to {code_sum[fid]} "
                            f"but departure delay is {a['departure_delay_minutes']}")

    # cancelled round trips: outbound and return must both be cancelled
    flight_by_id = {f["flight_id"]: f for f in flights}
    for f in flights:
        if f["rotation_seq"] % 2 == 1 and actuals[f["flight_id"]]["flight_status"] == "CANCELLED":
            ret = next((g for g in flights if g["previous_flight_id"] == f["flight_id"]), None)
            if ret and actuals[ret["flight_id"]]["flight_status"] != "CANCELLED":
                problems.append(f"{f['flight_id']} cancelled but its return {ret['flight_id']} operated")

    # the ACTUAL aircraft's journey must be physically possible (this is what swaps could break)
    by_tail = defaultdict(list)
    for fid, a in actuals.items():
        if a["flight_status"] != "CANCELLED":
            by_tail[a["actual_aircraft_registration"]].append(flight_by_id[fid])
    for reg, legs in by_tail.items():
        legs.sort(key=lambda f: actuals[f["flight_id"]]["_dep"])
        for prev, cur in zip(legs, legs[1:]):
            a_prev, a_cur = actuals[prev["flight_id"]], actuals[cur["flight_id"]]
            if cur["origin_airport_code"] != prev["destination_airport_code"]:
                problems.append(f"{reg}: TELEPORT before {cur['flight_id']}")
            if a_cur["_dep"] < a_prev["_arr"] + timedelta(minutes=MIN_TURNAROUND_MINUTES[cur["_body"]]):
                problems.append(f"{reg}: impossible turnaround before {cur['flight_id']}")
            if a_cur["actual_previous_flight_id"] != prev["flight_id"]:
                problems.append(f"{reg}: wrong actual_previous_flight_id at {cur['flight_id']}")

    if problems:
        raise ValueError("Actuals validation failed:\n  " + "\n  ".join(problems[:20]))
    print("ACTUALS VALIDATION PASSED (codes reconcile, round trips consistent, "
          "actual aircraft journeys physically possible)")


def validate_maintenance(flights, actuals, maintenance, supply):
    problems = []

    # 1) stock can never go negative at any moment, in any store
    for key, movements in supply.ledger.items():
        running = 0
        for t, d, reason in sorted(movements, key=lambda m: (m[0], -m[1])):
            running += d
            if running < 0:
                problems.append(f"NEGATIVE STOCK {key} at {fmt_utc(t)} ({reason})")
                break

    # 2) a grounded aircraft must not fly again before its repair is complete
    flight_by_id = {f["flight_id"]: f for f in flights}
    flown_by = defaultdict(list)
    for fid, a in actuals.items():
        if a["flight_status"] != "CANCELLED":
            flown_by[a["actual_aircraft_registration"]].append(a)
    for e in maintenance.events:
        if e["_grounded_until"] is None:
            continue
        later = [a for a in flown_by[e["aircraft_registration"]] if a["_dep"] > e["_grounded_from"]]
        if later:
            nxt = min(later, key=lambda a: a["_dep"])
            if nxt["_dep"] < e["_grounded_until"]:
                problems.append(f"{e['fault_id']}: {e['aircraft_registration']} flew "
                                f"{nxt['flight_id']} before repair was complete")
            if flight_by_id[nxt["flight_id"]]["origin_airport_code"] != e["_grounded_at"]:
                problems.append(f"{e['fault_id']}: aircraft left from a different airport than the repair")

    # 3) shipments cannot arrive before they leave, or earlier than planned
    for s in supply.shipments:
        if not (s["_dispatched"] <= s["_planned"] <= s["_actual"]):
            problems.append(f"{s['shipment_id']}: impossible shipment times")

    if problems:
        raise ValueError("Maintenance/supply validation failed:\n  " + "\n  ".join(problems[:20]))
    print("MAINTENANCE VALIDATION PASSED (no negative stock, no flying before repair, shipment times valid)")


# =====================================================
# SUMMARY
# =====================================================
def summarise(flights, actuals, delay_rows, weather_events, swaps):
    statuses = Counter(a["flight_status"] for a in actuals.values())
    operated = len(actuals) - statuses["CANCELLED"]
    otp = 100 * statuses["ON_TIME"] / operated if operated else 0

    print(f"\nFLIGHTS: {len(flights)}   operated: {operated}   cancelled: {statuses['CANCELLED']}")
    print(f"ON-TIME PERFORMANCE (departure < {ON_TIME_THRESHOLD_MINUTES} min late): {otp:.1f}%")

    reasons = Counter(a["cancellation_reason"] for a in actuals.values() if a["flight_status"] == "CANCELLED")
    for reason, n in reasons.most_common():
        print(f"  cancelled {n:>3} x  {reason}")

    print("\nWEATHER EVENTS:")
    for e in weather_events:
        print(f"  {e['weather_event_id']}  {e['airport_code']}  {e['severity']:<8} "
              f"{e['start_utc']} -> {e['end_utc']}")

    minutes_by_code = Counter()
    for d in delay_rows:
        minutes_by_code[d["delay_code"]] += d["delay_minutes"]
    total = sum(minutes_by_code.values()) or 1
    print("\nDELAY MINUTES BY CAUSE:")
    for code, mins in minutes_by_code.most_common():
        print(f"  {code}  {DELAY_CODES[code]:<48} {mins:>6} min  ({100 * mins / total:.0f}%)")

    print(f"\nTAIL SWAPS: {len(swaps)}   total delay minutes saved: {sum(s['minutes_saved'] for s in swaps)}")
    for s in swaps[:5]:
        print(f"  {s['swap_id']} {s['swap_type']:<9} {s['airport_code']} {s['flight_id']:<16} late {s['late_aircraft']} "
              f"-> {s['replacement_aircraft']}  (saved {s['minutes_saved']} min)")

    # biggest cascade along the ACTUAL aircraft journeys
    flight_by_id = {f["flight_id"]: f for f in flights}
    by_tail = defaultdict(list)
    for fid, a in actuals.items():
        if a["flight_status"] != "CANCELLED":
            by_tail[a["actual_aircraft_registration"]].append(flight_by_id[fid])
    best = (0, [])
    for legs in by_tail.values():
        legs.sort(key=lambda f: actuals[f["flight_id"]]["_dep"])
        for i, leg in enumerate(legs):
            if actuals[leg["flight_id"]]["_primary"] > 0:
                chain = [leg]
                for nxt in legs[i + 1:]:
                    if actuals[nxt["flight_id"]]["_reactionary"] > 0:
                        chain.append(nxt)
                    else:
                        break
                if len(chain) > best[0]:
                    best = (len(chain), chain)

    if best[0] > 1:
        print(f"\nBIGGEST CASCADE: one primary delay spread across {best[0] - 1} later flight(s)")
        for leg in best[1]:
            a = actuals[leg["flight_id"]]
            print(f"  {leg['flight_id']:<16} {leg['origin_airport_code']}->{leg['destination_airport_code']}"
                  f"  dep delay {a['departure_delay_minutes']:>4} min"
                  f"  (primary {a['_primary']:>3}, reactionary {a['_reactionary']:>3})")


def summarise_maintenance(flights, actuals, maintenance, supply, snapshot_rows):
    events = maintenance.events
    print(f"\nFAULTS FOUND: {len(events)}")
    for crit, n in sorted(Counter(e["criticality"] for e in events).items()):
        print(f"  {crit:<6} {n}")
    print("FAULT OUTCOMES:")
    for action, n in Counter(e["action"] for e in events).most_common():
        print(f"  {action:<32} {n}")

    groundings = [e for e in events if e["_grounded_until"] is not None]
    total_hours = sum((e["_grounded_until"] - e["_grounded_from"]).total_seconds() / 3600 for e in groundings)
    print(f"AIRCRAFT GROUNDED (AOG or repair before flight): {len(groundings)}   "
          f"total {total_hours:.1f} hours on the ground")
    print(f"PURCHASE ORDERS: {Counter(p['order_type'] for p in supply.purchase_orders)}")
    print(f"SHIPMENTS:       {Counter(s['shipment_type'] for s in supply.shipments)}")
    print(f"INVENTORY SNAPSHOT ROWS: {len(snapshot_rows)}")

    # the most damaging grounding, traced through the network (the project-guide scenario)
    if groundings:
        worst = max(groundings, key=lambda e: e["_grounded_until"] - e["_grounded_from"])
        reg = worst["aircraft_registration"]
        hours = (worst["_grounded_until"] - worst["_grounded_from"]).total_seconds() / 3600
        print(f"\nWORST GROUNDING - {worst['fault_id']}")
        print(f"  {reg}: fault found at {worst['airport_code']} after {worst['detected_after_flight_id']}"
              f" ({worst['detected_utc']})")
        print(f"  Grounded at {worst['_grounded_at']} from {fmt_utc(worst['_grounded_from'])}"
              f"  [{worst['action']}]")
        print(f"  Fault: {worst['part_description']} ({worst['part_number']}, ATA {worst['ata_chapter']}, "
              f"{worst['criticality']})")
        print(f"  Part source: {worst['part_source']} -> available {worst['part_available_utc']}")
        print(f"  Repair complete: {worst['repair_completed_utc']}  ({hours:.1f} h on the ground)")
        flight_by_id = {f["flight_id"]: f for f in flights}
        after = sorted((a for a in actuals.values()
                        if a["flight_status"] != "CANCELLED" and a["actual_aircraft_registration"] == reg
                        and a["_dep"] > worst["_grounded_from"]), key=lambda a: a["_dep"])[:3]
        for a in after:
            f = flight_by_id[a["flight_id"]]
            print(f"  -> next: {a['flight_id']:<16} {f['origin_airport_code']}->{f['destination_airport_code']}"
                  f"  departed {a['departure_delay_minutes']} min late")


# =====================================================
# OUTPUT
# =====================================================
def write_and_upload(session, filename, columns, rows):
    path = OUT_LOCAL / filename
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    session.file.put(str(path), OUT_STAGE, auto_compress=False, overwrite=True)
    print(f"UP  {filename:<22} {len(rows):>5} rows -> {OUT_STAGE}")


def main():
    session = get_active_session()
    rng = random.Random(SEED)          # isolated, reproducible randomness
    REF_LOCAL.mkdir(parents=True, exist_ok=True)
    OUT_LOCAL.mkdir(parents=True, exist_ok=True)

    airports = load_reference(session, "airports.csv")
    airlines = load_reference(session, "airlines.csv")
    aircraft = load_reference(session, "aircraft.csv")
    routes = load_reference(session, "routes.csv")
    parts = load_reference(session, "parts.csv")
    suppliers = load_reference(session, "suppliers.csv")
    hubs = {a["hub_airport_code"] for a in airlines}
    COURIER_MINUTES.update(build_courier_times(routes))

    # 3a - the plan
    flights = build_schedule(airports, airlines, aircraft, routes)
    validate_schedule(flights)
    print(f"STANDBY AIRCRAFT (no schedule, kept at hub): {', '.join(sorted(pick_standby(aircraft)))}")

    # 3b + 3c - what actually happened (operations, maintenance and supply chain together)
    week_start = datetime.combine(START_DATE, time(0, 0), tzinfo=UTC)
    weather_events = generate_weather_events(rng, airports, airlines)
    supply = SupplyChain(rng, parts, suppliers, airlines, aircraft, week_start)
    maintenance = MaintenanceControl(rng, parts, aircraft, supply)
    actuals, delay_rows, swaps = simulate_actuals(rng, flights, weather_events, hubs,
                                                  aircraft, airlines, maintenance)
    validate_actuals(flights, actuals, delay_rows)
    validate_maintenance(flights, actuals, maintenance, supply)
    snapshot_rows = supply.snapshots(week_start, NUM_DAYS * 24)

    summarise(flights, actuals, delay_rows, weather_events, swaps)
    summarise_maintenance(flights, actuals, maintenance, supply, snapshot_rows)

    print()
    by_dep = sorted(flights, key=lambda f: (f["_dep"], f["flight_id"]))
    write_and_upload(session, "flights_schedule.csv", SCHEDULE_COLUMNS, by_dep)
    write_and_upload(session, "flights_actual.csv", ACTUAL_COLUMNS,
                     [actuals[f["flight_id"]] for f in by_dep])
    write_and_upload(session, "flight_delays.csv", DELAY_COLUMNS, delay_rows)
    write_and_upload(session, "weather_events.csv", WEATHER_COLUMNS, weather_events)
    write_and_upload(session, "tail_swaps.csv", SWAP_COLUMNS, swaps)
    write_and_upload(session, "maintenance_events.csv", MAINTENANCE_COLUMNS, maintenance.events)
    write_and_upload(session, "shipments.csv", SHIPMENT_COLUMNS, supply.shipments)
    write_and_upload(session, "purchase_orders.csv", PO_COLUMNS, supply.purchase_orders)
    write_and_upload(session, "inventory_snapshots.csv", SNAPSHOT_COLUMNS, snapshot_rows)


main()