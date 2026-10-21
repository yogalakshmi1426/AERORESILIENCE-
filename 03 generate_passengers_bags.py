"""
AERORESILIENCE - Synthetic data generator (Snowflake)
03 PASSENGERS & BAGS  (bookings, connections, misconnections, bag journeys)

Reads  : @AERORESILIENCE.BRONZE.RAW_STAGE/reference/  (airports, airlines, aircraft, min_connection_times)
         @AERORESILIENCE.BRONZE.RAW_STAGE/truth/      (flights_schedule, flights_actual)
Writes : @AERORESILIENCE.BRONZE.RAW_STAGE/truth/
           passengers.csv          grain: one passenger journey (one booking)          [contains synthetic PII]
           passenger_segments.csv  grain: one passenger on one flight segment
           bags.csv                grain: one checked bag
           bag_scans.jsonl.gz      grain: one scan event of one bag  (JSON Lines, nested, gzip)

Bookings are made against the SCHEDULE; travel follows the ACTUALS (delays, cancellations,
tail swaps). Passengers and bags depend on flights but never change them (one-way dependency),
so this is a separate step from the operations simulator.
"""

import csv
import gzip
import json
import random
import string
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from snowflake.snowpark.context import get_active_session

# =====================================================
# SETTINGS
# =====================================================
SEED = 42
LOAD_FACTOR_RANGE = (0.75, 0.95)        # share of seats sold per flight
CONNECTING_SHARE = {"EK": 0.60, "QR": 0.60, "KL": 0.35, "BA": 0.30}   # of inbound-to-hub passengers
MAX_CONNECTION_MINUTES = 360            # longest connection we sell
CABIN_MIX = {"NARROW": {"ECONOMY": 0.90, "BUSINESS": 0.10},
             "WIDE":   {"ECONOMY": 0.80, "PREMIUM_ECONOMY": 0.08, "BUSINESS": 0.12}}
FF_TIER_MIX = {"NONE": 0.70, "BLUE": 0.15, "SILVER": 0.09, "GOLD": 0.06}
BAGS_PER_PASSENGER = {"NARROW": [(0, 0.55), (1, 0.40), (2, 0.05)],
                      "WIDE":   [(0, 0.15), (1, 0.60), (2, 0.25)]}
BAG_WEIGHT_KG = (7.0, 32.0)

BOARDING_CLOSES_MIN = 15                # passenger must be at the gate this long before departure
BAG_LOAD_CUTOFF_MIN = 10                # bag must reach the aircraft this long before departure
PAX_TRANSFER_FACTOR = (0.35, 0.80)      # passenger transfer time as a share of the airport's MCT
BAG_TRANSFER_FACTOR = (0.45, 1.15)      # bag transfer time as a share of MCT (bags are slower)
CHECK_IN_BEFORE_MIN = {"NARROW": (60, 150), "WIDE": (90, 210)}
RUSH_DELIVERY_HOURS = (3, 12)           # courier time to deliver a rush bag to the passenger

AIRLINE_NUMERIC_CODE = {"BA": "125", "EK": "176", "KL": "074", "QR": "157"}   # bag-tag prefixes

FIRST_NAMES = ["Aisha", "Ben", "Carlos", "Daniela", "Emeka", "Fatima", "George", "Hana", "Ivan",
               "Jia", "Kofi", "Lena", "Mohammed", "Nadia", "Oliver", "Priya", "Quentin", "Rosa",
               "Samir", "Tomoko", "Usha", "Viktor", "Wei", "Ximena", "Yusuf", "Zara", "Arjun",
               "Chloe", "Diego", "Elif"]
LAST_NAMES = ["Adeyemi", "Brown", "Chen", "Dubois", "Evans", "Fernandez", "Gupta", "Hassan",
              "Ivanova", "Jensen", "Khan", "Lopez", "Muller", "Nakamura", "Okafor", "Patel",
              "Quinn", "Rossi", "Singh", "Tanaka", "Usman", "Van Dijk", "Williams", "Xu",
              "Yilmaz", "Zhang", "Kowalski", "Mensah", "Silva", "Ahmed"]

REF_STAGE = "@AERORESILIENCE.BRONZE.RAW_STAGE/reference/"
TRUTH_STAGE = "@AERORESILIENCE.BRONZE.RAW_STAGE/truth/"
IN_LOCAL = Path("/tmp/aeroresilience/p4_in")
OUT_LOCAL = Path("/tmp/aeroresilience/p4_out")

PASSENGER_COLUMNS = [
    "booking_ref", "passenger_id", "passenger_name", "email", "frequent_flyer_tier", "cabin",
    "airline_code", "origin_airport_code", "final_destination_code", "is_connecting",
    "planned_arrival_utc", "actual_arrival_utc", "arrival_delay_minutes", "journey_status",
]
SEGMENT_COLUMNS = ["booking_ref", "segment_no", "planned_flight_id", "actual_flight_id", "segment_status"]
BAG_COLUMNS = [
    "bag_tag", "booking_ref", "airline_code", "origin_airport_code", "final_destination_code",
    "weight_kg", "bag_status", "delivered_utc",
]


# =====================================================
# HELPERS
# =====================================================
def load_csv(session, stage, filename):
    session.file.get(stage + filename, str(IN_LOCAL))
    with open(IN_LOCAL / filename, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    print(f"IN  {filename:<26} {len(rows):>6} rows")
    return rows


def parse_utc(s):
    return datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ") if s else None


def fmt_utc(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ") if dt else ""


def minutes(n):
    return timedelta(minutes=n)


def weighted(rng, mix):
    keys = list(mix)
    return rng.choices(keys, weights=[mix[k] for k in keys], k=1)[0]


# =====================================================
# THE FLIGHT WORLD (read-only)
# =====================================================
class FlightWorld:
    def __init__(self, schedule, actuals, airports, airlines, aircraft, mct_rows):
        actual_by_id = {a["flight_id"]: a for a in actuals}
        seats = {a["registration"]: int(a["seats"]) for a in aircraft}
        body = {a["registration"]: a["body_type"] for a in aircraft}
        self.country = {a["airport_code"]: a["country_code"] for a in airports}
        self.hub_of = {a["airline_code"]: a["hub_airport_code"] for a in airlines}
        self.mct = {(m["airport_code"], m["connection_type"]): int(m["min_connection_minutes"])
                    for m in mct_rows}

        self.flights = {}
        for s in schedule:
            a = actual_by_id[s["flight_id"]]
            planned_reg, actual_reg = s["aircraft_registration"], a["actual_aircraft_registration"]
            # seats we can sell = the smaller of the planned and the actual aircraft (safe after swaps)
            cap = min(seats[planned_reg], seats[actual_reg]) if actual_reg else seats[planned_reg]
            self.flights[s["flight_id"]] = {
                "id": s["flight_id"], "number": s["flight_number"], "airline": s["airline_code"],
                "route": s["route_id"], "origin": s["origin_airport_code"],
                "dest": s["destination_airport_code"], "body": body[planned_reg], "seats": cap,
                "sched_dep": parse_utc(s["scheduled_departure_utc"]),
                "sched_arr": parse_utc(s["scheduled_arrival_utc"]),
                "cancelled": a["flight_status"] == "CANCELLED",
                "dep": parse_utc(a["actual_departure_utc"]), "arr": parse_utc(a["actual_arrival_utc"]),
            }
        self.by_route = defaultdict(list)
        for f in sorted(self.flights.values(), key=lambda f: (f["sched_dep"], f["id"])):
            self.by_route[f["route"]].append(f)
        self.reserved = Counter()        # flight_id -> seats SOLD in advance (always honoured)
        self.extra = Counter()           # flight_id -> rebooked passengers squeezed into UNSOLD seats

    def connection_type(self, f_in, f_out):
        dom_in = self.country[f_in["origin"]] == self.country[f_in["dest"]]
        dom_out = self.country[f_out["origin"]] == self.country[f_out["dest"]]
        return f"{'DOM' if dom_in else 'INTL'}_{'DOM' if dom_out else 'INTL'}"

    def mct_minutes(self, airport, conn_type):
        return self.mct.get((airport, conn_type), self.mct.get((airport, "INTL_INTL"), 60))

    def next_operated(self, route, ready_by, cutoff_min, need_seat):
        """Earliest operated flight on a route that can still be caught at `ready_by`."""
        for f in self.by_route[route]:
            if f["cancelled"] or f["dep"] - minutes(cutoff_min) < ready_by:
                continue
            if need_seat and self.reserved[f["id"]] + self.extra[f["id"]] >= f["seats"]:
                continue                 # only unsold seats can be given to rebooked passengers
            return f
        return None


# =====================================================
# BOOKINGS (made against the schedule)
# =====================================================
def make_booking_ref(rng, used):
    while True:
        ref = "".join(rng.choices(string.ascii_uppercase.replace("O", "").replace("I", "") + "23456789", k=6))
        if ref not in used:
            used.add(ref)
            return ref


def create_bookings(rng, world):
    target = {}
    for f in sorted(world.flights.values(), key=lambda f: (f["sched_dep"], f["id"])):
        target[f["id"]] = round(f["seats"] * rng.uniform(*LOAD_FACTOR_RANGE))
    sold = Counter()
    itineraries = []            # list of lists of planned flights

    flights_sorted = sorted(world.flights.values(), key=lambda f: (f["sched_dep"], f["id"]))
    outbound_from_hub = defaultdict(list)
    for f in flights_sorted:
        if f["origin"] == world.hub_of[f["airline"]]:
            outbound_from_hub[f["airline"]].append(f)

    # 1) inbound-to-hub flights: some passengers connect onwards, the rest end at the hub
    for f1 in flights_sorted:
        hub = world.hub_of[f1["airline"]]
        if f1["dest"] != hub:
            continue
        connecting = round(target[f1["id"]] * CONNECTING_SHARE[f1["airline"]])
        for _ in range(connecting):
            options = []
            for f2 in outbound_from_hub[f1["airline"]]:
                if f2["dest"] == f1["origin"] or sold[f2["id"]] >= target[f2["id"]]:
                    continue
                gap = (f2["sched_dep"] - f1["sched_arr"]).total_seconds() / 60
                mct = world.mct_minutes(hub, world.connection_type(f1, f2))
                if mct <= gap <= MAX_CONNECTION_MINUTES:
                    options.append(f2)
            if not options:
                break
            f2 = rng.choice(options)
            itineraries.append([f1, f2])
            sold[f1["id"]] += 1
            sold[f2["id"]] += 1
        # local passengers finishing at the hub
        while sold[f1["id"]] < target[f1["id"]]:
            itineraries.append([f1])
            sold[f1["id"]] += 1

    # 2) every other flight: fill remaining seats with local (point-to-point) passengers
    for f in flights_sorted:
        while sold[f["id"]] < target[f["id"]]:
            itineraries.append([f])
            sold[f["id"]] += 1

    return itineraries


# =====================================================
# TRAVEL (follows the actuals)
# =====================================================
def travel(rng, world, plan):
    """
    Move one passenger (and, separately, their bags) along their planned itinerary,
    reacting to cancellations and late inbound flights. Returns the flights actually taken.
    """
    taken, ready, statuses = [], None, []
    for i, planned in enumerate(plan):
        f = planned
        catchable = (not f["cancelled"]) and (ready is None or ready <= f["dep"] - minutes(BOARDING_CLOSES_MIN))
        if catchable:                                           # their sold seat is guaranteed
            status = "FLOWN_AS_PLANNED"
        else:
            after = ready if ready is not None else f["sched_dep"] - minutes(BOARDING_CLOSES_MIN)
            f = world.next_operated(planned["route"], after, BOARDING_CLOSES_MIN, need_seat=True)
            status = "REBOOKED_CANCELLATION" if planned["cancelled"] else "REBOOKED_MISCONNECTION"
            if f is None:     # no catchable flight left inside the simulated period
                statuses.append("NOT_FLOWN_BY_PERIOD_END")
                return taken, statuses, False
            world.extra[f["id"]] += 1
        taken.append(f)
        statuses.append(status)
        if i + 1 < len(plan):                                   # time to walk to the next gate
            nxt = plan[i + 1]
            mct = world.mct_minutes(f["dest"], world.connection_type(f, nxt))
            ready = f["arr"] + minutes(round(mct * rng.uniform(*PAX_TRANSFER_FACTOR)))
    return taken, statuses, True


# =====================================================
# BAGS
# =====================================================
class BagFactory:
    def __init__(self, rng):
        self.rng = rng
        self.serial = Counter()
        self.scan_no = 0
        self.scans = []

    def new_tag(self, airline):
        self.serial[airline] += 1
        return f"0{AIRLINE_NUMERIC_CODE[airline]}{self.serial[airline]:06d}"

    def scan(self, bag, event, moment, airport, area, flight=None):
        self.scan_no += 1
        self.scans.append({
            "scan_id": f"SC-{self.scan_no:08d}",
            "event_type": event,
            "event_time_utc": fmt_utc(moment),
            "bag": {"tag": bag["bag_tag"], "weight_kg": bag["weight_kg"], "booking_ref": bag["booking_ref"]},
            "flight": {"flight_id": flight["id"], "flight_number": flight["number"]} if flight else None,
            "location": {"airport": airport, "area": area},
            "source_system": f"BRS-{airport}",
            "_t": moment,
        })

    def origin_scans(self, bag, first):
        rng = self.rng
        lo, hi = CHECK_IN_BEFORE_MIN[first["body"]]
        t_checkin = first["sched_dep"] - minutes(rng.randint(lo, hi))
        t_checkin = min(t_checkin, first["dep"] - minutes(60))       # rebooked passengers check in for the new flight
        t_screen = t_checkin + minutes(rng.randint(5, 20))
        t_sort = t_screen + minutes(rng.randint(10, 30))
        t_load = min(max(first["dep"] - minutes(rng.randint(10, 35)), t_sort + minutes(1)),
                     first["dep"] - minutes(2))                       # always before the doors close
        o = first["origin"]
        self.scan(bag, "CHECK_IN", t_checkin, o, "CHECK_IN_DESK", first)
        self.scan(bag, "SCREENED", t_screen, o, "HOLD_BAGGAGE_SCREENING", first)
        self.scan(bag, "SORTED", t_sort, o, "MAKE_UP", first)
        self.scan(bag, "LOADED", t_load, o, "AIRCRAFT_HOLD", first)

    def stranded(self, world, bag, taken):
        """Passenger reached the hub but has no onward seat inside the period: bag is held there."""
        rng = self.rng
        self.origin_scans(bag, taken[0])
        f = taken[-1]
        t_off = f["arr"] + minutes(rng.randint(10, 25))
        self.scan(bag, "OFFLOADED", t_off, f["dest"], "RAMP", f)
        t_sort = t_off + minutes(rng.randint(15, 40))
        self.scan(bag, "TRANSFER_SORTED", t_sort, f["dest"], "TRANSFER_SYSTEM")
        self.scan(bag, "HELD_AWAITING_REBOOKING", t_sort + minutes(rng.randint(5, 20)), f["dest"], "EARLY_BAG_STORE")
        return "IN_TRANSIT_AT_PERIOD_END", None

    def journey(self, world, bag, pax_flights, plan):
        """Generate the scan events for one bag following the passenger's actual flights."""
        rng = self.rng
        first = pax_flights[0]
        self.origin_scans(bag, first)

        current, mishandled = first, False
        for nxt in pax_flights[1:]:
            hub = current["dest"]
            t_off = current["arr"] + minutes(rng.randint(10, 25))
            self.scan(bag, "OFFLOADED", t_off, hub, "RAMP", current)
            mct = world.mct_minutes(hub, world.connection_type(current, nxt))
            bag_ready = current["arr"] + minutes(round(mct * rng.uniform(*BAG_TRANSFER_FACTOR)))
            bag_ready = max(bag_ready, t_off + minutes(5))
            self.scan(bag, "TRANSFER_SORTED", bag_ready, hub, "TRANSFER_SYSTEM", nxt)
            if bag_ready <= nxt["dep"] - minutes(BAG_LOAD_CUTOFF_MIN):
                t_load = max(nxt["dep"] - minutes(rng.randint(10, 25)), bag_ready + minutes(1))
                self.scan(bag, "LOADED", t_load, hub, "AIRCRAFT_HOLD", nxt)
                current = nxt
            else:
                # passenger made it, bag did not: rush bag on the next flight of that route
                mishandled = True
                # flagged when the transfer system sorts the bag and finds its flight closed
                t_missed = bag_ready + minutes(1)
                self.scan(bag, "MISSED_CONNECTION", t_missed, hub, "TRANSFER_SYSTEM", nxt)
                t_tag = t_missed + minutes(rng.randint(5, 20))
                rush = world.next_operated(nxt["route"], t_tag + minutes(1), BAG_LOAD_CUTOFF_MIN, need_seat=False)
                if rush is None:      # rescue flight falls after the simulated period: outcome unknown yet
                    return "MISHANDLED_IN_TRANSIT_AT_PERIOD_END", None
                self.scan(bag, "RUSH_TAGGED", t_tag, hub, "RUSH_AREA", rush)
                t_load = min(max(rush["dep"] - minutes(rng.randint(10, 25)), t_tag + minutes(1)),
                             rush["dep"] - minutes(2))
                self.scan(bag, "LOADED", t_load, hub, "AIRCRAFT_HOLD", rush)
                current = rush

        dest = current["dest"]
        t_off = current["arr"] + minutes(rng.randint(10, 25))
        self.scan(bag, "OFFLOADED", t_off, dest, "RAMP", current)
        if mishandled:
            t_deliver = t_off + timedelta(hours=rng.randint(*RUSH_DELIVERY_HOURS))
            self.scan(bag, "DELIVERED_TO_PASSENGER", t_deliver, dest, "COURIER")
            return "MISHANDLED_DELIVERED_LATE", t_deliver
        t_belt = t_off + minutes(rng.randint(10, 30))
        self.scan(bag, "ARRIVED_RECLAIM", t_belt, dest, "RECLAIM_BELT", current)
        return "DELIVERED_WITH_PASSENGER", t_belt

    def returned(self, bag, plan):
        """Passenger could not travel at all: bag is checked in, then handed back."""
        f = plan[0]
        t = f["sched_dep"] - minutes(120)
        self.scan(bag, "CHECK_IN", t, f["origin"], "CHECK_IN_DESK", f)
        self.scan(bag, "RETURNED_TO_PASSENGER", t + minutes(180), f["origin"], "CHECK_IN_DESK", f)
        return "RETURNED_TO_PASSENGER", t + minutes(180)


# =====================================================
# BUILD EVERYTHING
# =====================================================
def build(rng, world):
    itineraries = create_bookings(rng, world)
    for plan in itineraries:
        for f in plan:
            world.reserved[f["id"]] += 1
    # process passengers in order of their first departure (earlier travellers grab rebooking seats first)
    itineraries.sort(key=lambda p: (p[0]["sched_dep"], p[0]["id"], len(p)))

    passengers, segments, bags = [], [], []
    factory = BagFactory(rng)
    used_refs = set()

    for n, plan in enumerate(itineraries, start=1):
        airline = plan[0]["airline"]
        body = "WIDE" if any(f["body"] == "WIDE" for f in plan) else "NARROW"
        ref = make_booking_ref(rng, used_refs)
        first, last = rng.choice(FIRST_NAMES), rng.choice(LAST_NAMES)
        taken, statuses, completed = travel(rng, world, plan)

        planned_arr = plan[-1]["sched_arr"]
        actual_arr = taken[-1]["arr"] if completed else None
        if not completed:
            journey_status = "INCOMPLETE_AT_PERIOD_END"
        elif all(s == "FLOWN_AS_PLANNED" for s in statuses):
            journey_status = "AS_PLANNED"
        else:
            journey_status = "REBOOKED"

        passengers.append({
            "booking_ref": ref, "passenger_id": f"PAX-{n:06d}",
            "passenger_name": f"{first} {last}",
            "email": f"{first}.{last}.{n}@example.com".lower().replace(" ", ""),
            "frequent_flyer_tier": weighted(rng, FF_TIER_MIX), "cabin": weighted(rng, CABIN_MIX[body]),
            "airline_code": airline, "origin_airport_code": plan[0]["origin"],
            "final_destination_code": plan[-1]["dest"], "is_connecting": len(plan) > 1,
            "planned_arrival_utc": fmt_utc(planned_arr), "actual_arrival_utc": fmt_utc(actual_arr),
            "arrival_delay_minutes": (round((actual_arr - planned_arr).total_seconds() / 60)
                                      if actual_arr else ""),
            "journey_status": journey_status,
            "_plan": plan, "_taken": taken,
        })
        for i, planned in enumerate(plan):
            segments.append({
                "booking_ref": ref, "segment_no": i + 1, "planned_flight_id": planned["id"],
                "actual_flight_id": taken[i]["id"] if i < len(taken) else "",
                "segment_status": statuses[i] if i < len(statuses) else "NOT_FLOWN_BY_PERIOD_END",
            })

        counts, weights = zip(*BAGS_PER_PASSENGER[body])
        for _ in range(rng.choices(counts, weights=weights, k=1)[0]):
            bag = {"bag_tag": factory.new_tag(airline), "booking_ref": ref, "airline_code": airline,
                   "origin_airport_code": plan[0]["origin"], "final_destination_code": plan[-1]["dest"],
                   "weight_kg": round(rng.uniform(*BAG_WEIGHT_KG), 1)}
            if completed:
                status, delivered = factory.journey(world, bag, taken, plan)
            elif taken:
                status, delivered = factory.stranded(world, bag, taken)
            else:
                status, delivered = factory.returned(bag, plan)
            bag.update(bag_status=status, delivered_utc=fmt_utc(delivered),
                       _pax_arrival=actual_arr)
            bags.append(bag)

    return passengers, segments, bags, factory.scans


# =====================================================
# VALIDATION
# =====================================================
def validate(world, passengers, segments, bags, scans):
    problems = []

    # 1) no actual flight carries more passengers than it has seats
    flown = Counter(s["actual_flight_id"] for s in segments if s["actual_flight_id"])
    for fid, n in flown.items():
        if n > world.flights[fid]["seats"]:
            problems.append(f"{fid}: {n} passengers but only {world.flights[fid]['seats']} seats")
        if world.flights[fid]["cancelled"]:
            problems.append(f"{fid}: passengers flown on a CANCELLED flight")

    # 2) passenger journeys are physically possible (connect in the right place, in time)
    for p in passengers:
        for a, b in zip(p["_taken"], p["_taken"][1:]):
            if a["dest"] != b["origin"]:
                problems.append(f"{p['booking_ref']}: connection airport mismatch")
            if b["dep"] - minutes(BOARDING_CLOSES_MIN) < a["arr"]:
                problems.append(f"{p['booking_ref']}: boarded {b['id']} before landing from {a['id']}")

    # 3) bag tags unique; every bag's scans are in time order and consistent with its flights
    tags = [b["bag_tag"] for b in bags]
    if len(tags) != len(set(tags)):
        problems.append("Duplicate bag tags")
    by_tag = defaultdict(list)
    for s in scans:
        by_tag[s["bag"]["tag"]].append(s)
    for tag, events in by_tag.items():
        for prev, cur in zip(events, events[1:]):
            if cur["_t"] < prev["_t"]:
                problems.append(f"{tag}: scan {cur['event_type']} earlier than {prev['event_type']}")
        for s in events:
            f = world.flights.get(s["flight"]["flight_id"]) if s["flight"] else None
            if s["event_type"] == "LOADED":
                if f["cancelled"] or s["_t"] >= f["dep"] or s["location"]["airport"] != f["origin"]:
                    problems.append(f"{tag}: impossible LOADED scan on {f['id']}")
            if s["event_type"] == "OFFLOADED":
                if s["_t"] <= f["arr"] or s["location"]["airport"] != f["dest"]:
                    problems.append(f"{tag}: impossible OFFLOADED scan on {f['id']}")

    # 4) reconciliation: every bag ends in exactly one known status
    statuses = Counter(b["bag_status"] for b in bags)
    if sum(statuses.values()) != len(bags):
        problems.append("Bag statuses do not reconcile")

    if problems:
        raise ValueError("Passenger/bag validation failed:\n  " + "\n  ".join(problems[:20]))
    print("VALIDATION PASSED (no overbooking, possible connections, bag scans consistent with flights)")


# =====================================================
# SUMMARY
# =====================================================
def summarise(world, passengers, segments, bags, scans):
    n_pax = len(passengers)
    connecting = sum(1 for p in passengers if p["is_connecting"])
    print(f"\nPASSENGER JOURNEYS: {n_pax}   connecting: {connecting} ({100 * connecting / n_pax:.0f}%)")
    print(f"PASSENGER SEGMENTS: {len(segments)}")
    for status, n in Counter(p["journey_status"] for p in passengers).most_common():
        print(f"  {status:<16} {n}")
    late = [p for p in passengers if p["arrival_delay_minutes"] != "" and p["arrival_delay_minutes"] >= 15]
    print(f"Journeys arriving 15+ min late: {len(late)} ({100 * len(late) / n_pax:.1f}%)")

    print(f"\nBAGS: {len(bags)}   SCAN EVENTS: {len(scans)}")
    for status, n in Counter(b["bag_status"] for b in bags).most_common():
        print(f"  {status:<28} {n}")
    mishandled = sum(1 for b in bags if b["bag_status"].startswith("MISHANDLED"))
    print(f"MISHANDLED BAGS PER 1,000 PASSENGERS: {1000 * mishandled / n_pax:.1f}")

    # impact chain: which late inbound flight caused the most misconnections?
    impact = defaultdict(lambda: {"pax": 0, "bags": 0, "onward": set()})
    seg_by_ref = defaultdict(list)
    for s in segments:
        seg_by_ref[s["booking_ref"]].append(s)
    for p in passengers:
        segs = seg_by_ref[p["booking_ref"]]
        if len(segs) == 2 and segs[1]["segment_status"] == "REBOOKED_MISCONNECTION":
            impact[segs[0]["actual_flight_id"]]["pax"] += 1
            impact[segs[0]["actual_flight_id"]]["onward"].add(segs[1]["planned_flight_id"])
    for b in bags:
        if b["bag_status"].startswith("MISHANDLED"):
            segs = seg_by_ref[b["booking_ref"]]
            impact[segs[0]["actual_flight_id"]]["bags"] += 1
    if impact:
        fid, i = max(impact.items(), key=lambda kv: (kv[1]["pax"] + kv[1]["bags"], kv[0]))
        f = world.flights[fid]
        delay = round((f["dep"] - f["sched_dep"]).total_seconds() / 60)
        print(f"\nBIGGEST IMPACT CHAIN: {fid} {f['origin']}->{f['dest']} departed {delay} min late")
        print(f"  -> {i['pax']} passengers misconnected  -> {i['bags']} bags mishandled"
              f"  -> {len(i['onward'])} onward flights affected")


# =====================================================
# OUTPUT
# =====================================================
def write_csv_and_upload(session, filename, columns, rows):
    path = OUT_LOCAL / filename
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    session.file.put(str(path), TRUTH_STAGE, auto_compress=False, overwrite=True)
    print(f"UP  {filename:<24} {len(rows):>7} rows -> {TRUTH_STAGE}")


def write_jsonl_gz_and_upload(session, filename, rows):
    path = OUT_LOCAL / filename
    with gzip.open(path, "wt", encoding="utf-8") as fh:
        for r in sorted(rows, key=lambda r: (r["_t"], r["scan_id"])):     # in event-time order
            fh.write(json.dumps({k: v for k, v in r.items() if not k.startswith("_")}) + "\n")
    session.file.put(str(path), TRUTH_STAGE, auto_compress=False, overwrite=True)
    print(f"UP  {filename:<24} {len(rows):>7} rows -> {TRUTH_STAGE}  "
          f"({path.stat().st_size / 1_000_000:.1f} MB compressed)")


def main():
    session = get_active_session()
    rng = random.Random(SEED)
    IN_LOCAL.mkdir(parents=True, exist_ok=True)
    OUT_LOCAL.mkdir(parents=True, exist_ok=True)

    airports = load_csv(session, REF_STAGE, "airports.csv")
    airlines = load_csv(session, REF_STAGE, "airlines.csv")
    aircraft = load_csv(session, REF_STAGE, "aircraft.csv")
    mct_rows = load_csv(session, REF_STAGE, "min_connection_times.csv")
    schedule = load_csv(session, TRUTH_STAGE, "flights_schedule.csv")
    actuals = load_csv(session, TRUTH_STAGE, "flights_actual.csv")

    world = FlightWorld(schedule, actuals, airports, airlines, aircraft, mct_rows)
    passengers, segments, bags, scans = build(rng, world)
    validate(world, passengers, segments, bags, scans)
    summarise(world, passengers, segments, bags, scans)

    print()
    write_csv_and_upload(session, "passengers.csv", PASSENGER_COLUMNS, passengers)
    write_csv_and_upload(session, "passenger_segments.csv", SEGMENT_COLUMNS, segments)
    write_csv_and_upload(session, "bags.csv", BAG_COLUMNS, bags)
    write_jsonl_gz_and_upload(session, "bag_scans.jsonl.gz", scans)


main()