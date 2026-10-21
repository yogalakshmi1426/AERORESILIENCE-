"""
AERORESILIENCE - Synthetic data generator (Snowflake)
01 REFERENCE DATA  (v2: planned fleets, route network, connections, suppliers, parts)

Uploads to @AERORESILIENCE.BRONZE.RAW_STAGE/reference/ :
    airports.csv              grain: one airport
    airlines.csv              grain: one airline
    aircraft.csv              grain: one aircraft (one tail registration)
    routes.csv                grain: one airline flying ONE DIRECTION between two airports
    min_connection_times.csv  grain: one airport + one connection type   (composite key)
    suppliers.csv             grain: one supplier
    parts.csv                 grain: one part number (fits exactly one aircraft type)

NOTE: block times, MCTs and costs are realistic approximations for simulation,
not official figures. Supplier names are fictional.
"""

import csv
import random
from pathlib import Path
from snowflake.snowpark.context import get_active_session

# =====================================================
# SETTINGS
# =====================================================
SEED = 42
LOCAL_DIR = Path("/tmp/aeroresilience/reference")
STAGE_PATH = "@AERORESILIENCE.BRONZE.RAW_STAGE/reference/"

# =====================================================
# AIRPORTS: (airport_code, airport_name, city, country_code, timezone)
# =====================================================
AIRPORTS = [
    ("LHR", "London Heathrow", "London", "GB", "Europe/London"),
    ("MAN", "Manchester", "Manchester", "GB", "Europe/London"),
    ("AMS", "Amsterdam Schiphol", "Amsterdam", "NL", "Europe/Amsterdam"),
    ("CDG", "Paris Charles de Gaulle", "Paris", "FR", "Europe/Paris"),
    ("FRA", "Frankfurt", "Frankfurt", "DE", "Europe/Berlin"),
    ("DXB", "Dubai International", "Dubai", "AE", "Asia/Dubai"),
    ("DOH", "Hamad International", "Doha", "QA", "Asia/Qatar"),
    ("BOM", "Chhatrapati Shivaji Maharaj International", "Mumbai", "IN", "Asia/Kolkata"),
    ("SIN", "Singapore Changi", "Singapore", "SG", "Asia/Singapore"),
    ("JFK", "John F. Kennedy International", "New York", "US", "America/New_York"),
]

# =====================================================
# AIRLINES: (airline_code, airline_name, country_code, hub_airport_code)
# =====================================================
AIRLINES = [
    ("BA", "British Airways", "GB", "LHR"),
    ("EK", "Emirates", "AE", "DXB"),
    ("KL", "KLM Royal Dutch Airlines", "NL", "AMS"),
    ("QR", "Qatar Airways", "QA", "DOH"),
]

# =====================================================
# AIRCRAFT TYPES (keyed by ICAO type code)
# icao_type: (manufacturer, model, body_type, typical_seats, first_year_built)
# =====================================================
AIRCRAFT_TYPES = {
    "A320": ("Airbus", "A320",      "NARROW", 180, 2008),
    "B738": ("Boeing", "737-800",   "NARROW", 186, 2008),
    "A359": ("Airbus", "A350-900",  "WIDE",   325, 2015),
    "B77W": ("Boeing", "777-300ER", "WIDE",   354, 2008),
    "B789": ("Boeing", "787-9",     "WIDE",   290, 2014),
}
LAST_YEAR_BUILT = 2024

# Planned fleets: airline -> {icao_type: number of aircraft}
FLEETS = {
    "BA": {"A320": 3, "B789": 2, "B77W": 1},
    "EK": {"B77W": 4, "A359": 1},              # Emirates: wide-body only
    "KL": {"B738": 3, "B789": 2},
    "QR": {"A359": 2, "B789": 1, "A320": 1},
}

# Registration prefix = country of registration (G- UK, A6- UAE, PH- NL, A7- Qatar)
REGISTRATION_PREFIX = {"BA": "G-", "EK": "A6-", "KL": "PH-", "QR": "A7-"}

# =====================================================
# ROUTE NETWORK (hub-and-spoke)
# airline: [(spoke, minutes hub->spoke, minutes spoke->hub, required_body_type)]
# Directions differ because of the jet stream (eastbound is usually faster)
# =====================================================
ROUTE_NETWORK = {
    "BA": [("MAN", 60, 60, "NARROW"), ("AMS", 75, 75, "NARROW"), ("CDG", 75, 80, "NARROW"),
           ("FRA", 95, 95, "NARROW"), ("DXB", 410, 460, "WIDE"), ("JFK", 490, 415, "WIDE"),
           ("SIN", 780, 830, "WIDE")],
    "EK": [("LHR", 460, 410, "WIDE"), ("MAN", 470, 420, "WIDE"), ("AMS", 430, 390, "WIDE"),
           ("BOM", 175, 190, "WIDE"), ("SIN", 440, 450, "WIDE"), ("JFK", 840, 780, "WIDE")],
    "KL": [("LHR", 70, 75, "NARROW"), ("MAN", 80, 80, "NARROW"), ("CDG", 80, 80, "NARROW"),
           ("FRA", 70, 70, "NARROW"), ("JFK", 500, 430, "WIDE"), ("SIN", 765, 820, "WIDE"),
           ("DXB", 390, 430, "WIDE")],
    "QR": [("LHR", 445, 400, "WIDE"), ("MAN", 450, 410, "WIDE"), ("CDG", 420, 385, "WIDE"),
           ("FRA", 395, 365, "WIDE"), ("BOM", 195, 225, "NARROW"), ("SIN", 450, 480, "WIDE")],
}

# =====================================================
# MINIMUM CONNECTION TIMES (illustrative)
# (airport_code, connection_type, minutes)  -> composite key: airport + type
# =====================================================
MIN_CONNECTION_TIMES = [
    ("LHR", "DOM_DOM", 45), ("LHR", "DOM_INTL", 60), ("LHR", "INTL_DOM", 75), ("LHR", "INTL_INTL", 60),
    ("MAN", "DOM_DOM", 40), ("MAN", "DOM_INTL", 50), ("MAN", "INTL_DOM", 60), ("MAN", "INTL_INTL", 50),
    ("AMS", "INTL_INTL", 50), ("CDG", "INTL_INTL", 60), ("FRA", "INTL_INTL", 45),
    ("DXB", "INTL_INTL", 75), ("DOH", "INTL_INTL", 60), ("BOM", "INTL_INTL", 90),
    ("SIN", "INTL_INTL", 60), ("JFK", "INTL_INTL", 90),
]

# =====================================================
# SUPPLIERS (fictional)
# (supplier_id, supplier_name, country_code, ship_from_airport_code, on_time_rate)
# on_time_rate is used in Part 5 to decide how often shipments arrive late
# =====================================================
SUPPLIERS = [
    ("SUP-01", "Aerotek Components Ltd",      "GB", "LHR", 0.92),
    ("SUP-02", "Hollandia Aero Parts BV",     "NL", "AMS", 0.95),
    ("SUP-03", "Gulf Aviation Supply LLC",    "AE", "DXB", 0.88),
    ("SUP-04", "Rhein Avionik GmbH",          "DE", "FRA", 0.90),
    ("SUP-05", "Lion City Aerospace Pte Ltd", "SG", "SIN", 0.85),
    ("SUP-06", "Atlantic Hydraulics Inc",     "US", "JFK", 0.80),
]

# =====================================================
# PART TEMPLATES  -> one part number per template per aircraft type
# (ata_chapter, description, criticality, cost_min_gbp, cost_max_gbp, lead_min_days, lead_max_days)
# criticality follows the MEL idea: NO_GO = cannot fly, GO_IF = fly with conditions, GO = no impact
# =====================================================
PART_TEMPLATES = [
    ("21", "Air cycle machine",            "GO_IF", 60000, 110000, 5, 14),
    ("24", "Integrated drive generator",   "GO_IF", 90000, 150000, 7, 21),
    ("29", "Engine-driven hydraulic pump", "NO_GO", 35000,  60000, 3, 10),
    ("32", "Main wheel assembly",          "NO_GO",  8000,  15000, 1,  4),
    ("32", "Brake assembly",               "NO_GO", 20000,  45000, 2,  7),
    ("34", "Weather radar transceiver",    "GO_IF", 40000,  80000, 5, 14),
    ("36", "Bleed air valve",              "GO_IF",  9000,  18000, 3, 10),
    ("49", "APU starter motor",            "GO_IF", 12000,  25000, 3, 10),
    ("33", "Cabin reading light unit",     "GO",       50,    200, 1,  5),
    ("38", "Lavatory flush motor",         "GO",     1500,   3000, 2,  7),
    ("52", "Passenger door seal kit",      "GO",      300,    900, 1,  5),
]


# =====================================================
# GENERATORS
# =====================================================
def make_registration(prefix):
    letters = "".join(random.choices("ABCDEFGHIJKLMNOPQRSTUVWXYZ", k=3))
    return f"{prefix}{letters}"


def generate_aircraft():
    """Build aircraft from the PLANNED fleets (not random models)."""
    rows = []
    used = set()
    for airline_code, _name, _country, _hub in AIRLINES:
        for icao_type, count in FLEETS[airline_code].items():
            manufacturer, model, body_type, seats, first_year = AIRCRAFT_TYPES[icao_type]
            for _ in range(count):
                reg = make_registration(REGISTRATION_PREFIX[airline_code])
                while reg in used:
                    reg = make_registration(REGISTRATION_PREFIX[airline_code])
                used.add(reg)
                year_built = random.randint(first_year, LAST_YEAR_BUILT)
                rows.append((reg, airline_code, icao_type, manufacturer, model,
                             body_type, seats, year_built))
    return rows


def generate_routes():
    """One row per direction: hub->spoke and spoke->hub are different routes."""
    hubs = {code: hub for code, _n, _c, hub in AIRLINES}
    rows = []
    for airline_code, legs in ROUTE_NETWORK.items():
        hub = hubs[airline_code]
        for spoke, out_min, back_min, body_type in legs:
            rows.append((f"{airline_code}-{hub}-{spoke}", airline_code, hub, spoke, out_min, body_type))
            rows.append((f"{airline_code}-{spoke}-{hub}", airline_code, spoke, hub, back_min, body_type))
    return rows


def generate_parts():
    """One part number per template per aircraft type, e.g. 32-B77W-05."""
    rows = []
    supplier_ids = [s[0] for s in SUPPLIERS]
    for icao_type in AIRCRAFT_TYPES:
        for i, (ata, desc, crit, cmin, cmax, lmin, lmax) in enumerate(PART_TEMPLATES, start=1):
            part_number = f"{ata}-{icao_type}-{i:02d}"
            unit_cost = round(random.uniform(cmin, cmax), -1)        # nearest 10 GBP
            lead_days = random.randint(lmin, lmax)
            supplier_id = random.choice(supplier_ids)
            rows.append((part_number, desc, ata, icao_type, crit, unit_cost,
                         lead_days, supplier_id))
    return rows


# =====================================================
# VALIDATION - stop before uploading broken reference data
# =====================================================
def validate(aircraft, routes, parts):
    airport_codes = {a[0] for a in AIRPORTS}
    airline_codes = {a[0] for a in AIRLINES}
    supplier_ids = {s[0] for s in SUPPLIERS}
    problems = []

    for code, _n, _c, hub in AIRLINES:
        if hub not in airport_codes:
            problems.append(f"Airline {code}: hub {hub} is not a known airport")

    for route_id, airline, origin, dest, _mins, body in routes:
        if origin not in airport_codes or dest not in airport_codes:
            problems.append(f"Route {route_id}: unknown airport")
        fleet_bodies = {AIRCRAFT_TYPES[t][2] for t in FLEETS[airline]}
        if body not in fleet_bodies:
            problems.append(f"Route {route_id}: needs {body} but {airline} has no {body} aircraft")

    for airport, _type, _mins in MIN_CONNECTION_TIMES:
        if airport not in airport_codes:
            problems.append(f"MCT: unknown airport {airport}")

    if len({r[0] for r in routes}) != len(routes):
        problems.append("Duplicate route_id found")
    if len({a[0] for a in aircraft}) != len(aircraft):
        problems.append("Duplicate aircraft registration found")
    if len({p[0] for p in parts}) != len(parts):
        problems.append("Duplicate part_number found")

    for p in parts:
        if p[7] not in supplier_ids:
            problems.append(f"Part {p[0]}: unknown supplier {p[7]}")
    for a in aircraft:
        if a[1] not in airline_codes:
            problems.append(f"Aircraft {a[0]}: unknown airline {a[1]}")

    if problems:
        raise ValueError("Reference data validation failed:\n  " + "\n  ".join(problems))
    print("VALIDATION PASSED")


# =====================================================
# OUTPUT
# =====================================================
def write_csv(filename, header, rows):
    path = LOCAL_DIR / filename
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)
    print(f"OK  {filename:<26} {len(rows):>4} rows")
    return path


def upload_to_stage(session, local_path):
    session.file.put(str(local_path), STAGE_PATH, auto_compress=False, overwrite=True)
    print(f"UP  {local_path.name:<26} -> {STAGE_PATH}")


def main():
    random.seed(SEED)
    LOCAL_DIR.mkdir(parents=True, exist_ok=True)

    aircraft = generate_aircraft()
    routes = generate_routes()
    parts = generate_parts()

    validate(aircraft, routes, parts)          # nothing is uploaded unless this passes

    files = [
        write_csv("airports.csv",
                  ["airport_code", "airport_name", "city", "country_code", "timezone"],
                  AIRPORTS),
        write_csv("airlines.csv",
                  ["airline_code", "airline_name", "country_code", "hub_airport_code"],
                  AIRLINES),
        write_csv("aircraft.csv",
                  ["registration", "airline_code", "icao_type", "manufacturer", "model",
                   "body_type", "seats", "year_built"],
                  aircraft),
        write_csv("routes.csv",
                  ["route_id", "airline_code", "origin_airport_code",
                   "destination_airport_code", "block_time_minutes", "required_body_type"],
                  routes),
        write_csv("min_connection_times.csv",
                  ["airport_code", "connection_type", "min_connection_minutes"],
                  MIN_CONNECTION_TIMES),
        write_csv("suppliers.csv",
                  ["supplier_id", "supplier_name", "country_code",
                   "ship_from_airport_code", "on_time_rate"],
                  SUPPLIERS),
        write_csv("parts.csv",
                  ["part_number", "description", "ata_chapter", "icao_type", "criticality",
                   "unit_cost_gbp", "standard_lead_time_days", "supplier_id"],
                  parts),
    ]

    session = get_active_session()
    for path in files:
        upload_to_stage(session, path)

    print("Reference data v2 generated, validated and uploaded.")


main()