"""
mock_data_generator.py

Generates realistic synthetic data for:
  1. Local unit-test fixtures (returned as plain Python dicts / lists).
  2. BigQuery seeding (INSERT rows into all tables for integration testing
     and ML recommendation-system training).

Usage
-----
# Print a summary of what would be generated (dry run):
    python mock_data_generator.py --dry-run

# Seed BigQuery with default row counts:
    python mock_data_generator.py --seed-bq

# Seed with custom counts:
    python mock_data_generator.py --seed-bq --users 50 --events 200

# Export generated data to JSON for offline use:
    python mock_data_generator.py --export mock_data.json
"""

import argparse
import json
import random
import uuid
from datetime import datetime, timedelta, timezone
from google.cloud import bigquery

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

PROJECT_ID = "carlos-negron-uprm"
DATASET = "database"

SPORTS = ["Soccer", "Basketball", "Volleyball", "Tennis", "Baseball", "Pickleball"]
SKILL_LEVELS = ["beginner", "intermediate", "advanced"]
EVENT_STATUSES = ["open", "full", "completed", "cancelled"]
FRIENDSHIP_STATUSES = ["pending", "accepted", "blocked"]
ACTIVITY_TYPES = ["join_event", "leave_event", "view_event", "search", "session_complete"]
VISIBILITY = ["public", "private"]

# Miami-Dade / South Florida bounding box (matching app.py mock location)
LAT_RANGE = (25.60, 25.90)
LNG_RANGE = (-80.40, -80.10)

VENUE_NAMES = [
    "Tropical Park",
    "Tamiami Park",
    "Jose Marti Park",
    "Lummus Park",
    "Flamingo Park",
    "Bayfront Park",
    "Amelia Earhart Park",
    "Crandon Park",
    "Matheson Hammock",
    "A.D. Barnes Park",
]

FIRST_NAMES = [
    "Carlos", "Maria", "Jean", "Sofia", "Luis", "Ana", "Miguel", "Isabella",
    "Diego", "Valentina", "Andres", "Camila", "Rafael", "Daniela", "Jorge",
    "Natalia", "Fernando", "Gabriela", "Marco", "Paula",
]
LAST_NAMES = [
    "Garcia", "Rodriguez", "Martinez", "Lopez", "Gonzalez", "Hernandez",
    "Perez", "Sanchez", "Ramirez", "Torres", "Flores", "Rivera", "Cruz",
    "Morales", "Reyes", "Jimenez", "Ortiz", "Alvarez", "Mendoza", "Ruiz",
]

# ---------------------------------------------------------------------------
# Low-level helpers
# ---------------------------------------------------------------------------

def _uid() -> str:
    return str(uuid.uuid4())


def _rand_lat() -> float:
    """Return a random latitude within the South Florida bounding box."""
    return round(random.uniform(*LAT_RANGE), 6)  # FIX: was using LNG_RANGE


def _rand_lng() -> float:
    """Return a random longitude within the South Florida bounding box."""
    return round(random.uniform(*LNG_RANGE), 6)  # FIX: missing function added


def _rand_ts(days_offset_min: int = -60, days_offset_max: int = 60) -> datetime:
    """Return a random UTC datetime within an offset window from now."""
    now = datetime.now(timezone.utc)
    delta = timedelta(
        days=random.randint(days_offset_min, days_offset_max),  # FIX: was randit
        hours=random.randint(0, 23),                            # FIX: was randit
        minutes=random.choice([0, 15, 30, 45])
    )
    return now + delta


def _rand_email(first: str, last: str) -> str:
    domains = ["gmail.com", "yahoo.com", "hotmail.com", "outlook.com"]
    return f"{first.lower()}.{last.lower()}{random.randint(1, 99)}@{random.choice(domains)}"


# ---------------------------------------------------------------------------
# Entity generators
# ---------------------------------------------------------------------------

def generate_users(n: int = 20) -> list[dict]:
    """
    Generate `n` synthetic user records.

    Each user has:
      - 1–3 sports with random skill levels
      - A home location within the South Florida bounding box
    """
    users = []
    for _ in range(n):
        first = random.choice(FIRST_NAMES)
        last  = random.choice(LAST_NAMES)
        lat   = _rand_lat()   # FIX: was _rand_lat() called correctly now
        lng   = _rand_lng()   # FIX: was missing _rand_lng definition
        num_sports = random.randint(1, 3)
        sports = [
            {"sport": s, "skill_level": random.choice(SKILL_LEVELS)}
            for s in random.sample(SPORTS, num_sports)
        ]
        users.append({
            "user_id":    _uid(),
            "email":      _rand_email(first, last),
            "created_at": _rand_ts(-180, -1).isoformat(),
            "home_lat":   lat,
            "home_lng":   lng,
            "sports":     sports,
        })
    return users


def generate_locations(n: int = 15) -> list[dict]:
    """
    Generate `n` synthetic venue / location records.
    """
    locations = []
    for i in range(n):
        lat  = _rand_lat()   # FIX: was rand_lat()
        lng  = _rand_lng()   # FIX: was rand_lng()
        name = random.choice(VENUE_NAMES) + (f" #{i+1}" if i >= len(VENUE_NAMES) else "")
        locations.append({
            "location_id": _uid(),
            "name":        name,
            "address":     f"{random.randint(100, 9999)} SW {random.randint(1, 199)} St, Miami, FL",
            "lat":         lat,
            "lng":         lng,
            "created_at":  _rand_ts(-365, -30).isoformat(),
            "created_by":  None,
            "is_public":   random.choice([True, True, True, False]),
        })
    return locations


def generate_events(
    n: int = 40,
    user_ids: list[str] | None = None,
    locations: list[dict] | None = None,
) -> list[dict]:
    """
    Generate `n` synthetic events.

    Parameters
    ----------
    user_ids  : Pool of user_ids to assign as creators.
    locations : Pool of location dicts to embed as the location STRUCT.
    """
    if not user_ids:
        user_ids = [_uid() for _ in range(5)]
    if not locations:
        locations = generate_locations(10)

    events = []
    for _ in range(n):
        loc        = random.choice(locations)
        start_time = _rand_ts(-30, 60)
        duration   = timedelta(hours=random.choice([1, 1.5, 2, 3]))
        end_time   = start_time + duration

        now = datetime.now(timezone.utc)
        if end_time < now:
            status = random.choices(
                ["completed", "cancelled"], weights=[85, 15]
            )[0]
        else:
            status = random.choices(
                ["open", "full"], weights=[80, 20]
            )[0]

        events.append({
            "event_id":   _uid(),
            "sport":      random.choice(SPORTS),
            "location": {
                "location_id": loc["location_id"],
                "name":        loc["name"],
                "address":     loc["address"],
                "lat":         loc["lat"],
                "lng":         loc["lng"],
            },
            "created_by":  random.choice(user_ids),
            "start_time":  start_time.isoformat(),
            "end_time":    end_time.isoformat(),
            "max_players": random.choice([6, 8, 10, 12, 16, 22]),
            "visibility":  random.choices(["public", "private"], weights=[85, 15])[0],
            "status":      status,
            "created_at":  _rand_ts(-60, -1).isoformat(),
            "updated_at":  _rand_ts(-10, 0).isoformat(),
        })
    return events


def generate_friendships(user_ids: list[str], max_pairs: int | None = None) -> list[dict]:  # FIX: none → None
    """
    Generate bidirectional friendship rows (two rows per pair, one per direction).

    Produces at most len(user_ids) * 2 unique pairs, capped at `max_pairs`.
    """
    if max_pairs is None:
        max_pairs = len(user_ids) * 2

    all_pairs = set()
    rows      = []

    shuffled = user_ids[:]  # FIX: was user.ids[:]
    random.shuffle(shuffled)

    for i, uid in enumerate(shuffled):
        candidates  = [u for u in shuffled if u != uid]
        num_friends = random.randint(1, min(5, len(candidates)))  # FIX: was randit
        for friend in random.sample(candidates, num_friends):
            pair = tuple(sorted([uid, friend]))
            if pair in all_pairs or len(all_pairs) >= max_pairs:
                continue
            all_pairs.add(pair)

            status       = random.choices(FRIENDSHIP_STATUSES, weights=[10, 80, 10])[0]
            requested_by = random.choice([uid, friend])
            now          = _rand_ts(-180, -1).isoformat()

            # Direction 1
            rows.append({
                "user_id":      uid,
                "friend_id":    friend,
                "status":       status,
                "requested_by": requested_by,
                "created_at":   now,
                "updated_at":   now,
            })
            # Direction 2 (mirror)
            rows.append({
                "user_id":      friend,
                "friend_id":    uid,
                "status":       status,
                "requested_by": requested_by,
                "created_at":   now,
                "updated_at":   now,
            })

    return rows


def generate_event_participants(
    event_ids: list[str],
    user_ids: list[str],
    rows_per_event_range: tuple[int, int] = (2, 8),
) -> list[dict]:
    """
    Generate append-only participant rows.

    For realism, some users have a 'left' row after a 'joined' row.
    """
    rows = []
    for event_id in event_ids:
        n_participants = random.randint(*rows_per_event_range)
        participants   = random.sample(user_ids, min(n_participants, len(user_ids)))
        base_time      = _rand_ts(-30, -1)

        for user_id in participants:
            joined_at = base_time + timedelta(minutes=random.randint(0, 120))
            rows.append({
                "event_id":  event_id,
                "user_id":   user_id,
                "joined_at": joined_at.isoformat(),
                "status":    "joined",
            })
            # ~20 % chance the user later left
            if random.random() < 0.20:
                left_at = joined_at + timedelta(minutes=random.randint(5, 60))
                rows.append({
                    "event_id":  event_id,
                    "user_id":   user_id,
                    "joined_at": left_at.isoformat(),
                    "status":    "left",
                })
    return rows


def generate_user_activity(
    user_ids: list[str],
    event_ids: list[str],   # FIX: was lsit[str]
    rows_per_user: int = 10,
) -> list[dict]:
    """
    Generate user_activity rows simulating realistic app interactions.
    """
    rows = []
    for user_id in user_ids:
        for _ in range(rows_per_user):
            activity_type = random.choice(ACTIVITY_TYPES)
            sport         = random.choice(SPORTS)
            lat           = _rand_lat()   # FIX: was _rand.lat()
            lng           = _rand_lng()   # FIX: was _rand.lng()
            ts            = _rand_ts(-60, 0)
            duration      = (random.randint(30, 180) if activity_type == "session_complete" else None)  # FIX: was randit
            event_id      = (
                random.choice(event_ids)
                if activity_type in ("join_event", "leave_event", "view_event")  # FIX: was "joing_event"
                else None
            )

            rows.append({
                "activity_id":      str(uuid.uuid4()),
                "user_id":          user_id,
                "event_id":         event_id,
                "sport":            sport,
                "duration_minutes": duration,
                "location": {
                    "location_id": _uid(),
                    "name":        random.choice(VENUE_NAMES),
                    "lat":         lat,
                    "lng":         lng,
                },
                "activity_type": activity_type,
                "timestamp":     ts.isoformat(),
            })
    return rows


def generate_recommendations(
    user_ids: list[str],
    event_ids: list[str],
    recs_per_user: int = 5,
) -> list[dict]:
    """
    Generate plausible precomputed recommendation scores.

    Scores are drawn from a Beta(5, 2) distribution so the cache
    skews toward high-confidence recommendations, which is realistic
    for a trained model output.
    """
    rows = []
    for user_id in user_ids:
        sampled_events = random.sample(event_ids, min(recs_per_user, len(event_ids)))
        for event_id in sampled_events:
            rows.append({
                "user_id":      user_id,
                "event_id":     event_id,
                "score":        round(random.betavariate(5, 2), 4),
                "generated_at": _rand_ts(-1, 0).isoformat(),
            })
    return rows


# ---------------------------------------------------------------------------
# Full dataset generator
# ---------------------------------------------------------------------------

def generate_all(
    n_users: int  = 20,
    n_events: int = 40,
) -> dict:
    """
    Generate a complete, internally consistent mock dataset.

    Returns a dict with keys: users, locations, events, friendships,
    participants, activity, recommendations.
    """
    users     = generate_users(n_users)
    locations = generate_locations(max(10, n_events // 4))

    user_ids  = [u["user_id"] for u in users]
    loc_dicts = locations

    # Patch location created_by with real user_ids
    for loc in loc_dicts:
        loc["created_by"] = random.choice(user_ids)

    events       = generate_events(n_events, user_ids, loc_dicts)
    event_ids    = [e["event_id"] for e in events]

    friendships  = generate_friendships(user_ids)
    participants = generate_event_participants(event_ids, user_ids)
    activity     = generate_user_activity(user_ids, event_ids)
    recs         = generate_recommendations(user_ids, event_ids)

    return {
        "users":           users,
        "locations":       locations,
        "events":          events,
        "friendships":     friendships,
        "participants":    participants,
        "activity":        activity,
        "recommendations": recs,
    }


# ---------------------------------------------------------------------------
# BigQuery seeder
# ---------------------------------------------------------------------------

def _tbl(name: str) -> str:
    return f"{PROJECT_ID}.{DATASET}.{name}"


def _bq_insert_users(client: bigquery.Client, users: list[dict]) -> None:
    rows = []
    for u in users:
        rows.append({
            "user_id":    u["user_id"],
            "email":      u["email"],
            "created_at": u["created_at"],
            "home_lat":   u["home_lat"],
            "home_lng":   u["home_lng"],
            "sports":     u["sports"],
        })
    errors = client.insert_rows_json(_tbl("users"), rows)
    if errors:
        raise RuntimeError(f"users insert errors: {errors}")


def _bq_insert_locations(client: bigquery.Client, locations: list[dict]) -> None:
    rows = []
    for loc in locations:
        rows.append({
            "location_id": loc["location_id"],
            "name":        loc["name"],
            "address":     loc["address"],
            "lat":         loc["lat"],
            "lng":         loc["lng"],
            "created_at":  loc["created_at"],
            "created_by":  loc["created_by"],
            "is_public":   loc["is_public"],
        })
    errors = client.insert_rows_json(_tbl("locations"), rows)
    if errors:
        raise RuntimeError(f"locations insert errors: {errors}")


def _bq_insert_events(client: bigquery.Client, events: list[dict]) -> None:
    rows = []
    for e in events:
        loc = e["location"]
        rows.append({
            "event_id":    e["event_id"],
            "sport":       e["sport"],
            "location": {
                "location_id": loc["location_id"],
                "name":        loc["name"],
                "address":     loc.get("address", ""),
                "lat":         loc["lat"],
                "lng":         loc["lng"],
            },
            "created_by":  e["created_by"],
            "start_time":  e["start_time"],
            "end_time":    e["end_time"],
            "max_players": e["max_players"],
            "visibility":  e["visibility"],
            "status":      e["status"],
            "created_at":  e["created_at"],
            "updated_at":  e["updated_at"],
        })
    errors = client.insert_rows_json(_tbl("events"), rows)
    if errors:
        raise RuntimeError(f"events insert errors: {errors}")


def _bq_insert_friendships(client: bigquery.Client, rows: list[dict]) -> None:
    errors = client.insert_rows_json(_tbl("friendship"), rows)
    if errors:
        raise RuntimeError(f"friendship insert errors: {errors}")


def _bq_insert_participants(client: bigquery.Client, rows: list[dict]) -> None:
    errors = client.insert_rows_json(_tbl("event_participants"), rows)
    if errors:
        raise RuntimeError(f"event_participants insert errors: {errors}")


def _bq_insert_activity(client: bigquery.Client, activity: list[dict]) -> None:
    rows = []
    for a in activity:
        loc = a.get("location") or {}
        rows.append({
            "activity_id":      a["activity_id"],
            "user_id":          a["user_id"],
            "event_id":         a.get("event_id"),
            "sport":            a["sport"],
            "duration_minutes": a.get("duration_minutes"),
            "location": {
                "location_id": loc.get("location_id"),
                "name":        loc.get("name"),
                "lat":         loc.get("lat"),
                "lng":         loc.get("lng"),
            } if loc else None,
            "activity_type": a["activity_type"],
            "timestamp":     a["timestamp"],
        })
    errors = client.insert_rows_json(_tbl("user_activity"), rows)
    if errors:
        raise RuntimeError(f"user_activity insert errors: {errors}")


def _bq_insert_recommendations(client: bigquery.Client, recs: list[dict]) -> None:
    errors = client.insert_rows_json(_tbl("user_recommendations"), recs)
    if errors:
        raise RuntimeError(f"user_recommendations insert errors: {errors}")


def seed_bigquery(data: dict) -> None:
    """
    Insert all generated mock data into BigQuery tables.

    Insertion order respects logical dependencies:
      users → locations → events → friendships → participants → activity → recs
    """
    bq = bigquery.Client()
    print("Seeding BigQuery…")

    print(f"  → {len(data['users'])} users")
    _bq_insert_users(bq, data["users"])

    print(f"  → {len(data['locations'])} locations")
    _bq_insert_locations(bq, data["locations"])

    print(f"  → {len(data['events'])} events")
    _bq_insert_events(bq, data["events"])

    print(f"  → {len(data['friendships'])} friendship rows")
    _bq_insert_friendships(bq, data["friendships"])

    print(f"  → {len(data['participants'])} participant rows")
    _bq_insert_participants(bq, data["participants"])

    print(f"  → {len(data['activity'])} activity rows")
    _bq_insert_activity(bq, data["activity"])

    print(f"  → {len(data['recommendations'])} recommendation rows")
    _bq_insert_recommendations(bq, data["recommendations"])

    print("Done. ✓")


# ---------------------------------------------------------------------------
# Convenience fixture accessors (for unit tests)
# ---------------------------------------------------------------------------

def make_user(**overrides) -> dict:
    """Return a single mock user dict, with optional field overrides."""
    base = generate_users(1)[0]
    base.update(overrides)
    return base


def make_event(**overrides) -> dict:
    """Return a single mock event dict, with optional field overrides."""
    base = generate_events(1)[0]
    base.update(overrides)
    return base


def make_location(**overrides) -> dict:
    """Return a single mock location dict, with optional field overrides."""
    base = generate_locations(1)[0]
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# CLI entrypoint
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Sports Connect mock data generator")
    parser.add_argument("--users",   type=int, default=20,   help="Number of users to generate")
    parser.add_argument("--events",  type=int, default=40,   help="Number of events to generate")
    parser.add_argument("--dry-run", action="store_true",    help="Print row counts without writing")
    parser.add_argument("--seed-bq", action="store_true",    help="Insert data into BigQuery")
    parser.add_argument("--export",  type=str, default=None, metavar="FILE",
                        help="Export generated data as JSON to FILE")
    args = parser.parse_args()

    data = generate_all(n_users=args.users, n_events=args.events)

    print("\n=== Mock Data Summary ===")
    for key, val in data.items():
        print(f"  {key:20s}: {len(val):>5} rows")

    if args.export:
        with open(args.export, "w") as f:
            json.dump(data, f, indent=2, default=str)
        print(f"\nExported to {args.export}")

    if args.seed_bq:
        seed_bigquery(data)

    if args.dry_run or (not args.seed_bq and not args.export):
        print("\n(Dry run — no data written. Use --seed-bq or --export to persist.)")


if __name__ == "__main__":
    main()