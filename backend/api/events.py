"""
api/events.py — Events endpoints
==================================

GET  /api/events/nearby          Query params: lat, lng, radius_m (default 5000), status
GET  /api/events/<event_id>      Full event detail including live participant count
GET  /api/events/sport/<sport>   All open public events for a sport
POST /api/events                 Create a new event
POST /api/events/<event_id>/join     Body: { user_id }
POST /api/events/<event_id>/leave    Body: { user_id }
GET  /api/events/<event_id>/participants
"""

import uuid
from datetime import datetime, timezone
from flask import Blueprint, jsonify, request
from google.cloud import bigquery

from api.db import run_query, tbl

events_bp = Blueprint("events", __name__)


# ── helpers ────────────────────────────────────────────────────────────────

def _serialize_row(row: dict) -> dict:
    """Convert BigQuery datetime/date objects to ISO strings for JSON."""
    out = {}
    for k, v in row.items():
        if isinstance(v, datetime):
            out[k] = v.isoformat()
        elif isinstance(v, dict):
            out[k] = _serialize_row(v)
        else:
            out[k] = v
    return out


def _active_participant_count(event_id: str) -> int:
    """Latest-status CTE: count users whose last action was 'joined'."""
    sql = f"""
        WITH latest AS (
            SELECT user_id,
                   status,
                   ROW_NUMBER() OVER (
                       PARTITION BY event_id, user_id
                       ORDER BY joined_at DESC
                   ) AS rn
            FROM {tbl("event_participants")}
            WHERE event_id = @event_id
        )
        SELECT COUNT(*) AS cnt
        FROM latest
        WHERE rn = 1 AND status = 'joined'
    """
    rows = run_query(sql, [bigquery.ScalarQueryParameter("event_id", "STRING", event_id)])
    return rows[0]["cnt"] if rows else 0


# ── routes ─────────────────────────────────────────────────────────────────

@events_bp.get("/nearby")
def nearby_events():
    """
    Return public open events within radius_m metres of (lat, lng).
    Also returns the live participant count and spots_left for each event,
    making it easy for the map and event-list UI to render without extra calls.

    Query params:
        lat        float   required
        lng        float   required
        radius_m   float   default 5000
        status     str     default 'open'
        sport      str     optional filter
    """
    try:
        lat      = float(request.args["lat"])
        lng      = float(request.args["lng"])
    except (KeyError, ValueError):
        return jsonify({"error": "lat and lng are required and must be numbers"}), 400

    radius_m = float(request.args.get("radius_m", 5000))
    status   = request.args.get("status", "open")
    sport    = request.args.get("sport")

    sport_filter = "AND LOWER(e.sport) = LOWER(@sport)" if sport else ""

    sql = f"""
        SELECT
            e.event_id,
            e.sport,
            e.location,
            e.created_by,
            e.start_time,
            e.end_time,
            e.max_players,
            e.status,
            e.visibility,
            e.skill_level,
            ST_DISTANCE(
                e.location.geog,
                ST_GEOGPOINT(@lng, @lat)
            ) AS distance_meters
        FROM {tbl("events")} e
        WHERE e.visibility  = 'public'
          AND e.status      = @status
          AND e.start_time >= CURRENT_TIMESTAMP()
          AND ST_DISTANCE(
                e.location.geog,
                ST_GEOGPOINT(@lng, @lat)
              ) <= @radius_m
          {sport_filter}
        ORDER BY distance_meters ASC
        LIMIT 50
    """
    params = [
        bigquery.ScalarQueryParameter("lat",      "FLOAT64", lat),
        bigquery.ScalarQueryParameter("lng",      "FLOAT64", lng),
        bigquery.ScalarQueryParameter("radius_m", "FLOAT64", radius_m),
        bigquery.ScalarQueryParameter("status",   "STRING",  status),
    ]
    if sport:
        params.append(bigquery.ScalarQueryParameter("sport", "STRING", sport))

    rows = run_query(sql, params)

    events = []
    for row in rows:
        r = _serialize_row(row)
        count = _active_participant_count(r["event_id"])
        r["participant_count"] = count
        r["spots_left"]        = max(0, (r.get("max_players") or 0) - count)
        events.append(r)

    return jsonify({"events": events, "count": len(events)}), 200


@events_bp.get("/<event_id>")
def get_event(event_id: str):
    sql = f"""
        SELECT
            event_id, sport, location, created_by,
            start_time, end_time, max_players,
            visibility, status, skill_level,
            created_at, updated_at
        FROM {tbl("events")}
        WHERE event_id = @event_id
        LIMIT 1
    """
    rows = run_query(sql, [bigquery.ScalarQueryParameter("event_id", "STRING", event_id)])
    if not rows:
        return jsonify({"error": "event not found"}), 404

    r     = _serialize_row(rows[0])
    count = _active_participant_count(event_id)
    r["participant_count"] = count
    r["spots_left"]        = max(0, (r.get("max_players") or 0) - count)
    return jsonify(r), 200


@events_bp.get("/sport/<sport>")
def events_by_sport(sport: str):
    sql = f"""
        SELECT
            event_id, sport, location, created_by,
            start_time, end_time, max_players,
            visibility, status, skill_level
        FROM {tbl("events")}
        WHERE LOWER(sport) = LOWER(@sport)
          AND status       = 'open'
          AND visibility   = 'public'
          AND start_time  >= CURRENT_TIMESTAMP()
        ORDER BY start_time ASC
        LIMIT 50
    """
    rows = run_query(sql, [bigquery.ScalarQueryParameter("sport", "STRING", sport)])
    return jsonify({"events": [_serialize_row(r) for r in rows]}), 200


@events_bp.post("")
def create_event():
    """
    Body (JSON):
        sport, created_by, start_time (ISO), end_time (ISO),
        max_players, skill_level, visibility,
        location: { location_id, name, address, lat, lng }
    """
    body = request.get_json(silent=True) or {}

    required = ["sport", "created_by", "start_time", "end_time", "location"]
    missing  = [f for f in required if f not in body]
    if missing:
        return jsonify({"error": f"missing fields: {missing}"}), 400

    loc      = body["location"]
    event_id = str(uuid.uuid4())
    now      = datetime.now(timezone.utc).isoformat()

    sql = f"""
        INSERT INTO {tbl("events")}
            (event_id, sport, location, created_by,
             start_time, end_time, max_players, skill_level,
             visibility, status, created_at, updated_at)
        VALUES (
            @event_id, @sport,
            STRUCT(
                @loc_id   AS location_id,
                @loc_name AS name,
                @loc_addr AS address,
                @loc_lat  AS lat,
                @loc_lng  AS lng,
                ST_GEOGPOINT(@loc_lng, @loc_lat) AS geog
            ),
            @created_by,
            TIMESTAMP(@start_time), TIMESTAMP(@end_time),
            @max_players, @skill_level, @visibility,
            'open', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
        )
    """
    params = [
        bigquery.ScalarQueryParameter("event_id",   "STRING",  event_id),
        bigquery.ScalarQueryParameter("sport",      "STRING",  body["sport"]),
        bigquery.ScalarQueryParameter("created_by", "STRING",  body["created_by"]),
        bigquery.ScalarQueryParameter("start_time", "STRING",  body["start_time"]),
        bigquery.ScalarQueryParameter("end_time",   "STRING",  body["end_time"]),
        bigquery.ScalarQueryParameter("max_players","INT64",   body.get("max_players", 10)),
        bigquery.ScalarQueryParameter("skill_level","STRING",  body.get("skill_level", "intermediate")),
        bigquery.ScalarQueryParameter("visibility", "STRING",  body.get("visibility", "public")),
        bigquery.ScalarQueryParameter("loc_id",     "STRING",  loc.get("location_id", str(uuid.uuid4()))),
        bigquery.ScalarQueryParameter("loc_name",   "STRING",  loc.get("name", "")),
        bigquery.ScalarQueryParameter("loc_addr",   "STRING",  loc.get("address", "")),
        bigquery.ScalarQueryParameter("loc_lat",    "FLOAT64", loc["lat"]),
        bigquery.ScalarQueryParameter("loc_lng",    "FLOAT64", loc["lng"]),
    ]
    run_query(sql, params)
    return jsonify({"event_id": event_id, "status": "created"}), 201


@events_bp.post("/<event_id>/join")
def join_event(event_id: str):
    body    = request.get_json(silent=True) or {}
    user_id = body.get("user_id")
    if not user_id:
        return jsonify({"error": "user_id required"}), 400

    # Check event exists and is open
    event_rows = run_query(
        f"SELECT status, max_players FROM {tbl('events')} WHERE event_id = @eid LIMIT 1",
        [bigquery.ScalarQueryParameter("eid", "STRING", event_id)],
    )
    if not event_rows:
        return jsonify({"error": "event not found"}), 404
    event = event_rows[0]
    if event["status"] != "open":
        return jsonify({"error": f"event is not open (status={event['status']})"}), 409

    # Check already joined
    already = run_query(f"""
        WITH latest AS (
            SELECT user_id, status,
                   ROW_NUMBER() OVER (PARTITION BY event_id, user_id ORDER BY joined_at DESC) rn
            FROM {tbl("event_participants")} WHERE event_id = @eid
        )
        SELECT COUNT(*) AS cnt FROM latest
        WHERE rn=1 AND status='joined' AND user_id=@uid
    """, [
        bigquery.ScalarQueryParameter("eid", "STRING", event_id),
        bigquery.ScalarQueryParameter("uid", "STRING", user_id),
    ])
    if already[0]["cnt"] > 0:
        return jsonify({"error": "already joined"}), 409

    # Check capacity
    count = _active_participant_count(event_id)
    if event.get("max_players") and count >= event["max_players"]:
        return jsonify({"error": "event is at capacity"}), 409

    run_query(f"""
        INSERT INTO {tbl("event_participants")} (event_id, user_id, joined_at, status)
        VALUES (@eid, @uid, CURRENT_TIMESTAMP(), 'joined')
    """, [
        bigquery.ScalarQueryParameter("eid", "STRING", event_id),
        bigquery.ScalarQueryParameter("uid", "STRING", user_id),
    ])
    return jsonify({"status": "joined"}), 200


@events_bp.post("/<event_id>/leave")
def leave_event(event_id: str):
    body    = request.get_json(silent=True) or {}
    user_id = body.get("user_id")
    if not user_id:
        return jsonify({"error": "user_id required"}), 400

    run_query(f"""
        INSERT INTO {tbl("event_participants")} (event_id, user_id, joined_at, status)
        VALUES (@eid, @uid, CURRENT_TIMESTAMP(), 'left')
    """, [
        bigquery.ScalarQueryParameter("eid", "STRING", event_id),
        bigquery.ScalarQueryParameter("uid", "STRING", user_id),
    ])
    return jsonify({"status": "left"}), 200


@events_bp.get("/<event_id>/participants")
def get_participants(event_id: str):
    sql = f"""
        WITH latest AS (
            SELECT user_id, status, joined_at,
                   ROW_NUMBER() OVER (
                       PARTITION BY event_id, user_id
                       ORDER BY joined_at DESC
                   ) AS rn
            FROM {tbl("event_participants")}
            WHERE event_id = @eid
        )
        SELECT l.user_id, u.email, l.joined_at, l.status
        FROM latest l
        JOIN {tbl("users")} u ON u.user_id = l.user_id
        WHERE l.rn = 1 AND l.status = 'joined'
        ORDER BY l.joined_at ASC
    """
    rows = run_query(sql, [bigquery.ScalarQueryParameter("eid", "STRING", event_id)])
    return jsonify({"participants": [_serialize_row(r) for r in rows]}), 200