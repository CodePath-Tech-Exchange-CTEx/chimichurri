"""
api/users.py — User & social endpoints
========================================

GET  /api/users/<user_id>                    Profile
GET  /api/users/<user_id>/friends            Accepted friends list
POST /api/users/<user_id>/friends/request    Body: { friend_id }
POST /api/users/<user_id>/friends/accept     Body: { friend_id }
POST /api/users/<user_id>/friends/reject     Body: { friend_id }
GET  /api/users/<user_id>/activity           Recent activity (limit queryable)
POST /api/users/<user_id>/activity           Log an activity row
GET  /api/users/<user_id>/posts              User posts
POST /api/users/<user_id>/posts              Create a post
GET  /api/users/<user_id>/feed               Friends' recent activity + posts
"""

import uuid
from datetime import datetime, timezone
from flask import Blueprint, jsonify, request
from google.cloud import bigquery

from api.db import run_query, tbl

users_bp = Blueprint("users", __name__)


# ── profile ────────────────────────────────────────────────────────────────

@users_bp.get("/<user_id>")
def get_user(user_id: str):
    sql = f"""
        SELECT user_id, email, created_at, home_lat, home_lng, sports
        FROM {tbl("users")}
        WHERE user_id = @uid
        LIMIT 1
    """
    rows = run_query(sql, [bigquery.ScalarQueryParameter("uid", "STRING", user_id)])
    if not rows:
        return jsonify({"error": "user not found"}), 404
    r = rows[0]
    return jsonify({
        "user_id":    r["user_id"],
        "email":      r["email"],
        "created_at": r["created_at"].isoformat() if r.get("created_at") else None,
        "home_lat":   r["home_lat"],
        "home_lng":   r["home_lng"],
        "sports":     list(r["sports"]) if r.get("sports") else [],
    }), 200


# ── friends ────────────────────────────────────────────────────────────────

@users_bp.get("/<user_id>/friends")
def get_friends(user_id: str):
    sql = f"""
        SELECT f.friend_id, u.email, u.sports, f.created_at, f.updated_at
        FROM {tbl("friendship")} f
        JOIN {tbl("users")} u ON u.user_id = f.friend_id
        WHERE f.user_id = @uid AND f.status = 'accepted'
        ORDER BY f.updated_at DESC
    """
    rows = run_query(sql, [bigquery.ScalarQueryParameter("uid", "STRING", user_id)])
    return jsonify({
        "friends": [
            {
                "user_id":    r["friend_id"],
                "email":      r["email"],
                "sports":     list(r["sports"]) if r.get("sports") else [],
                "created_at": r["created_at"].isoformat() if r.get("created_at") else None,
            }
            for r in rows
        ]
    }), 200


@users_bp.post("/<user_id>/friends/request")
def send_friend_request(user_id: str):
    body      = request.get_json(silent=True) or {}
    friend_id = body.get("friend_id")
    if not friend_id:
        return jsonify({"error": "friend_id required"}), 400
    if friend_id == user_id:
        return jsonify({"error": "cannot friend yourself"}), 400

    # Check existing
    existing = run_query(f"""
        SELECT COUNT(*) AS cnt FROM {tbl("friendship")}
        WHERE (user_id=@uid AND friend_id=@fid)
           OR (user_id=@fid AND friend_id=@uid)
    """, [
        bigquery.ScalarQueryParameter("uid", "STRING", user_id),
        bigquery.ScalarQueryParameter("fid", "STRING", friend_id),
    ])
    if existing[0]["cnt"] > 0:
        return jsonify({"error": "relationship already exists"}), 409

    now = datetime.now(timezone.utc).isoformat()
    run_query(f"""
        INSERT INTO {tbl("friendship")}
            (user_id, friend_id, status, requested_by, created_at, updated_at)
        VALUES
            (@uid, @fid, 'pending', @uid, @now, @now),
            (@fid, @uid, 'pending', @uid, @now, @now)
    """, [
        bigquery.ScalarQueryParameter("uid", "STRING", user_id),
        bigquery.ScalarQueryParameter("fid", "STRING", friend_id),
        bigquery.ScalarQueryParameter("now", "TIMESTAMP", now),
    ])
    return jsonify({"status": "request_sent"}), 201


@users_bp.post("/<user_id>/friends/accept")
def accept_friend(user_id: str):
    body      = request.get_json(silent=True) or {}
    friend_id = body.get("friend_id")
    if not friend_id:
        return jsonify({"error": "friend_id required"}), 400

    now = datetime.now(timezone.utc).isoformat()
    run_query(f"""
        UPDATE {tbl("friendship")}
        SET status='accepted', updated_at=@now
        WHERE (user_id=@uid AND friend_id=@fid)
           OR (user_id=@fid AND friend_id=@uid)
    """, [
        bigquery.ScalarQueryParameter("uid", "STRING", user_id),
        bigquery.ScalarQueryParameter("fid", "STRING", friend_id),
        bigquery.ScalarQueryParameter("now", "TIMESTAMP", now),
    ])
    return jsonify({"status": "accepted"}), 200


@users_bp.post("/<user_id>/friends/reject")
def reject_friend(user_id: str):
    body      = request.get_json(silent=True) or {}
    friend_id = body.get("friend_id")
    if not friend_id:
        return jsonify({"error": "friend_id required"}), 400

    run_query(f"""
        DELETE FROM {tbl("friendship")}
        WHERE (user_id=@uid AND friend_id=@fid)
           OR (user_id=@fid AND friend_id=@uid)
    """, [
        bigquery.ScalarQueryParameter("uid", "STRING", user_id),
        bigquery.ScalarQueryParameter("fid", "STRING", friend_id),
    ])
    return jsonify({"status": "rejected"}), 200


# ── activity ───────────────────────────────────────────────────────────────

@users_bp.get("/<user_id>/activity")
def get_activity(user_id: str):
    limit         = int(request.args.get("limit", 20))
    activity_type = request.args.get("type")

    type_clause = "AND activity_type = @atype" if activity_type else ""
    params = [
        bigquery.ScalarQueryParameter("uid",   "STRING", user_id),
        bigquery.ScalarQueryParameter("limit", "INT64",  limit),
    ]
    if activity_type:
        params.append(bigquery.ScalarQueryParameter("atype", "STRING", activity_type))

    sql = f"""
        SELECT activity_id, user_id, event_id, sport,
               duration_minutes, location, activity_type, timestamp
        FROM {tbl("user_activity")}
        WHERE user_id = @uid {type_clause}
        ORDER BY timestamp DESC
        LIMIT @limit
    """
    rows = run_query(sql, params)
    return jsonify({
        "activity": [
            {**r, "timestamp": r["timestamp"].isoformat() if r.get("timestamp") else None}
            for r in rows
        ]
    }), 200


@users_bp.post("/<user_id>/activity")
def log_activity(user_id: str):
    body = request.get_json(silent=True) or {}
    activity_id = str(uuid.uuid4())
    loc = body.get("location") or {}

    if loc:
        sql = f"""
            INSERT INTO {tbl("user_activity")}
                (activity_id, user_id, event_id, sport, duration_minutes,
                 location, activity_type, timestamp)
            VALUES (
                @aid, @uid, @eid, @sport, @dur,
                STRUCT(@loc_id AS location_id, @loc_name AS name,
                       @loc_lat AS lat, @loc_lng AS lng,
                       ST_GEOGPOINT(@loc_lng, @loc_lat) AS geog),
                @atype, CURRENT_TIMESTAMP()
            )
        """
        params = [
            bigquery.ScalarQueryParameter("aid",      "STRING",  activity_id),
            bigquery.ScalarQueryParameter("uid",      "STRING",  user_id),
            bigquery.ScalarQueryParameter("eid",      "STRING",  body.get("event_id")),
            bigquery.ScalarQueryParameter("sport",    "STRING",  body.get("sport")),
            bigquery.ScalarQueryParameter("dur",      "INT64",   body.get("duration_minutes")),
            bigquery.ScalarQueryParameter("atype",    "STRING",  body.get("activity_type", "session_complete")),
            bigquery.ScalarQueryParameter("loc_id",   "STRING",  loc.get("location_id", str(uuid.uuid4()))),
            bigquery.ScalarQueryParameter("loc_name", "STRING",  loc.get("name", "")),
            bigquery.ScalarQueryParameter("loc_lat",  "FLOAT64", loc.get("lat", 0.0)),
            bigquery.ScalarQueryParameter("loc_lng",  "FLOAT64", loc.get("lng", 0.0)),
        ]
    else:
        sql = f"""
            INSERT INTO {tbl("user_activity")}
                (activity_id, user_id, event_id, sport,
                 duration_minutes, activity_type, timestamp)
            VALUES (@aid, @uid, @eid, @sport, @dur, @atype, CURRENT_TIMESTAMP())
        """
        params = [
            bigquery.ScalarQueryParameter("aid",   "STRING", activity_id),
            bigquery.ScalarQueryParameter("uid",   "STRING", user_id),
            bigquery.ScalarQueryParameter("eid",   "STRING", body.get("event_id")),
            bigquery.ScalarQueryParameter("sport", "STRING", body.get("sport")),
            bigquery.ScalarQueryParameter("dur",   "INT64",  body.get("duration_minutes")),
            bigquery.ScalarQueryParameter("atype", "STRING", body.get("activity_type", "session_complete")),
        ]
    run_query(sql, params)
    return jsonify({"activity_id": activity_id, "status": "logged"}), 201


# ── posts / feed ───────────────────────────────────────────────────────────

@users_bp.post("/<user_id>/posts")
def create_post(user_id: str):
    body    = request.get_json(silent=True) or {}
    content = body.get("content", "").strip()
    if not content:
        return jsonify({"error": "content required"}), 400

    post_id = str(uuid.uuid4())
    run_query(f"""
        INSERT INTO {tbl("posts")} (post_id, user_id, content, created_at)
        VALUES (@pid, @uid, @content, CURRENT_TIMESTAMP())
    """, [
        bigquery.ScalarQueryParameter("pid",     "STRING", post_id),
        bigquery.ScalarQueryParameter("uid",     "STRING", user_id),
        bigquery.ScalarQueryParameter("content", "STRING", content),
    ])
    return jsonify({"post_id": post_id, "status": "created"}), 201


@users_bp.get("/<user_id>/posts")
def get_posts(user_id: str):
    limit = int(request.args.get("limit", 10))
    rows  = run_query(f"""
        SELECT post_id, user_id, content, created_at
        FROM {tbl("posts")}
        WHERE user_id = @uid
        ORDER BY created_at DESC
        LIMIT @limit
    """, [
        bigquery.ScalarQueryParameter("uid",   "STRING", user_id),
        bigquery.ScalarQueryParameter("limit", "INT64",  limit),
    ])
    return jsonify({
        "posts": [
            {**r, "created_at": r["created_at"].isoformat() if r.get("created_at") else None}
            for r in rows
        ]
    }), 200


@users_bp.get("/<user_id>/feed")
def get_feed(user_id: str):
    """Friends' recent activity and posts combined, newest first."""
    limit = int(request.args.get("limit", 20))
    rows  = run_query(f"""
        SELECT
            a.activity_id AS id,
            a.user_id,
            u.email,
            a.sport,
            a.activity_type,
            a.timestamp   AS created_at,
            NULL          AS content,
            'activity'    AS feed_type
        FROM {tbl("user_activity")} a
        JOIN {tbl("friendship")} f ON f.friend_id = a.user_id
        JOIN {tbl("users")} u      ON u.user_id   = a.user_id
        WHERE f.user_id = @uid AND f.status = 'accepted'
        UNION ALL
        SELECT
            p.post_id  AS id,
            p.user_id,
            u.email,
            NULL       AS sport,
            NULL       AS activity_type,
            p.created_at,
            p.content,
            'post'     AS feed_type
        FROM {tbl("posts")} p
        JOIN {tbl("friendship")} f ON f.friend_id = p.user_id
        JOIN {tbl("users")} u      ON u.user_id   = p.user_id
        WHERE f.user_id = @uid AND f.status = 'accepted'
        ORDER BY created_at DESC
        LIMIT @limit
    """, [
        bigquery.ScalarQueryParameter("uid",   "STRING", user_id),
        bigquery.ScalarQueryParameter("limit", "INT64",  limit),
    ])
    return jsonify({
        "feed": [
            {**r, "created_at": r["created_at"].isoformat() if r.get("created_at") else None}
            for r in rows
        ]
    }), 200