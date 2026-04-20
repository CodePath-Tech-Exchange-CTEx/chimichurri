"""
api/recommend.py — Recommendation endpoints
============================================

GET  /api/recommend/<user_id>
     Serve cached recommendations from BigQuery (fast, < 50 ms).
     Query param: limit (default 10)

POST /api/recommend/<user_id>/refresh
     Trigger a full recompute: pulls events, scores them with the
     hybrid engine (Vertex AI + formula), writes results to BigQuery.
     Use this when: user profile changes, new events are added,
     or on a scheduled Cloud Scheduler job (e.g. nightly refresh).
     Returns the freshly computed scores immediately.

GET  /api/recommend/<user_id>/explain/<event_id>
     Human-readable explanation of WHY an event was recommended.
     Useful for the UI ("recommended because 3 friends joined" etc.)
"""

from flask import Blueprint, jsonify, request
from google.cloud import bigquery

from api.db import run_query, tbl
from recommender import Recommender, W_SEMANTIC, W_SPORT, W_SKILL, W_DISTANCE, W_SOCIAL, W_FRESH

recommend_bp = Blueprint("recommend", __name__)

# One engine instance per Flask worker process.
# The Vertex AI client inside it is lazily initialised on first use.
_engine = Recommender()


@recommend_bp.get("/<user_id>")
def get_recommendations(user_id: str):
    """
    Return cached recommendations.  Instant — no ML compute on this path.
    If the cache is empty (new user or never refreshed), automatically
    triggers a refresh so the user always gets something.
    """
    limit = int(request.args.get("limit", 10))

    cached = _engine.get_cached(user_id, limit=limit)

    if not cached:
        # First-time user or stale cache — compute synchronously.
        # For production you'd queue this as a background task,
        # but for MVP synchronous is fine.
        try:
            scored  = _engine.run_for_user(user_id)
            cached  = [
                {
                    "event_id":     s["event_id"],
                    "score":        s["score"],
                    "generated_at": s["generated_at"],
                    **s["_event"],
                }
                for s in scored[:limit]
            ]
        except Exception as e:
            return jsonify({
                "recommendations": [],
                "message": f"Could not compute recommendations: {str(e)}",
            }), 200

    return jsonify({
        "recommendations": cached,
        "count":           len(cached),
        "cached":          True,
    }), 200


@recommend_bp.post("/<user_id>/refresh")
def refresh_recommendations(user_id: str):
    """
    Recompute recommendations and refresh the BigQuery cache.
    Can be called from:
      - A Cloud Scheduler HTTP job (nightly refresh for all users)
      - The Streamlit app when the user opens the recommendations tab
      - After a user updates their sport preferences
    """
    radius_km = float(request.args.get("radius_km", 20.0))

    try:
        scored = _engine.run_for_user(user_id, radius_km=radius_km)
    except ValueError as e:
        return jsonify({"error": str(e)}), 404
    except Exception as e:
        return jsonify({"error": f"Recommendation engine error: {str(e)}"}), 500

    # Return top 10 with full event data attached
    results = [
        {
            "event_id":     s["event_id"],
            "score":        s["score"],
            "generated_at": s["generated_at"],
            **{k: v for k, v in s["_event"].items() if not k.startswith("_")},
        }
        for s in scored[:10]
    ]
    return jsonify({
        "recommendations": results,
        "count":           len(results),
        "total_scored":    len(scored),
        "cached":          False,
    }), 200


@recommend_bp.get("/<user_id>/explain/<event_id>")
def explain_recommendation(user_id: str, event_id: str):
    """
    Return a human-readable breakdown of the recommendation score
    for a specific (user, event) pair.
    Recomputes the sub-scores on demand — no extra BQ storage needed.
    """
    # Fetch user
    user_rows = run_query(f"""
        SELECT user_id, email, home_lat, home_lng, sports
        FROM {tbl("users")} WHERE user_id = @uid LIMIT 1
    """, [bigquery.ScalarQueryParameter("uid", "STRING", user_id)])
    if not user_rows:
        return jsonify({"error": "user not found"}), 404

    user = {**user_rows[0], "sports": list(user_rows[0].get("sports") or [])}

    # Fetch event
    event_rows = run_query(f"""
        SELECT event_id, sport, skill_level, location,
               start_time, end_time, max_players
        FROM {tbl("events")} WHERE event_id = @eid LIMIT 1
    """, [bigquery.ScalarQueryParameter("eid", "STRING", event_id)])
    if not event_rows:
        return jsonify({"error": "event not found"}), 404

    er  = event_rows[0]
    loc = er.get("location") or {}
    event = {
        "event_id":    er["event_id"],
        "sport":       er["sport"],
        "skill_level": er.get("skill_level", "intermediate"),
        "location":    dict(loc),
        "start_time":  er["start_time"].isoformat() if er.get("start_time") else "",
        "_participant_ids": [],
    }

    # Participant list for social score
    p_rows = run_query(f"""
        WITH latest AS (
            SELECT user_id, status,
                   ROW_NUMBER() OVER (PARTITION BY event_id, user_id ORDER BY joined_at DESC) rn
            FROM {tbl("event_participants")} WHERE event_id = @eid
        )
        SELECT user_id FROM latest WHERE rn=1 AND status='joined'
    """, [bigquery.ScalarQueryParameter("eid", "STRING", event_id)])
    event["_participant_ids"] = [r["user_id"] for r in p_rows]

    friend_ids = _engine._fetch_friend_ids(user_id)

    sport_m  = _engine._sport_match(user, event)
    skill_m  = _engine._skill_match(user, event)
    dist_s   = _engine._distance_score(user, event)
    social_s = _engine._social_score(event, friend_ids)
    fresh_s  = _engine._freshness(event)

    # Friendly labels for the UI
    def label(v: float) -> str:
        if v >= 0.8: return "great"
        if v >= 0.5: return "good"
        if v >= 0.2: return "fair"
        return "low"

    friend_overlap = len(friend_ids & set(event["_participant_ids"]))

    explanation = {
        "event_id": event_id,
        "user_id":  user_id,
        "factors": {
            "sport_match": {
                "score": round(sport_m, 3),
                "label": label(sport_m),
                "reason": (
                    f"You play {event['sport']}"
                    if sport_m == 1.0
                    else f"You don't play {event['sport']}"
                ),
            },
            "skill_match": {
                "score": round(skill_m, 3),
                "label": label(skill_m),
                "reason": f"Event is {event['skill_level']} level",
            },
            "distance": {
                "score": round(dist_s, 3),
                "label": label(dist_s),
                "reason": "Based on distance from your home location",
            },
            "social": {
                "score": round(social_s, 3),
                "label": label(social_s),
                "reason": (
                    f"{friend_overlap} of your friends joined this event"
                    if friend_overlap > 0
                    else "None of your friends have joined yet"
                ),
            },
            "freshness": {
                "score": round(fresh_s, 3),
                "label": label(fresh_s),
                "reason": f"Event starts {event.get('start_time', 'soon')}",
            },
        },
        "weights": {
            "semantic":  W_SEMANTIC,
            "sport":     W_SPORT,
            "skill":     W_SKILL,
            "distance":  W_DISTANCE,
            "social":    W_SOCIAL,
            "freshness": W_FRESH,
        },
        "note": "Semantic (Vertex AI embedding) score requires a refresh call to compute.",
    }

    return jsonify(explanation), 200