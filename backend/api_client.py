"""
api_client.py — HTTP client for the Sports Connect Flask backend
================================================================
Drop this file into the root of the Streamlit repo (next to app.py).

Usage in app.py / local_data.py:
    import api_client as api

    # Get nearby events (replaces local_data.get_nearby_events)
    events = api.get_nearby_events(lat=25.7617, lng=-80.1918, radius_m=5000)

    # Get recommendations
    recs = api.get_recommendations(user_id)

    # Send a message
    api.send_message(sender_id, receiver_id, "Hey, want to play?")

The base URL is read from the BACKEND_URL environment variable.
For local development, set it to http://localhost:8080.
For production (Cloud Run), set it to the Flask service URL.

In Streamlit Cloud Run, add this to the deployment env vars:
    BACKEND_URL=https://sports-connect-api-xxxx-uc.a.run.app
"""

import os
import requests
from typing import Any

# ── configuration ──────────────────────────────────────────────────────────

_BASE = os.environ.get("BACKEND_URL", "http://localhost:8080").rstrip("/")
_TIMEOUT = 10   # seconds; Vertex AI refresh can take longer → use refresh_recommendations directly


def _get(path: str, params: dict | None = None) -> Any:
    r = requests.get(f"{_BASE}{path}", params=params, timeout=_TIMEOUT)
    r.raise_for_status()
    return r.json()


def _post(path: str, body: dict | None = None) -> Any:
    r = requests.post(f"{_BASE}{path}", json=body or {}, timeout=_TIMEOUT)
    r.raise_for_status()
    return r.json()


# ── events ──────────────────────────────────────────────────────────────────

def get_nearby_events(
    lat: float,
    lng: float,
    radius_m: float = 5000,
    status: str = "open",
    sport: str | None = None,
) -> list[dict]:
    params = {"lat": lat, "lng": lng, "radius_m": radius_m, "status": status}
    if sport:
        params["sport"] = sport
    return _get("/api/events/nearby", params).get("events", [])


def get_event(event_id: str) -> dict | None:
    try:
        return _get(f"/api/events/{event_id}")
    except requests.HTTPError as e:
        if e.response.status_code == 404:
            return None
        raise


def join_event(user_id: str, event_id: str) -> dict:
    return _post(f"/api/events/{event_id}/join", {"user_id": user_id})


def leave_event(user_id: str, event_id: str) -> dict:
    return _post(f"/api/events/{event_id}/leave", {"user_id": user_id})


def get_event_participants(event_id: str) -> list[dict]:
    return _get(f"/api/events/{event_id}/participants").get("participants", [])


def create_event(payload: dict) -> dict:
    return _post("/api/events", payload)


# ── messages ────────────────────────────────────────────────────────────────

def get_conversations(user_id: str) -> list[dict]:
    return _get(f"/api/messages/{user_id}/conversations").get("conversations", [])


def get_thread(user_id: str, other_id: str) -> list[dict]:
    return _get(f"/api/messages/{user_id}/with/{other_id}").get("messages", [])


def send_message(sender_id: str, receiver_id: str, content: str) -> dict:
    return _post("/api/messages/send", {
        "sender_id":   sender_id,
        "receiver_id": receiver_id,
        "content":     content,
    })


def get_unread_count(user_id: str) -> int:
    return _get(f"/api/messages/{user_id}/unread-count").get("unread", 0)


# ── recommendations ─────────────────────────────────────────────────────────

def get_recommendations(user_id: str, limit: int = 10) -> list[dict]:
    return _get(f"/api/recommend/{user_id}", {"limit": limit}).get("recommendations", [])


def refresh_recommendations(user_id: str, radius_km: float = 20.0) -> list[dict]:
    """Triggers Vertex AI recompute — may take 5-15 seconds."""
    return _post(
        f"/api/recommend/{user_id}/refresh",
        {}
    ).get("recommendations", [])


def explain_recommendation(user_id: str, event_id: str) -> dict:
    return _get(f"/api/recommend/{user_id}/explain/{event_id}")


# ── users / social ──────────────────────────────────────────────────────────

def get_user(user_id: str) -> dict | None:
    try:
        return _get(f"/api/users/{user_id}")
    except requests.HTTPError as e:
        if e.response.status_code == 404:
            return None
        raise


def get_friends(user_id: str) -> list[dict]:
    return _get(f"/api/users/{user_id}/friends").get("friends", [])


def send_friend_request(user_id: str, friend_id: str) -> dict:
    return _post(f"/api/users/{user_id}/friends/request", {"friend_id": friend_id})


def accept_friend(user_id: str, friend_id: str) -> dict:
    return _post(f"/api/users/{user_id}/friends/accept", {"friend_id": friend_id})


def get_activity(user_id: str, limit: int = 20) -> list[dict]:
    return _get(f"/api/users/{user_id}/activity", {"limit": limit}).get("activity", [])


def log_activity(user_id: str, payload: dict) -> dict:
    return _post(f"/api/users/{user_id}/activity", payload)


def get_feed(user_id: str, limit: int = 20) -> list[dict]:
    return _get(f"/api/users/{user_id}/feed", {"limit": limit}).get("feed", [])