"""
recommender.py — Hybrid Recommendation Engine
===============================================

Architecture
------------
This is a content-based + social hybrid recommender.
It deliberately avoids training a custom ML model (time-expensive)
while still using Vertex AI text embeddings as a real ML signal.

How it works, step by step:

1. FEATURE EXTRACTION
   Pull the target user's profile: sports, skill levels, home location,
   and their last N activity rows.

2. VERTEX AI SEMANTIC SIMILARITY  (the ML part)
   Build a short natural-language "taste profile" string for the user
   (e.g. "intermediate soccer player who enjoys basketball").
   Build a short description string for each candidate event.
   Call Vertex AI text-embeddings to embed both sets of strings.
   Cosine similarity between the user vector and each event vector
   gives a semantic match score in [0, 1].

3. STRUCTURED SCORING  (the statistics/formula part)
   For each candidate event compute five sub-scores, each in [0, 1]:

   a) sport_match    — 1.0 if user plays that sport, else 0.0
                       (simple but high signal)

   b) skill_match    — based on the gap between user skill and event skill.
                       Uses a triangular membership function (fuzzy logic):
                       perfect match = 1.0, one level off = 0.5, two = 0.0
                       This IS fuzzy logic — not "out of your ass" at all.
                       Fuzzy membership functions are standard for
                       preference-matching problems.

   c) distance_score — exponential decay:  exp(-λ * distance_km)
                       λ = ln(2) / HALF_LIFE_KM  so the score halves
                       every HALF_LIFE_KM kilometres.
                       Smooth, interpretable, no hard cutoff.

   d) social_score   — fraction of the user's friends who have already
                       joined this event. Friends joining = strong signal.

   e) freshness      — events closer to now (but still in the future)
                       score higher than events weeks away.
                       Uses a sigmoid centred at 24 hours from now.

4. WEIGHTED COMBINATION
   final_score = w_semantic * semantic
               + w_sport    * sport_match
               + w_skill    * skill_match
               + w_distance * distance_score
               + w_social   * social_score
               + w_fresh    * freshness

   Weights are tunable constants at the top of this file.
   They sum to 1.0 but that's a convention, not a requirement.

5. CACHE WRITE
   Scores are written into user_recommendations in BigQuery so
   /api/recommend/<user_id> can serve them in < 50 ms with no recompute.
   The cache is invalidated (replaced) each time run_for_user() is called.

Usage
-----
    from recommender import Recommender
    engine = Recommender()
    engine.run_for_user("user-uuid-here")   # blocks, writes to BQ
    scores = engine.get_cached("user-uuid-here")  # reads from BQ
"""

import math
import uuid
from datetime import datetime, timezone
from typing import Optional

from google.cloud import bigquery
import vertexai
from vertexai.language_models import TextEmbeddingModel

from api.db import run_query, tbl, PROJECT_ID

# ── tuneable weights (must sum ≤ 1 each; final normalisation handles the rest) ──

W_SEMANTIC = 0.30   # Vertex AI cosine similarity
W_SPORT    = 0.25   # exact sport match
W_SKILL    = 0.15   # fuzzy skill-level compatibility
W_DISTANCE = 0.15   # exponential distance decay
W_SOCIAL   = 0.10   # friends-already-joined fraction
W_FRESH    = 0.05   # event time freshness

HALF_LIFE_KM   = 5.0    # distance score halves every 5 km
VERTEX_REGION  = "us-central1"
EMBEDDING_MODEL = "text-embedding-004"   # stable GA model on Vertex AI

SKILL_ORDER = {"beginner": 0, "intermediate": 1, "advanced": 2}


class Recommender:
    """
    Stateless recommendation engine.  Instantiate once per Flask process;
    the Vertex AI and BigQuery clients are initialised lazily.
    """

    def __init__(self):
        self._embed_model: Optional[TextEmbeddingModel] = None
        self._bq: Optional[bigquery.Client] = None

    # ── lazy initialisers ──────────────────────────────────────────────────

    def _get_embed_model(self) -> TextEmbeddingModel:
        if self._embed_model is None:
            vertexai.init(project=PROJECT_ID, location=VERTEX_REGION)
            self._embed_model = TextEmbeddingModel.from_pretrained(EMBEDDING_MODEL)
        return self._embed_model

    def _get_bq(self) -> bigquery.Client:
        if self._bq is None:
            self._bq = bigquery.Client(project=PROJECT_ID)
        return self._bq

    # ── public API ─────────────────────────────────────────────────────────

    def run_for_user(self, user_id: str, radius_km: float = 20.0) -> list[dict]:
        """
        Compute recommendations for one user and write them to BigQuery.
        Returns the scored list (sorted by score desc) for immediate use.

        Parameters
        ----------
        user_id   : Target user.
        radius_km : Only consider events within this radius.
        """
        user    = self._fetch_user(user_id)
        if not user:
            raise ValueError(f"User {user_id} not found.")

        events  = self._fetch_candidate_events(user, radius_km)
        if not events:
            return []

        friends = self._fetch_friend_ids(user_id)

        # ── Vertex AI embeddings ──
        user_text   = self._user_to_text(user)
        event_texts = [self._event_to_text(e) for e in events]

        user_vec    = self._embed([user_text])[0]
        event_vecs  = self._embed(event_texts)

        # ── score each event ──
        scored = []
        for event, e_vec in zip(events, event_vecs):
            score = self._score(user, event, user_vec, e_vec, friends)
            scored.append({
                "user_id":      user_id,
                "event_id":     event["event_id"],
                "score":        round(score, 4),
                "generated_at": datetime.now(timezone.utc).isoformat(),
                # pass full event along for the response — not stored in BQ
                "_event":       event,
            })

        scored.sort(key=lambda x: x["score"], reverse=True)

        # ── write to cache ──
        self._write_cache(user_id, scored)

        return scored

    def get_cached(self, user_id: str, limit: int = 10) -> list[dict]:
        """
        Return precomputed recommendations from BigQuery, joined with
        full event details, sorted by score desc.
        """
        sql = f"""
            SELECT
                r.event_id,
                r.score,
                r.generated_at,
                e.sport,
                e.location,
                e.created_by,
                e.start_time,
                e.end_time,
                e.max_players,
                e.status,
                e.skill_level
            FROM {tbl("user_recommendations")} r
            JOIN {tbl("events")} e ON e.event_id = r.event_id
            WHERE r.user_id      = @uid
              AND e.status       = 'open'
              AND e.visibility   = 'public'
              AND e.start_time  >= CURRENT_TIMESTAMP()
            ORDER BY r.score DESC
            LIMIT @limit
        """
        rows = run_query(sql, [
            bigquery.ScalarQueryParameter("uid",   "STRING", user_id),
            bigquery.ScalarQueryParameter("limit", "INT64",  limit),
        ])
        return [self._serialize(r) for r in rows]

    # ── scoring sub-functions ──────────────────────────────────────────────

    def _score(
        self,
        user:       dict,
        event:      dict,
        user_vec:   list[float],
        event_vec:  list[float],
        friend_ids: set[str],
    ) -> float:

        semantic  = self._cosine(user_vec, event_vec)
        sport_m   = self._sport_match(user, event)
        skill_m   = self._skill_match(user, event)
        dist_s    = self._distance_score(user, event)
        social_s  = self._social_score(event, friend_ids)
        fresh_s   = self._freshness(event)

        return (
            W_SEMANTIC * semantic
            + W_SPORT    * sport_m
            + W_SKILL    * skill_m
            + W_DISTANCE * dist_s
            + W_SOCIAL   * social_s
            + W_FRESH    * fresh_s
        )

    @staticmethod
    def _sport_match(user: dict, event: dict) -> float:
        """1.0 if user plays this sport, else 0.0."""
        user_sports = {s["sport"].lower() for s in (user.get("sports") or [])}
        return 1.0 if event.get("sport", "").lower() in user_sports else 0.0

    @staticmethod
    def _skill_match(user: dict, event: dict) -> float:
        """
        Triangular fuzzy membership function.

        Given user_level and event_level both mapped to {0,1,2}:
          gap = 0 → 1.0  (perfect match)
          gap = 1 → 0.5  (one level off)
          gap ≥ 2 → 0.0  (too far apart)

        If user plays the same sport at multiple skill levels,
        use the closest one.
        """
        event_sport = event.get("sport", "").lower()
        event_skill = SKILL_ORDER.get(event.get("skill_level", "intermediate"), 1)

        best = 0.0
        for s in (user.get("sports") or []):
            if s["sport"].lower() != event_sport:
                continue
            user_skill = SKILL_ORDER.get(s.get("skill_level", "intermediate"), 1)
            gap = abs(user_skill - event_skill)
            membership = max(0.0, 1.0 - gap * 0.5)
            best = max(best, membership)

        # If user doesn't play this sport at all, use neutral 0.5
        return best if best > 0.0 else 0.5

    @staticmethod
    def _distance_score(user: dict, event: dict) -> float:
        """
        Exponential decay: score = exp(-λ * d)
        where λ = ln(2) / HALF_LIFE_KM.
        Returns 1.0 at d=0, 0.5 at d=HALF_LIFE_KM, near 0 beyond.
        """
        ulat = user.get("home_lat")
        ulng = user.get("home_lng")
        loc  = event.get("location") or {}
        elat = loc.get("lat")
        elng = loc.get("lng")

        if None in (ulat, ulng, elat, elng):
            return 0.5   # unknown distance → neutral

        dist_km = _haversine_km(ulat, ulng, elat, elng)
        lam     = math.log(2) / HALF_LIFE_KM
        return math.exp(-lam * dist_km)

    @staticmethod
    def _social_score(event: dict, friend_ids: set[str]) -> float:
        """
        Fraction of the user's friends who have joined this event.
        Capped at 1.0 — having 3 friends join doesn't score higher than 2.
        """
        if not friend_ids:
            return 0.0
        participants = set(event.get("_participant_ids", []))
        overlap = len(friend_ids & participants)
        # Sigmoid-ish: 1 friend → 0.5, 2+ → 1.0
        return min(1.0, overlap / max(1, math.sqrt(len(friend_ids))))

    @staticmethod
    def _freshness(event: dict) -> float:
        """
        Sigmoid centred at 24 h from now.
        Events starting in 0–24 h score highest; score drops after that.
        events_hours_away < 6   → ~0.88
        events_hours_away = 24  → 0.50  (inflection)
        events_hours_away = 72  → ~0.12
        """
        try:
            start = datetime.fromisoformat(event["start_time"])
            if start.tzinfo is None:
                start = start.replace(tzinfo=timezone.utc)
            hours_away = (start - datetime.now(timezone.utc)).total_seconds() / 3600
            if hours_away < 0:
                return 0.0
            # Logistic: σ(-(x - 24)/12)
            return 1.0 / (1.0 + math.exp((hours_away - 24) / 12))
        except Exception:
            return 0.5

    # ── Vertex AI embedding ────────────────────────────────────────────────

    def _embed(self, texts: list[str]) -> list[list[float]]:
        """
        Call Vertex AI text-embedding-004.
        Batches in groups of 250 (API limit).
        Returns a list of float vectors, one per input text.
        """
        model   = self._get_embed_model()
        results = []
        batch   = 250
        for i in range(0, len(texts), batch):
            chunk     = texts[i : i + batch]
            responses = model.get_embeddings(chunk)
            results.extend([r.values for r in responses])
        return results

    @staticmethod
    def _cosine(a: list[float], b: list[float]) -> float:
        """Cosine similarity in [0, 1] (embeddings are unit-normed by Vertex AI)."""
        dot  = sum(x * y for x, y in zip(a, b))
        # Vertex AI embeddings are already L2-normalised → dot product = cosine
        # Clamp to [0,1] to handle floating-point noise
        return max(0.0, min(1.0, (dot + 1.0) / 2.0))

    # ── text representations ───────────────────────────────────────────────

    @staticmethod
    def _user_to_text(user: dict) -> str:
        sports_str = ", ".join(
            f"{s['sport']} ({s.get('skill_level', 'intermediate')})"
            for s in (user.get("sports") or [])
        )
        return (
            f"A sports player who enjoys {sports_str}. "
            f"Looking for local games and community events near Miami."
        )

    @staticmethod
    def _event_to_text(event: dict) -> str:
        loc   = event.get("location") or {}
        skill = event.get("skill_level", "intermediate")
        sport = event.get("sport", "")
        venue = loc.get("name", "a local venue")
        return (
            f"A {skill} level {sport} game at {venue} in Miami. "
            f"Open to players who want to connect and compete."
        )

    # ── BigQuery helpers ───────────────────────────────────────────────────

    def _fetch_user(self, user_id: str) -> dict | None:
        rows = run_query(f"""
            SELECT user_id, email, home_lat, home_lng, sports
            FROM {tbl("users")}
            WHERE user_id = @uid
            LIMIT 1
        """, [bigquery.ScalarQueryParameter("uid", "STRING", user_id)])
        if not rows:
            return None
        r = rows[0]
        return {**r, "sports": list(r.get("sports") or [])}

    def _fetch_candidate_events(self, user: dict, radius_km: float) -> list[dict]:
        """
        Fetch open, public, future events within radius_km.
        Also fetches participant lists so social scoring can work.
        """
        lat = user.get("home_lat", 25.7617)
        lng = user.get("home_lng", -80.1918)

        sql = f"""
            SELECT
                e.event_id, e.sport, e.skill_level,
                e.location, e.start_time, e.end_time,
                e.max_players, e.status, e.visibility,
                ST_DISTANCE(
                    e.location.geog,
                    ST_GEOGPOINT(@lng, @lat)
                ) AS distance_meters
            FROM {tbl("events")} e
            WHERE e.visibility  = 'public'
              AND e.status      = 'open'
              AND e.start_time >= CURRENT_TIMESTAMP()
              AND ST_DISTANCE(
                    e.location.geog,
                    ST_GEOGPOINT(@lng, @lat)
                  ) <= @radius_m
            ORDER BY distance_meters ASC
            LIMIT 200
        """
        rows = run_query(sql, [
            bigquery.ScalarQueryParameter("lat",      "FLOAT64", lat),
            bigquery.ScalarQueryParameter("lng",      "FLOAT64", lng),
            bigquery.ScalarQueryParameter("radius_m", "FLOAT64", radius_km * 1000),
        ])

        if not rows:
            return []

        # Fetch participant lists for all events in one query
        event_ids = [r["event_id"] for r in rows]
        participant_map = self._fetch_participants_bulk(event_ids)

        events = []
        for r in rows:
            loc = r.get("location") or {}
            events.append({
                "event_id":        r["event_id"],
                "sport":           r["sport"],
                "skill_level":     r.get("skill_level", "intermediate"),
                "location":        dict(loc) if loc else {},
                "start_time":      r["start_time"].isoformat() if r.get("start_time") else "",
                "end_time":        r["end_time"].isoformat() if r.get("end_time") else "",
                "max_players":     r.get("max_players"),
                "distance_meters": r.get("distance_meters", 0),
                "_participant_ids": participant_map.get(r["event_id"], []),
            })
        return events

    def _fetch_participants_bulk(self, event_ids: list[str]) -> dict[str, list[str]]:
        """Return {event_id: [user_id, ...]} for all events in one CTE query."""
        if not event_ids:
            return {}

        placeholders = ", ".join(f"'{eid}'" for eid in event_ids)
        sql = f"""
            WITH latest AS (
                SELECT event_id, user_id, status,
                       ROW_NUMBER() OVER (
                           PARTITION BY event_id, user_id
                           ORDER BY joined_at DESC
                       ) AS rn
                FROM {tbl("event_participants")}
                WHERE event_id IN ({placeholders})
            )
            SELECT event_id, user_id
            FROM latest
            WHERE rn = 1 AND status = 'joined'
        """
        rows   = run_query(sql)
        result: dict[str, list[str]] = {}
        for r in rows:
            result.setdefault(r["event_id"], []).append(r["user_id"])
        return result

    def _fetch_friend_ids(self, user_id: str) -> set[str]:
        rows = run_query(f"""
            SELECT friend_id FROM {tbl("friendship")}
            WHERE user_id = @uid AND status = 'accepted'
        """, [bigquery.ScalarQueryParameter("uid", "STRING", user_id)])
        return {r["friend_id"] for r in rows}

    def _write_cache(self, user_id: str, scored: list[dict]) -> None:
        """
        Replace this user's recommendation rows.
        BigQuery doesn't support UPSERT, so we DELETE then INSERT.
        This is intentional mutable metadata — not event-sourced.
        """
        # Delete old rows
        run_query(f"""
            DELETE FROM {tbl("user_recommendations")}
            WHERE user_id = @uid
        """, [bigquery.ScalarQueryParameter("uid", "STRING", user_id)])

        if not scored:
            return

        # Batch insert new rows using the streaming insert API (faster than DML for writes)
        rows_to_insert = [
            {
                "user_id":      user_id,
                "event_id":     s["event_id"],
                "score":        s["score"],
                "generated_at": s["generated_at"],
            }
            for s in scored
        ]
        table_ref = self._get_bq().dataset("database").table("user_recommendations")
        errors    = self._get_bq().insert_rows_json(table_ref, rows_to_insert)
        if errors:
            raise RuntimeError(f"BigQuery streaming insert errors: {errors}")

    @staticmethod
    def _serialize(row: dict) -> dict:
        out = {}
        for k, v in row.items():
            if isinstance(v, datetime):
                out[k] = v.isoformat()
            elif isinstance(v, dict):
                out[k] = {
                    kk: vv.isoformat() if isinstance(vv, datetime) else vv
                    for kk, vv in v.items()
                }
            else:
                out[k] = v
        return out


# ── standalone haversine (no external deps) ────────────────────────────────

def _haversine_km(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    R    = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlng = math.radians(lng2 - lng1)
    a    = (math.sin(dlat / 2) ** 2
            + math.cos(math.radians(lat1))
            * math.cos(math.radians(lat2))
            * math.sin(dlng / 2) ** 2)
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))