"""
data_fetcher.py

Data access layer for the Sports Connect application.
All writes that target append-only tables (event_participants, user_activity)
follow BigQuery best-practice: never DELETE/UPDATE; INSERT a new status row.
The friendship table uses a two-row bidirectional model (one row per direction).
"""

from google.cloud import bigquery
from anthropic import Anthropic
import uuid
from datetime import datetime, timezone

# ---------------------------------------------------------------------------
# Client & constants
# ---------------------------------------------------------------------------

client = None

def _get_client():
    global client
    if client is None:
        client = bigquery.Client()
    return client

anthropic_client = None

def _get_anthropic_client():
    global anthropic_client
    if anthropic_client is None:
        anthropic_client = Anthropic()
    return anthropic_client

PROJECT_ID = "carlos-negron-uprm"
DATASET    = "database"
T          = f"`{PROJECT_ID}.{DATASET}"   # table prefix helper


def _tbl(name: str) -> str:
    """Return a fully-qualified, back-tick-quoted BigQuery table path."""
    return f"`{PROJECT_ID}.{DATASET}.{name}`"


# ---------------------------------------------------------------------------
# Core query runner
# ---------------------------------------------------------------------------

def run_query(query: str, params: list | None = None) -> list[dict]:
    """
    Execute a parameterized BigQuery query and return rows as plain dicts.

    Parameters
    ----------
    query  : Standard SQL string, with optional @param placeholders.
    params : List of bigquery.ScalarQueryParameter (or Struct/Array variants).

    Returns
    -------
    List of row dicts.  Empty list when no rows match.
    """
    job_config = bigquery.QueryJobConfig(query_parameters=params or [])
    query_job  = _get_client().query(query, job_config=job_config)
    result     = query_job.result()
    return [dict(row) for row in result]


# ---------------------------------------------------------------------------
# USER FUNCTIONS
# ---------------------------------------------------------------------------

def get_user(user_id: str) -> dict | None:
    """
    Fetch a single user by user_id.

    Returns the user dict, or None if not found.
    The `sports` field comes back as a list of dicts
    [{"sport": "...", "skill_level": "..."}, ...].
    """
    query = f"""
        SELECT
            user_id,
            email,
            created_at,
            home_lat,
            home_lng,
            sports
        FROM {_tbl("users")}
        WHERE user_id = @user_id
        LIMIT 1
    """
    params = [bigquery.ScalarQueryParameter("user_id", "STRING", user_id)]
    rows   = run_query(query, params)
    return rows[0] if rows else None


def get_users_by_sport(sport: str) -> list[dict]:
    """
    Return all users whose `sports` array contains the given sport name
    (case-insensitive).

    Each row includes user_id, email, and the matched skill_level.
    """
    query = f"""
        SELECT
            u.user_id,
            u.email,
            s.sport,
            s.skill_level
        FROM {_tbl("users")} u
        CROSS JOIN UNNEST(u.sports) AS s
        WHERE LOWER(s.sport) = LOWER(@sport)
        ORDER BY u.user_id
    """
    params = [bigquery.ScalarQueryParameter("sport", "STRING", sport)]
    return run_query(query, params)


# ---------------------------------------------------------------------------
# FRIENDSHIP FUNCTIONS
# ---------------------------------------------------------------------------

def get_friends(user_id: str) -> list[dict]:
    """
    Return all accepted friends of a user.

    Because the table is bidirectional (two rows per friendship),
    we only query rows where user_id = @user_id and status = 'accepted'.
    """
    query = f"""
        SELECT
            f.friend_id,
            u.email,
            f.created_at,
            f.updated_at
        FROM {_tbl("friendship")} f
        JOIN {_tbl("users")} u
          ON u.user_id = f.friend_id
        WHERE f.user_id  = @user_id
          AND f.status   = 'accepted'
        ORDER BY f.updated_at DESC
    """
    params = [bigquery.ScalarQueryParameter("user_id", "STRING", user_id)]
    return run_query(query, params)


def send_friend_request(user_id: str, friend_id: str) -> None:
    """
    Insert two pending friendship rows (one per direction).

    Raises ValueError if a relationship already exists in any status.
    """
    # Guard: check for an existing row in either direction
    check_query = f"""
        SELECT COUNT(*) AS cnt
        FROM {_tbl("friendship")}
        WHERE (user_id = @user_id AND friend_id = @friend_id)
           OR (user_id = @friend_id AND friend_id = @user_id)
    """
    params = [
        bigquery.ScalarQueryParameter("user_id",   "STRING", user_id),
        bigquery.ScalarQueryParameter("friend_id", "STRING", friend_id),
    ]
    rows = run_query(check_query, params)
    if rows and rows[0]["cnt"] > 0:
        raise ValueError(
            f"A friendship relationship already exists between {user_id} and {friend_id}."
        )

    now = datetime.now(timezone.utc).isoformat()
    insert_query = f"""
        INSERT INTO {_tbl("friendship")}
            (user_id, friend_id, status, requested_by, created_at, updated_at)
        VALUES
            (@user_id,   @friend_id, 'pending', @user_id, @now, @now),
            (@friend_id, @user_id,   'pending', @user_id, @now, @now)
    """
    params_insert = [
        bigquery.ScalarQueryParameter("user_id",   "STRING",    user_id),
        bigquery.ScalarQueryParameter("friend_id", "STRING",    friend_id),
        bigquery.ScalarQueryParameter("now",       "TIMESTAMP", now),
    ]
    run_query(insert_query, params_insert)


def _update_friendship_status(user_id: str, friend_id: str, new_status: str) -> None:
    """
    Internal helper: UPDATE both directional rows to new_status.
    BigQuery DML UPDATE is used here because friendship status is mutable metadata,
    not an event-sourced log.
    """
    now = datetime.now(timezone.utc).isoformat()
    update_query = f"""
        UPDATE {_tbl("friendship")}
        SET    status     = @new_status,
               updated_at = @now
        WHERE (user_id = @user_id   AND friend_id = @friend_id)
           OR (user_id = @friend_id AND friend_id = @user_id)
    """
    params = [
        bigquery.ScalarQueryParameter("new_status", "STRING",    new_status),
        bigquery.ScalarQueryParameter("user_id",    "STRING",    user_id),
        bigquery.ScalarQueryParameter("friend_id",  "STRING",    friend_id),
        bigquery.ScalarQueryParameter("now",        "TIMESTAMP", now),
    ]
    run_query(update_query, params)


def accept_friend_request(user_id: str, friend_id: str) -> None:
    """
    Accept a pending friend request from friend_id to user_id.
    Both directional rows are updated to 'accepted'.
    """
    _update_friendship_status(user_id, friend_id, "accepted")


def reject_friend_request(user_id: str, friend_id: str) -> None:
    """
    Reject / delete a pending friend request.
    Both directional rows are deleted to keep the table clean.
    Note: BigQuery DELETE is DML and incurs at least 10 MB billing minimum.
    """
    delete_query = f"""
        DELETE FROM {_tbl("friendship")}
        WHERE (user_id = @user_id   AND friend_id = @friend_id)
           OR (user_id = @friend_id AND friend_id = @user_id)
    """
    params = [
        bigquery.ScalarQueryParameter("user_id",   "STRING", user_id),
        bigquery.ScalarQueryParameter("friend_id", "STRING", friend_id),
    ]
    run_query(delete_query, params)


# ---------------------------------------------------------------------------
# EVENT FUNCTIONS
# ---------------------------------------------------------------------------

def get_event(event_id: str) -> dict | None:
    """
    Fetch a single event by event_id.

    The nested `location` STRUCT is returned as a dict inside the row dict.
    Returns None if not found.
    """
    query = f"""
        SELECT
            event_id,
            sport,
            location,
            created_by,
            start_time,
            end_time,
            max_players,
            visibility,
            status,
            created_at,
            updated_at
        FROM {_tbl("events")}
        WHERE event_id = @event_id
        LIMIT 1
    """
    params = [bigquery.ScalarQueryParameter("event_id", "STRING", event_id)]
    rows   = run_query(query, params)
    return rows[0] if rows else None


def get_events_by_sport(sport: str, status: str = "open") -> list[dict]:
    """
    Return all public events for a given sport that are still open.

    Parameters
    ----------
    sport  : Sport name (case-insensitive match).
    status : Filter by event status (default 'open').
    """
    query = f"""
        SELECT
            event_id,
            sport,
            location,
            created_by,
            start_time,
            end_time,
            max_players,
            status,
            visibility
        FROM {_tbl("events")}
        WHERE LOWER(sport)  = LOWER(@sport)
          AND status        = @status
          AND visibility    = 'public'
          AND start_time   >= CURRENT_TIMESTAMP()
        ORDER BY start_time ASC
    """
    params = [
        bigquery.ScalarQueryParameter("sport",  "STRING", sport),
        bigquery.ScalarQueryParameter("status", "STRING", status),
    ]
    return run_query(query, params)


def get_nearby_events(
    lat: float,
    lng: float,
    radius_meters: float = 5000,
    status: str = "open",
) -> list[dict]:
    """
    Return public, open events whose venue is within `radius_meters` of
    the supplied coordinates.

    Uses ST_DISTANCE on the embedded location.geog STRUCT field.

    Parameters
    ----------
    lat            : Latitude of the search origin.
    lng            : Longitude of the search origin.
    radius_meters  : Search radius in metres (default 5 km).
    status         : Event status filter (default 'open').
    """
    query = f"""
        SELECT
            event_id,
            sport,
            location,
            created_by,
            start_time,
            end_time,
            max_players,
            status,
            ST_DISTANCE(
                location.geog,
                ST_GEOGPOINT(@lng, @lat)
            ) AS distance_meters
        FROM {_tbl("events")}
        WHERE visibility  = 'public'
          AND status      = @status
          AND start_time >= CURRENT_TIMESTAMP()
          AND ST_DISTANCE(
                location.geog,
                ST_GEOGPOINT(@lng, @lat)
              ) <= @radius_meters
        ORDER BY distance_meters ASC
    """
    params = [
        bigquery.ScalarQueryParameter("lat",           "FLOAT64", lat),
        bigquery.ScalarQueryParameter("lng",           "FLOAT64", lng),
        bigquery.ScalarQueryParameter("radius_meters", "FLOAT64", radius_meters),
        bigquery.ScalarQueryParameter("status",        "STRING",  status),
    ]
    return run_query(query, params)


def get_user_created_events(user_id: str) -> list[dict]:
    """
    Return all events created by a specific user, newest first.
    """
    query = f"""
        SELECT
            event_id,
            sport,
            location,
            start_time,
            end_time,
            max_players,
            status,
            visibility,
            created_at
        FROM {_tbl("events")}
        WHERE created_by = @user_id
        ORDER BY created_at DESC
    """
    params = [bigquery.ScalarQueryParameter("user_id", "STRING", user_id)]
    return run_query(query, params)


# ---------------------------------------------------------------------------
# EVENT PARTICIPANT FUNCTIONS
# ---------------------------------------------------------------------------

def join_event(user_id: str, event_id: str) -> None:
    """
    Add a user to an event by inserting a 'joined' participant row.

    Raises ValueError if:
      - The event does not exist or is not open.
      - The user has already joined (active 'joined' row exists).
      - The event is at max capacity.
    """
    # Verify event is open
    event = get_event(event_id)
    if not event:
        raise ValueError(f"Event {event_id} not found.")
    if event["status"] != "open":
        raise ValueError(f"Event {event_id} is not open (status={event['status']}).")

    # Check for existing active join
    check_query = f"""
        SELECT COUNT(*) AS cnt
        FROM {_tbl("event_participants")}
        WHERE event_id = @event_id
          AND user_id  = @user_id
          AND status   = 'joined'
    """
    params = [
        bigquery.ScalarQueryParameter("event_id", "STRING", event_id),
        bigquery.ScalarQueryParameter("user_id",  "STRING", user_id),
    ]
    rows = run_query(check_query, params)
    if rows and rows[0]["cnt"] > 0:
        raise ValueError(f"User {user_id} has already joined event {event_id}.")

    # Check capacity
    if event.get("max_players") is not None:
        capacity_query = f"""
            SELECT COUNT(*) AS cnt
            FROM {_tbl("event_participants")}
            WHERE event_id = @event_id
              AND status   = 'joined'
        """
        cap_rows = run_query(capacity_query, params[:1])  # only event_id needed
        if cap_rows and cap_rows[0]["cnt"] >= event["max_players"]:
            raise ValueError(f"Event {event_id} is at full capacity.")

    insert_query = f"""
        INSERT INTO {_tbl("event_participants")}
            (event_id, user_id, joined_at, status)
        VALUES
            (@event_id, @user_id, CURRENT_TIMESTAMP(), 'joined')
    """
    run_query(insert_query, params)


def leave_event(user_id: str, event_id: str) -> None:
    """
    Mark a user as having left an event.

    Follows the append-only pattern: inserts a new row with status='left'
    rather than deleting or updating the original join row.
    """
    insert_query = f"""
        INSERT INTO {_tbl("event_participants")}
            (event_id, user_id, joined_at, status)
        VALUES
            (@event_id, @user_id, CURRENT_TIMESTAMP(), 'left')
    """
    params = [
        bigquery.ScalarQueryParameter("event_id", "STRING", event_id),
        bigquery.ScalarQueryParameter("user_id",  "STRING", user_id),
    ]
    run_query(insert_query, params)


def get_event_participants(event_id: str) -> list[dict]:
    """
    Return users who are currently 'joined' to an event.

    Because the table is append-only, we take the latest status row per user
    and filter for those whose last action was 'joined'.
    """
    query = f"""
        WITH latest AS (
            SELECT
                user_id,
                status,
                joined_at,
                ROW_NUMBER() OVER (
                    PARTITION BY event_id, user_id
                    ORDER BY joined_at DESC
                ) AS rn
            FROM {_tbl("event_participants")}
            WHERE event_id = @event_id
        )
        SELECT
            l.user_id,
            u.email,
            l.joined_at,
            l.status
        FROM latest l
        JOIN {_tbl("users")} u ON u.user_id = l.user_id
        WHERE l.rn     = 1
          AND l.status = 'joined'
        ORDER BY l.joined_at ASC
    """
    params = [bigquery.ScalarQueryParameter("event_id", "STRING", event_id)]
    return run_query(query, params)


# ---------------------------------------------------------------------------
# ACTIVITY FUNCTIONS
# ---------------------------------------------------------------------------

def log_activity(
    user_id: str,
    activity_type: str,
    event_id: str | None = None,
    sport: str | None = None,
    duration_minutes: int | None = None,
    location: dict | None = None,
) -> None:
    """
    Append a row to user_activity.

    BigQuery does not honour GENERATE_UUID() as a column default in DML,
    so we generate the UUID in Python.

    Parameters
    ----------
    user_id          : ID of the acting user.
    activity_type    : e.g. 'join_event', 'leave_event', 'view_event', 'search'.
    event_id         : Optional linked event.
    sport            : Optional sport name.
    duration_minutes : Optional session length in minutes.
    location         : Optional dict with keys: location_id, name, lat, lng.
                       The geog point is computed from lat/lng inside BigQuery.
    """
    activity_id = str(uuid.uuid4())

    if location:
        query = f"""
            INSERT INTO {_tbl("user_activity")}
                (activity_id, user_id, event_id, sport, duration_minutes,
                 location, activity_type, timestamp)
            VALUES (
                @activity_id,
                @user_id,
                @event_id,
                @sport,
                @duration_minutes,
                STRUCT(
                    @loc_id   AS location_id,
                    @loc_name AS name,
                    @loc_lat  AS lat,
                    @loc_lng  AS lng,
                    ST_GEOGPOINT(@loc_lng, @loc_lat) AS geog
                ),
                @activity_type,
                CURRENT_TIMESTAMP()
            )
        """
        params = [
            bigquery.ScalarQueryParameter("activity_id",       "STRING",  activity_id),
            bigquery.ScalarQueryParameter("user_id",           "STRING",  user_id),
            bigquery.ScalarQueryParameter("event_id",          "STRING",  event_id),
            bigquery.ScalarQueryParameter("sport",             "STRING",  sport),
            bigquery.ScalarQueryParameter("duration_minutes",  "INT64",   duration_minutes),
            bigquery.ScalarQueryParameter("activity_type",     "STRING",  activity_type),
            bigquery.ScalarQueryParameter("loc_id",            "STRING",  location.get("location_id")),
            bigquery.ScalarQueryParameter("loc_name",          "STRING",  location.get("name")),
            bigquery.ScalarQueryParameter("loc_lat",           "FLOAT64", location.get("lat")),
            bigquery.ScalarQueryParameter("loc_lng",           "FLOAT64", location.get("lng")),
        ]
    else:
        query = f"""
            INSERT INTO {_tbl("user_activity")}
                (activity_id, user_id, event_id, sport,
                 duration_minutes, activity_type, timestamp)
            VALUES (
                @activity_id,
                @user_id,
                @event_id,
                @sport,
                @duration_minutes,
                @activity_type,
                CURRENT_TIMESTAMP()
            )
        """
        params = [
            bigquery.ScalarQueryParameter("activity_id",      "STRING", activity_id),
            bigquery.ScalarQueryParameter("user_id",          "STRING", user_id),
            bigquery.ScalarQueryParameter("event_id",         "STRING", event_id),
            bigquery.ScalarQueryParameter("sport",            "STRING", sport),
            bigquery.ScalarQueryParameter("duration_minutes", "INT64",  duration_minutes),
            bigquery.ScalarQueryParameter("activity_type",    "STRING", activity_type),
        ]

    run_query(query, params)


def get_user_activity(
    user_id: str,
    limit: int = 50,
    activity_type: str | None = None,
) -> list[dict]:
    """
    Fetch recent activity rows for a user, newest first.

    Parameters
    ----------
    user_id       : Target user.
    limit         : Max rows to return (default 50).
    activity_type : Optional filter (e.g. 'join_event').
    """
    type_filter = "AND activity_type = @activity_type" if activity_type else ""
    query = f"""
        SELECT
            activity_id,
            user_id,
            event_id,
            sport,
            duration_minutes,
            location,
            activity_type,
            timestamp
        FROM {_tbl("user_activity")}
        WHERE user_id = @user_id
        {type_filter}
        ORDER BY timestamp DESC
        LIMIT @limit
    """
    params = [
        bigquery.ScalarQueryParameter("user_id", "STRING", user_id),
        bigquery.ScalarQueryParameter("limit",   "INT64",  limit),
    ]
    if activity_type:
        params.append(
            bigquery.ScalarQueryParameter("activity_type", "STRING", activity_type)
        )
    return run_query(query, params)


# ---------------------------------------------------------------------------
# COMMUNITY / POSTS FUNCTIONS
# ---------------------------------------------------------------------------

def get_friend_activity(user_id: str, limit: int = 10) -> list[dict]:
    """
    Return the most recent activity rows from a user's accepted friends,
    ordered by timestamp descending.

    Parameters
    ----------
    user_id : The current user whose friends' activity we want.
    limit   : Max rows to return (default 10).

    Returns
    -------
    List of activity dicts, each including the friend's email for display.
    Keys: activity_id, user_id, email, event_id, sport, duration_minutes,
          location, activity_type, timestamp.
    """
    query = f"""
        SELECT
            a.activity_id,
            a.user_id,
            u.email,
            a.event_id,
            a.sport,
            a.duration_minutes,
            a.location,
            a.activity_type,
            a.timestamp
        FROM {_tbl("user_activity")} a
        JOIN {_tbl("friendship")} f
          ON f.friend_id = a.user_id
        JOIN {_tbl("users")} u
          ON u.user_id   = a.user_id
        WHERE f.user_id = @user_id
          AND f.status  = 'accepted'
        ORDER BY a.timestamp DESC
        LIMIT @limit
    """
    params = [
        bigquery.ScalarQueryParameter("user_id", "STRING", user_id),
        bigquery.ScalarQueryParameter("limit",   "INT64",  limit),
    ]
    return run_query(query, params)


def create_post(user_id: str, content: str) -> str:
    """
    Insert a new post into the posts table.

    Parameters
    ----------
    user_id : Author of the post.
    content : Text content of the post.

    Returns
    -------
    The generated post_id (UUID string).
    """
    post_id = str(uuid.uuid4())
    query = f"""
        INSERT INTO {_tbl("posts")}
            (post_id, user_id, content, created_at)
        VALUES
            (@post_id, @user_id, @content, CURRENT_TIMESTAMP())
    """
    params = [
        bigquery.ScalarQueryParameter("post_id",  "STRING", post_id),
        bigquery.ScalarQueryParameter("user_id",  "STRING", user_id),
        bigquery.ScalarQueryParameter("content",  "STRING", content),
    ]
    run_query(query, params)
    return post_id


def get_user_posts(user_id: str, limit: int = 10) -> list[dict]:
    """
    Return recent posts by a specific user, newest first.

    Parameters
    ----------
    user_id : Author whose posts to fetch.
    limit   : Max rows to return (default 10).

    Returns
    -------
    List of post dicts with keys: post_id, user_id, content, created_at.
    """
    query = f"""
        SELECT
            post_id,
            user_id,
            content,
            created_at
        FROM {_tbl("posts")}
        WHERE user_id = @user_id
        ORDER BY created_at DESC
        LIMIT @limit
    """
    params = [
        bigquery.ScalarQueryParameter("user_id", "STRING", user_id),
        bigquery.ScalarQueryParameter("limit",   "INT64",  limit),
    ]
    return run_query(query, params)


def get_friend_posts(user_id: str, limit: int = 10) -> list[dict]:
    """
    Return the most recent posts from a user's accepted friends,
    ordered by timestamp descending.

    Parameters
    ----------
    user_id : The current user whose friends' posts we want.
    limit   : Max rows to return (default 10).

    Returns
    -------
    List of post dicts with keys:
    post_id, user_id, email, content, created_at.
    """
    query = f"""
        SELECT
            p.post_id,
            p.user_id,
            u.email,
            p.content,
            p.created_at
        FROM {_tbl("posts")} p
        JOIN {_tbl("friendship")} f
          ON f.friend_id = p.user_id
        JOIN {_tbl("users")} u
          ON u.user_id   = p.user_id
        WHERE f.user_id = @user_id
          AND f.status  = 'accepted'
        ORDER BY p.created_at DESC
        LIMIT @limit
    """
    params = [
        bigquery.ScalarQueryParameter("user_id", "STRING", user_id),
        bigquery.ScalarQueryParameter("limit",   "INT64",  limit),
    ]
    return run_query(query, params)


# ---------------------------------------------------------------------------
# MESSAGES FUNCTIONS
# ---------------------------------------------------------------------------

def send_message(sender_id: str, receiver_id: str, content: str) -> str:
    """
    Insert a new message into the messages table.

    Returns the generated message_id (UUID string).
    """
    message_id = str(uuid.uuid4())
    query = f"""
        INSERT INTO {_tbl("messages")}
            (message_id, sender_id, receiver_id, content, timestamp, `read`)
        VALUES
            (@message_id, @sender_id, @receiver_id, @content, CURRENT_TIMESTAMP(), FALSE)
    """
    params = [
        bigquery.ScalarQueryParameter("message_id",  "STRING", message_id),
        bigquery.ScalarQueryParameter("sender_id",   "STRING", sender_id),
        bigquery.ScalarQueryParameter("receiver_id", "STRING", receiver_id),
        bigquery.ScalarQueryParameter("content",     "STRING", content),
    ]
    run_query(query, params)
    return message_id


def get_messages(user_id: str, other_id: str, limit: int = 50) -> list[dict]:
    """
    Return all messages between user_id and other_id, oldest first.

    Parameters
    ----------
    user_id  : The current user.
    other_id : The other user in the conversation.
    limit    : Max messages to return (default 50).

    Returns
    -------
    List of message dicts with keys:
    message_id, sender_id, receiver_id, content, timestamp, read.
    """
    query = f"""
        SELECT
            message_id,
            sender_id,
            receiver_id,
            content,
            timestamp,
            `read`
        FROM {_tbl("messages")}
        WHERE (sender_id = @user_id AND receiver_id = @other_id)
           OR (sender_id = @other_id AND receiver_id = @user_id)
        ORDER BY timestamp ASC
        LIMIT @limit
    """
    params = [
        bigquery.ScalarQueryParameter("user_id",  "STRING", user_id),
        bigquery.ScalarQueryParameter("other_id", "STRING", other_id),
        bigquery.ScalarQueryParameter("limit",    "INT64",  limit),
    ]
    return run_query(query, params)


# ---------------------------------------------------------------------------
# GENAI ADVICE FUNCTION
# ---------------------------------------------------------------------------

def get_genai_advice(user_id: str) -> str:
    """
    Call the Anthropic API to generate a personalized motivational advice
    message for the user based on their recent activity.

    Parameters
    ----------
    user_id : The user to generate advice for.

    Returns
    -------
    A plain-text advice string from the model.
    """
    # Pull recent activity to ground the advice
    recent = get_user_activity(user_id, limit=5)

    if recent:
        sports_played = list({r["sport"] for r in recent if r.get("sport")})
        total_minutes = sum(r["duration_minutes"] or 0 for r in recent)
        context = (
            f"The user has recently played {', '.join(sports_played) if sports_played else 'various sports'} "
            f"for a total of {total_minutes} minutes across their last {len(recent)} sessions."
        )
    else:
        context = "The user is just getting started and has no recent activity yet."

    message = _get_anthropic_client().messages.create(

        model="claude-sonnet-4-20250514",
        max_tokens=200,
        messages=[
            {
                "role": "user",
                "content": (
                    f"{context} "
                    "Give them a short (2-3 sentence), friendly tip about joining local sports "
                    "events, connecting with friends through sports, or getting more involved in "
                    "their sports community. Be specific to their recent activities if possible."
                ),
            }
        ],
    )
    return message.content[0].text


# ---------------------------------------------------------------------------
# RECOMMENDATION FUNCTIONS
# ---------------------------------------------------------------------------

def get_recommended_events(user_id: str, limit: int = 10) -> list[dict]:
    """
    Return precomputed recommended events for a user from the cache table,
    joined with full event details.

    Events are sorted by recommendation score (highest first).
    Only future, open, public events are returned.

    Parameters
    ----------
    user_id : Target user.
    limit   : Max recommendations to return (default 10).
    """
    query = f"""
        SELECT
            r.event_id,
            r.score,
            e.sport,
            e.location,
            e.created_by,
            e.start_time,
            e.end_time,
            e.max_players,
            e.status,
            r.generated_at
        FROM {_tbl("user_recommendations")} r
        JOIN {_tbl("events")} e
          ON e.event_id = r.event_id
        WHERE r.user_id      = @user_id
          AND e.status       = 'open'
          AND e.visibility   = 'public'
          AND e.start_time  >= CURRENT_TIMESTAMP()
        ORDER BY r.score DESC
        LIMIT @limit
    """
    params = [
        bigquery.ScalarQueryParameter("user_id", "STRING", user_id),
        bigquery.ScalarQueryParameter("limit",   "INT64",  limit),
    ]
    return run_query(query, params)