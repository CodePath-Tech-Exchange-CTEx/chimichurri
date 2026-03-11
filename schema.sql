-- =============================================================
-- Users Table
-- =============================================================

Create Table 'project.database.users' (
    user_id   String Not Null,
    email    String Not Null,
    created_at  Timestamp Not Null Default Current_Timestamp(),

    home_lat  Float64,
    home_lng  Float64,
    home_geog Geography Generated Always As (ST_GeogFromText('POINT(' || home_lat || ' ' || home_lng || ')')) Stored,

    sports Array<Struct<
        sport String,
        skill_level String,
    >>
)
Partition by Date(created_at)
Cluster By user_id
Options(
    description = "Users Table",
);

-- =============================================================
-- Friendship Table
-- =============================================================

Create Table 'project.database.friendship' (
    user_id        STRING NOT NULL,
    friend_id      STRING NOT NULL,
    status         STRING NOT NULL,  -- 'pending', 'accepted', 'blocked'
    requested_by   STRING,
    created_at     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    updated_at     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY DATE(created_at)
CLUSTER BY user_id, friend_id
OPTIONS (
    description = "One row per direction of friendship for efficient querying"
);

-- =============================================================
-- Locations Table
-- =============================================================

CREATE TABLE 'project.database.locations' (
    location_id STRING NOT NULL,
    name        STRING,
    address     STRING,

    lat         FLOAT64,
    lng         FLOAT64,
    geog        GEOGRAPHY GENERATED ALWAYS AS (ST_GEOGPOINT(lng, lat)) STORED,

    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    created_by  STRING,
    is_public   BOOL DEFAULT TRUE
)
PARTITION BY DATE(created_at)
CLUSTER BY location_id
OPTIONS (
    description = "Reusable physical locations / venues"
);

-- =============================================================
-- Events Table
-- =============================================================

CREATE TABLE 'project.database.events' (
    event_id    STRING NOT NULL,
    sport       STRING NOT NULL,

    location STRUCT<
        location_id STRING,
        name STRING,
        address STRING,
        lat FLOAT64,
        lng FLOAT64,
        geog GEOGRAPHY
    >,

    created_by  STRING NOT NULL, -- Foreign key to user_id
    start_time  TIMESTAMP NOT NULL,
    end_time    TIMESTAMP NOT NULL,

    max_players INT64,
    visibility  STRING DEFAULT 'public', -- public, friends-only, invite-only
    status      STRING DEFAULT 'open',   -- open, full, cancelled, completed

    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY DATE(start_time)
CLUSTER BY sport, created_by
OPTIONS (
    description = "Sport events / matches / games"
);

-- =============================================================
-- Event Participants Table
-- =============================================================

CREATE TABLE 'project.database.event_participants' (
    event_id    STRING NOT NULL, -- Foreign key to event
    user_id     STRING NOT NULL, -- Foreign key to user_id
    joined_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    status      STRING DEFAULT 'joined' -- joined, cancelled, waitlisted
)
PARTITION BY DATE(joined_at)
CLUSTER BY event_id, user_id
OPTIONS (
    description = "Append-only participant records per event"
);

SELECT COUNT(*)
FROM event_participants
WHERE event_id = 'E1'
AND status = 'joined';

-- =============================================================
-- User Activity / Session History
-- =============================================================

CREATE TABLE 'project.database.user_activity' (
    activity_id      STRING NOT NULL DEFAULT GENERATE_UUID(),
    user_id          STRING NOT NULL, -- Foreign key to user_id
    event_id         STRING, -- Foreign key to event
    sport            STRING,
    duration_minutes INT64,

    location STRUCT<
        location_id STRING,
        name STRING,
        lat FLOAT64,
        lng FLOAT64,
        geog GEOGRAPHY
    >,

    activity_type    STRING,  -- joined_event, completed_event, cancelled_event, created_event
    timestamp        TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY DATE(timestamp)
CLUSTER BY user_id, timestamp
OPTIONS (
    description = "Append-only history of user actions and sessions"
);

-- =============================================================
-- User Recommendations Cache
-- =============================================================

CREATE TABLE 'project.database.user_recommendations' (
    user_id       STRING NOT NULL, -- Foreign key to user_id
    event_id      STRING NOT NULL, -- Foreign key to event
    score         FLOAT64 NOT NULL,
    generated_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY DATE(generated_at)
CLUSTER BY user_id
OPTIONS (
    description = "Precomputed recommendations for fast API serving"
);

-- The Flask API would simply do:
-- SELECT event_id
-- FROM user_recommendations
-- WHERE user_id = 'X'
-- ORDER BY score DESC
-- LIMIT 20;