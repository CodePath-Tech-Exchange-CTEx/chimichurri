-- =============================================================
-- Users Table
-- =============================================================
CREATE TABLE `carlos-negron-uprm.database.users` (
    user_id    STRING NOT NULL,
    email      STRING NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),

    home_lat   FLOAT64,
    home_lng   FLOAT64,
    home_geog  GEOGRAPHY,  -- ← plain column, no GENERATED ALWAYS AS

    sports ARRAY<STRUCT
        sport       STRING,
        skill_level STRING
    >>
)
PARTITION BY DATE(created_at)
CLUSTER BY user_id
OPTIONS(
    description = "Users Table"
);

-- =============================================================
-- Friendship Table
-- =============================================================

CREATE TABLE `carlos-negron-uprm.database.friendship` (
    user_id        STRING NOT NULL,
    friend_id      STRING NOT NULL,
    status         STRING NOT NULL,  -- 'pending', 'accepted', 'blocked'
    requested_by   STRING,
    created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    updated_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY DATE(created_at)
CLUSTER BY user_id, friend_id
OPTIONS (
    description = "One row per direction of friendship for efficient querying"
);

-- =============================================================
-- Locations Table
-- =============================================================
CREATE TABLE `carlos-negron-uprm.database.locations` (
    location_id STRING NOT NULL,
    name        STRING,
    address     STRING,

    lat         FLOAT64,
    lng         FLOAT64,
    geog        GEOGRAPHY,

    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    created_by  STRING,
    is_public   BOOL DEFAULT TRUE
)
PARTITION BY DATE(created_at)
CLUSTER BY location_id
OPTIONS (
    description = "Reusable physical locations / venues"
);

-- =============================================================
-- Events Table
-- =============================================================

CREATE TABLE `carlos-negron-uprm.database.events` (
    event_id    STRING NOT NULL,
    sport       STRING NOT NULL,

    location STRUCT<
        location_id STRING,
        name STRING,
        address STRING,
        lat FLOAT64,
        lng FLOAT64,
        geog GEOGRAPHY
    >,

    created_by  STRING NOT NULL, 
    start_time  TIMESTAMP NOT NULL,
    end_time    TIMESTAMP NOT NULL,

    max_players INT64,
    visibility  STRING DEFAULT 'public', 
    status      STRING DEFAULT 'open',   

    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY DATE(start_time)
CLUSTER BY sport, created_by
OPTIONS (
    description = "Sport events / matches / games"
);

-- =============================================================
-- Event Participants Table
-- =============================================================

CREATE TABLE `carlos-negron-uprm.database.event_participants` (
    event_id    STRING NOT NULL,
    user_id     STRING NOT NULL,
    joined_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    status      STRING DEFAULT 'joined'
)
PARTITION BY DATE(joined_at)
CLUSTER BY event_id, user_id
OPTIONS (
    description = "Append-only participant records per event"
);

-- =============================================================
-- User Activity / Session History
-- =============================================================

CREATE TABLE `carlos-negron-uprm.database.user_activity` (
    activity_id      STRING DEFAULT GENERATE_UUID(),
    user_id          STRING NOT NULL,
    event_id         STRING,
    sport            STRING,
    duration_minutes INT64,

    location STRUCT<
        location_id STRING,
        name STRING,
        lat FLOAT64,
        lng FLOAT64,
        geog GEOGRAPHY
    >,

    activity_type    STRING,  
    timestamp        TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY DATE(timestamp)
CLUSTER BY user_id, timestamp
OPTIONS (
    description = "Append-only history of user actions and sessions"
);

-- =============================================================
-- User Recommendations Cache
-- =============================================================

CREATE TABLE `carlos-negron-uprm.database.user_recommendations` (
    user_id       STRING NOT NULL,
    event_id      STRING NOT NULL,
    score         FLOAT64 NOT NULL,
    generated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY DATE(generated_at)
CLUSTER BY user_id
OPTIONS (
    description = "Precomputed recommendations for fast API serving"
);


-- =============================================================
-- Posts Table (NEW - required for community share feature)
-- =============================================================
CREATE TABLE `carlos-negron-uprm.database.posts` (
    post_id    STRING NOT NULL,
    user_id    STRING NOT NULL,
    content    STRING,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY DATE(created_at)
CLUSTER BY user_id
OPTIONS (
    description = "Community posts shared by users"
);