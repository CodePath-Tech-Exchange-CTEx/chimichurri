CREATE TABLE `carlos-negron-uprm.database.messages` (
    message_id  STRING  NOT NULL,
    sender_id   STRING  NOT NULL,
    receiver_id STRING  NOT NULL,
    content     STRING  NOT NULL,
    timestamp   TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    `read`      BOOL    DEFAULT FALSE
)
PARTITION BY DATE(timestamp)
CLUSTER BY sender_id, receiver_id
OPTIONS (description = "Direct messages between users");