"""
api/messages.py — Chat / messaging endpoints
=============================================

GET  /api/messages/<user_id>/conversations
     List all conversation partners with the last message preview.

GET  /api/messages/<user_id>/with/<other_id>
     Full message thread between two users, chronological.

POST /api/messages/send
     Body: { sender_id, receiver_id, content }

This requires the messages table to exist in BigQuery.
Schema (add to schema.sql if not already present):

    CREATE TABLE `carlos-negron-uprm.database.messages` (
        message_id  STRING  NOT NULL,
        sender_id   STRING  NOT NULL,
        receiver_id STRING  NOT NULL,
        content     STRING  NOT NULL,
        timestamp   TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
        read        BOOL    DEFAULT FALSE
    )
    PARTITION BY DATE(timestamp)
    CLUSTER BY sender_id, receiver_id
    OPTIONS (description = "Direct messages between users");
"""

import uuid
from flask import Blueprint, jsonify, request
from google.cloud import bigquery

from api.db import run_query, tbl

messages_bp = Blueprint("messages", __name__)


# ── routes ─────────────────────────────────────────────────────────────────

@messages_bp.get("/<user_id>/conversations")
def list_conversations(user_id: str):
    """
    Return each unique conversation partner with:
      - their user_id and email
      - the last message content and timestamp
      - count of unread messages (sent to user_id, not yet read)

    The UNION lets us find partners regardless of who sent first.
    """
    sql = f"""
        WITH all_msgs AS (
            SELECT
                message_id, sender_id, receiver_id,
                content, timestamp, `read`
            FROM {tbl("messages")}
            WHERE sender_id = @uid OR receiver_id = @uid
        ),
        partners AS (
            SELECT
                CASE WHEN sender_id = @uid THEN receiver_id ELSE sender_id END AS partner_id,
                content,
                timestamp,
                CASE
                    WHEN receiver_id = @uid AND NOT `read` THEN 1
                    ELSE 0
                END AS is_unread,
                ROW_NUMBER() OVER (
                    PARTITION BY
                        CASE WHEN sender_id = @uid THEN receiver_id ELSE sender_id END
                    ORDER BY timestamp DESC
                ) AS rn
            FROM all_msgs
        )
        SELECT
            p.partner_id,
            u.email            AS partner_email,
            p.content          AS last_message,
            p.timestamp        AS last_timestamp,
            SUM(p.is_unread) OVER (PARTITION BY p.partner_id) AS unread_count
        FROM partners p
        JOIN {tbl("users")} u ON u.user_id = p.partner_id
        WHERE p.rn = 1
        ORDER BY p.timestamp DESC
    """
    rows = run_query(sql, [bigquery.ScalarQueryParameter("uid", "STRING", user_id)])

    convos = []
    for r in rows:
        convos.append({
            "partner_id":    r["partner_id"],
            "partner_email": r["partner_email"],
            "last_message":  r["last_message"],
            "last_timestamp": r["last_timestamp"].isoformat() if r.get("last_timestamp") else None,
            "unread_count":  r["unread_count"],
        })
    return jsonify({"conversations": convos}), 200


@messages_bp.get("/<user_id>/with/<other_id>")
def get_thread(user_id: str, other_id: str):
    """
    Return all messages between user_id and other_id, oldest first.
    Also marks messages sent to user_id as read (append-friendly note:
    BigQuery DML UPDATE is used here because read-status is mutable
    metadata, not event-sourced).
    """
    sql = f"""
        SELECT
            message_id, sender_id, receiver_id,
            content, timestamp, `read`
        FROM {tbl("messages")}
        WHERE (sender_id = @uid AND receiver_id = @oid)
           OR (sender_id = @oid AND receiver_id = @uid)
        ORDER BY timestamp ASC
    """
    params = [
        bigquery.ScalarQueryParameter("uid", "STRING", user_id),
        bigquery.ScalarQueryParameter("oid", "STRING", other_id),
    ]
    rows = run_query(sql, params)

    # Mark incoming messages as read
    mark_sql = f"""
        UPDATE {tbl("messages")}
        SET `read` = TRUE
        WHERE sender_id   = @oid
          AND receiver_id = @uid
          AND `read`      = FALSE
    """
    run_query(mark_sql, params)

    messages = [
        {
            "message_id":  r["message_id"],
            "sender_id":   r["sender_id"],
            "receiver_id": r["receiver_id"],
            "content":     r["content"],
            "timestamp":   r["timestamp"].isoformat() if r.get("timestamp") else None,
            "read":        r["read"],
        }
        for r in rows
    ]
    return jsonify({"messages": messages, "count": len(messages)}), 200


@messages_bp.post("/send")
def send_message():
    """
    Body (JSON): { sender_id, receiver_id, content }
    Returns the new message_id.
    """
    body = request.get_json(silent=True) or {}
    required = ["sender_id", "receiver_id", "content"]
    missing  = [f for f in required if not body.get(f)]
    if missing:
        return jsonify({"error": f"missing fields: {missing}"}), 400

    if body["sender_id"] == body["receiver_id"]:
        return jsonify({"error": "cannot message yourself"}), 400

    message_id = str(uuid.uuid4())
    sql = f"""
        INSERT INTO {tbl("messages")}
            (message_id, sender_id, receiver_id, content, timestamp, `read`)
        VALUES
            (@mid, @sid, @rid, @content, CURRENT_TIMESTAMP(), FALSE)
    """
    run_query(sql, [
        bigquery.ScalarQueryParameter("mid",     "STRING", message_id),
        bigquery.ScalarQueryParameter("sid",     "STRING", body["sender_id"]),
        bigquery.ScalarQueryParameter("rid",     "STRING", body["receiver_id"]),
        bigquery.ScalarQueryParameter("content", "STRING", body["content"].strip()),
    ])
    return jsonify({"message_id": message_id, "status": "sent"}), 201


@messages_bp.get("/<user_id>/unread-count")
def unread_count(user_id: str):
    """Quick badge count — how many unread messages does user_id have total."""
    sql = f"""
        SELECT COUNT(*) AS cnt
        FROM {tbl("messages")}
        WHERE receiver_id = @uid AND `read` = FALSE
    """
    rows = run_query(sql, [bigquery.ScalarQueryParameter("uid", "STRING", user_id)])
    return jsonify({"unread": rows[0]["cnt"] if rows else 0}), 200