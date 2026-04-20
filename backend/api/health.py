"""
api/health.py
Health check endpoint — Cloud Run uses this to verify the service is alive.
"""

from flask import Blueprint, jsonify

health_bp = Blueprint("health", __name__)


@health_bp.get("/healthz")
def health():
    return jsonify({"status": "ok"}), 200