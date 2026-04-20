"""
app.py — Sports Connect Flask Backend
======================================
Entry point for the Flask API service.

Registers all route blueprints and configures the app.
Run locally:
    FLASK_ENV=development python app.py
Run via gunicorn (production / Cloud Run):
    gunicorn --bind :8080 --workers 2 app:app
"""

import os
from flask import Flask
from flask_cors import CORS

from api.events    import events_bp
from api.messages  import messages_bp
from api.recommend import recommend_bp
from api.users     import users_bp
from api.health    import health_bp

def create_app() -> Flask:
    app = Flask(__name__)
    CORS(app)  # Streamlit lives on a different origin

    # Register blueprints
    app.register_blueprint(health_bp)
    app.register_blueprint(events_bp,    url_prefix="/api/events")
    app.register_blueprint(messages_bp,  url_prefix="/api/messages")
    app.register_blueprint(recommend_bp, url_prefix="/api/recommend")
    app.register_blueprint(users_bp,     url_prefix="/api/users")

    return app


app = create_app()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    debug = os.environ.get("FLASK_ENV") == "development"
    app.run(host="0.0.0.0", port=port, debug=debug)