import streamlit as st
from datetime import datetime
from modules import (
    display_map,
    display_session_summary,
    display_recent_games,
    display_personalized_recommendations
)

st.title("Sports Connect - Unit 2 Demo")

# -----------------------------
# Mock Data
# -----------------------------

user_location = {"lat": 25.7617, "lng": -80.1918}

sessions = [
    {
        "sport": "Soccer",
        "location": "Central Park",
        "start_time": datetime(2026, 2, 20, 10, 0),
        "end_time": datetime(2026, 2, 20, 12, 0)
    },
    {
        "sport": "Soccer",
        "location": "Central Park",
        "start_time": datetime(2026, 2, 21, 14, 0),
        "end_time": datetime(2026, 2, 21, 15, 0)
    },
    {
        "sport": "Basketball",
        "location": "Downtown Court",
        "start_time": datetime(2026, 2, 22, 18, 0),
        "end_time": datetime(2026, 2, 22, 19, 30)
    }
]

friends = ["Carlos", "Jean"]

# -----------------------------
# Display Functions
# -----------------------------

display_map(user_location)

st.divider()

display_session_summary(sessions)

st.divider()

display_recent_games(sessions)

st.divider()

display_personalized_recommendations(sessions, user_location, friends)