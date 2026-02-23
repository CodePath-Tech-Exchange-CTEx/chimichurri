import streamlit as st
from datetime import datetime
from modules import (
    display_map,
    display_session_summary,
    display_recent_games,
    display_personalized_recommendations
)

# --- CONFIG ---
st.set_page_config(page_title="Sports Connect", layout="wide") # line written by Gemini

# --- MOCK DATA ---
user_location = {"lat": 25.7617, "lng": -80.1918}
sessions = [
    {"sport": "Soccer", "location": "Central Park", "start_time": datetime(2026, 2, 20, 10, 0), "end_time": datetime(2026, 2, 20, 12, 0)},
    {"sport": "Soccer", "location": "Central Park", "start_time": datetime(2026, 2, 21, 14, 0), "end_time": datetime(2026, 2, 21, 15, 0)},
    {"sport": "Basketball", "location": "Downtown Court", "start_time": datetime(2026, 2, 22, 18, 0), "end_time": datetime(2026, 2, 22, 19, 30)}
]
friends = ["Carlos", "Jean"]

# --- SIDEBAR ---
with st.sidebar: # line written by Gemini
    st.title("🏆 Sports Connect")
    st.markdown("---")
    # Added "Home" as default index
    menu = st.radio("Menu", ["Home", "Profile", "Find a Game", "Friends"], index=0)
    st.write(f"Currently viewing: **{menu}**")

# --- HEADER ---
st.header("Sports Connect Unit 2.0 Demo")
st.divider()

# --- MAIN CONTENT LAYOUT ---
# We split into two main columns (Left 45%, Right 55%)
left_col, right_col = st.columns([1, 1.2], gap="large") # line written by Gemini

with left_col:
    # 1. Stats Section
    display_session_summary(sessions)
    st.write("") # Spacer

    # 2. Recent Activity Section
    display_recent_games(sessions)
    st.write("") # Spacer

    # 3. Recommendations Section
    display_personalized_recommendations(sessions, friends)

with right_col:
    # 4. Map Section
    display_map(user_location)