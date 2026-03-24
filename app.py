import streamlit as st
from datetime import datetime
from modules import (
    display_map,
    display_session_summary,
    display_recent_games,
    display_personalized_recommendations
)
import community_page
import activity_page

# --- CONFIG ---
st.set_page_config(page_title="Sports Connect", layout="wide")

# --- SESSION STATE ---
if "user_id" not in st.session_state:
    st.session_state["user_id"] = "user1"  # default user for now

# --- MOCK DATA ---
user_location = {"lat": 25.7617, "lng": -80.1918}
sessions = [
    {"sport": "Soccer", "location": "Central Park", "start_time": datetime(2026, 2, 20, 10, 0), "end_time": datetime(2026, 2, 20, 12, 0)},
    {"sport": "Soccer", "location": "Central Park", "start_time": datetime(2026, 2, 21, 14, 0), "end_time": datetime(2026, 2, 21, 15, 0)},
    {"sport": "Basketball", "location": "Downtown Court", "start_time": datetime(2026, 2, 22, 18, 0), "end_time": datetime(2026, 2, 22, 19, 30)}
]
friends = ["Carlos", "Jean"]

# --- SIDEBAR ---
with st.sidebar:
    st.title("🏆 Sports Connect")
    st.markdown("---")
    menu = st.radio("Menu", ["Home", "Activity", "Community", "Profile", "Find a Game", "Friends"], index=0)
    st.write(f"Currently viewing: **{menu}**")

# --- MAIN CONTENT ---
if menu == "Home":
    st.header("Sports Connect Unit 2.0 Demo")
    st.divider()
    left_col, right_col = st.columns([1, 1.2], gap="large")
    with left_col:
        display_session_summary(sessions)
        st.write("")
        display_recent_games(sessions)
        st.write("")
        display_personalized_recommendations(sessions, friends)
    with right_col:
        display_map(user_location)

elif menu == "Activity":
    activity_page.show_activity_page(st.session_state["user_id"])

elif menu == "Community":
    community_page.show_community_page(st.session_state["user_id"])

elif menu == "Profile":
    st.header("Profile")
    st.write("Profile page coming soon.")

elif menu == "Find a Game":
    st.header("Find a Game")
    st.write("Find a game page coming soon.")
