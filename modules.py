import streamlit as st
from collections import Counter
from datetime import datetime
import math


# ---------------------------------------------------
#  DISPLAY MAP
# ---------------------------------------------------

def display_map(user_location: dict):
    """
    Displays a simple map with nearby mock sports fields/courts.

    Args:
        user_location (dict): {"lat": float, "lng": float}
    """

    st.subheader("Nearby Sports Fields")

    # Mock nearby fields (since no DB integration yet)
    mock_fields = [
        {"name": "Central Park Soccer Field", "sport": "Soccer", "lat": user_location["lat"] + 0.002, "lng": user_location["lng"] + 0.002},
        {"name": "Downtown Basketball Court", "sport": "Basketball", "lat": user_location["lat"] - 0.0015, "lng": user_location["lng"] + 0.001},
        {"name": "Beach Volleyball Court", "sport": "Volleyball", "lat": user_location["lat"] + 0.001, "lng": user_location["lng"] - 0.002},
    ]

    st.map([{"lat": field["lat"], "lon": field["lng"]} for field in mock_fields])

    for field in mock_fields:
        st.write(f"**{field['name']}** — {field['sport']}")
    

# ---------------------------------------------------
#  DISPLAY SESSION SUMMARY
# ---------------------------------------------------

def display_session_summary(sessions: list):
    """
    Displays:
    - Total sessions played
    - Total hours
    - Favorite sport
    - Most played location
    """

    st.subheader("Session Summary")

    if not sessions:
        st.write("No sessions available.")
        return

    total_sessions = len(sessions)

    total_hours = 0
    sports = []
    locations = []

    for session in sessions:
        start = session.get("start_time")
        end = session.get("end_time")

        if isinstance(start, datetime) and isinstance(end, datetime):
            duration = (end - start).total_seconds() / 3600
            total_hours += duration

        sports.append(session.get("sport"))
        locations.append(session.get("location"))

    favorite_sport = Counter(sports).most_common(1)[0][0]
    most_played_location = Counter(locations).most_common(1)[0][0]

    st.metric("Total Sessions", total_sessions)
    st.metric("Total Hours", round(total_hours, 2))
    st.write("Favorite Sport", favorite_sport)
    st.write("Most Played Location", most_played_location)

# ---------------------------------------------------
#  DISPLAY RECENT GAMES
# ---------------------------------------------------

def display_recent_games(sessions: list):
    """
    Displays recent games including:
    - Duration
    - Location
    - Date
    - Sport
    """

    st.subheader("Recent Games")

    if not sessions:
        st.write("No recent games.")
        return

    for session in sessions:
        sport = session.get("sport")
        location = session.get("location")
        start = session.get("start_time")
        end = session.get("end_time")

        duration_hours = 0
        if isinstance(start, datetime) and isinstance(end, datetime):
            duration_hours = (end - start).total_seconds() / 3600

        date_str = start.strftime("%Y-%m-%d") if isinstance(start, datetime) else "Unknown date" # Line written by ChatGPT

        st.write("------") # Line written by ChatGPT
        st.write(f"Sport: {sport}") # Line written by ChatGPT
        st.write(f"Location: {location}") # Line written by ChatGPT
        st.write(f"Date: {date_str}") # Line written by ChatGPT
        st.write(f"Duration (hrs): {round(duration_hours, 2)}") # Line written by ChatGPT


# ---------------------------------------------------
#  DISPLAY PERSONALIZED RECOMMENDATIONS
# ---------------------------------------------------

def display_personalized_recommendations(user_history: list, user_location: dict, friends: list):
    """
    Displays a recommended event including:
    - Event name
    - Sport
    - Location
    - Explanation for recommendation
    """

    st.subheader("Recommended For You")

    if not user_history:
        st.write("No history available for recommendations.")
        return

    # Determine most played sport
    sports = [session.get("sport") for session in user_history]
    favorite_sport = Counter(sports).most_common(1)[0][0]

    # Mock events
    mock_events = [
        {"name": "Sunday Soccer League", "sport": "Soccer", "location": "Central Park"},
        {"name": "Evening Basketball Run", "sport": "Basketball", "location": "Downtown Court"},
        {"name": "Beach Volleyball Meetup", "sport": "Volleyball", "location": "South Beach"},
    ]

    # Select event matching favorite sport
    recommended_event = next((event for event in mock_events if event["sport"] == favorite_sport),mock_events[0])

    reason = f"Recommended because you frequently play {favorite_sport}."

    if friends:
        reason += f" {len(friends)} of your friends are active on the platform."

    st.write(f"Event: {recommended_event['name']}")
    st.write(f"Sport: {recommended_event['sport']}")
    st.write(f"Location: {recommended_event['location']}")
    st.write(f"Why this recommendation: {reason}")