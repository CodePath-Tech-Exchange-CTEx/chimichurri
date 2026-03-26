import streamlit as st
import pandas as pd
from collections import Counter
from datetime import datetime
 
def get_sport_icon(sport): # line written by Gemini
    icons = {
        "Soccer": "⚽",
        "Basketball": "🏀",
        "Volleyball": "🏐",
        "Tennis": "🎾"
    }
    return icons.get(sport, "🏃")
 
def display_map(user_location):
    st.subheader("📍 Nearby Courts and Fields")
    mock_fields = [
        {"name": "Central Park Soccer Field", "sport": "Soccer", "lat": user_location["lat"] + 0.002, "lon": user_location["lng"] + 0.002},
        {"name": "Downtown Basketball Court", "sport": "Basketball", "lat": user_location["lat"] - 0.0015, "lon": user_location["lng"] + 0.001},
        {"name": "Beach Volleyball Court", "sport": "Volleyball", "lat": user_location["lat"] + 0.001, "lon": user_location["lng"] - 0.002},
    ]
    st.map(pd.DataFrame(mock_fields), zoom=14)
    
    for field in mock_fields:
        with st.expander(f"{get_sport_icon(field['sport'])} {field['name']}"):
            st.write(f"This venue is ready for a **{field['sport']}** match!")
 
def display_session_summary(sessions): 
    st.subheader("📊 Your Stats") # line written by Gemini
    if not sessions:
        st.write("No data yet.")
        return
 
    total_hours = sum((s["end_time"] - s["start_time"]).total_seconds() / 3600 for s in sessions)
    sports = [s["sport"] for s in sessions]
    fav_sport = Counter(sports).most_common(1)[0][0]
    
    m1, m2 = st.columns(2)
    m1.metric("Total Sessions", len(sessions))
    m2.metric("Total Hours", f"{total_hours:.1f}h")
    
    summary_data = {
        "Stat": ["Favorite Sport", "Top Location"],
        "Value": [f"{get_sport_icon(fav_sport)} {fav_sport}", Counter([s["location"] for s in sessions]).most_common(1)[0][0]]
    }
    st.table(pd.DataFrame(summary_data))
 
def display_recent_games(sessions):
    st.subheader("🕒 Recent Activity") # line written by Gemini
    if not sessions:
        st.dataframe(pd.DataFrame(), use_container_width=True, hide_index=True)
        return
 
    recent_list = []
    for s in sessions[::-1]:
        # Handle both datetime objects and strings from BigQuery
        start = s["start_time"]
        end   = s["end_time"]
        if isinstance(start, str):
            start = datetime.fromisoformat(start)
        if isinstance(end, str):
            end = datetime.fromisoformat(end)
 
        duration = (end - start).total_seconds() / 3600
        recent_list.append({
            "Sport":    f"{get_sport_icon(s['sport'])} {s['sport']}",
            "Date":     start.strftime("%b %d"),
            "Duration": f"{duration:.1f}h" if duration > 0 else "N/A",
            "Location": s["location"]
        })
    st.dataframe(pd.DataFrame(recent_list), use_container_width=True, hide_index=True)
 
def display_personalized_recommendations(sessions, friends):
    st.subheader("🌟 Recommended") # line written by Gemini
 
    if not sessions:
        st.info("Complete some sessions to get personalized recommendations!")
        return
 
    fav_sport = Counter([s["sport"] for s in sessions]).most_common(1)[0][0]
    
    # Mock recommendation
    rec = {"name": "Sunday Soccer League", "sport": "Soccer", "loc": "Central Park"}
    
    st.info(f"**{rec['name']}**")
    rec_df = pd.DataFrame({
        "Detail": ["Sport", "Where", "Social"],
        "Info": [f"{get_sport_icon(rec['sport'])} {rec['sport']}", rec['loc'], f"👥 {len(friends)} friends active!"]
    })
    st.table(rec_df)