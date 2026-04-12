import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from collections import Counter
import data_fetcher

# ── PAGE CONFIG & CUSTOM CSS ──
st.set_page_config(
    page_title="Sports Connect",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items=None
)

# Custom CSS for dark theme with green accents
st.markdown("""
<style>
    :root {
        --primary-green: #22c55e;
        --bg-dark: #111;
        --bg-secondary: #1a1a1a;
        --bg-tertiary: #1e1e1e;
        --text-primary: #fff;
        --text-secondary: #888;
        --border-color: #222;
    }
    
    * {
        margin: 0;
        padding: 0;
    }
    
    html, body, [data-testid="stAppViewContainer"] {
        background-color: var(--bg-dark);
        color: var(--text-primary);
    }
    
    [data-testid="stSidebar"] {
        background-color: var(--bg-secondary);
        border-right: 1px solid var(--border-color);
    }
    
    [data-testid="stSidebar"] [data-testid="stVerticalBlockBorderContainer"] {
        background-color: var(--bg-secondary);
    }
    
    .sidebar-content {
        padding: 16px 12px;
    }
    
    .logo-section {
        display: flex;
        align-items: center;
        gap: 9px;
        margin-bottom: 20px;
        padding: 8px 0;
    }
    
    .logo-mark {
        width: 30px;
        height: 30px;
        background-color: var(--primary-green);
        border-radius: 7px;
        display: flex;
        align-items: center;
        justify-content: center;
        font-size: 11px;
        font-weight: 700;
        color: #000;
    }
    
    .logo-text {
        font-size: 13px;
        font-weight: 500;
        color: var(--text-primary);
    }
    
    .nav-section {
        margin: 20px 0 0 0;
    }
    
    .nav-item {
        display: flex;
        align-items: center;
        gap: 10px;
        padding: 9px 10px;
        border-radius: 8px;
        font-size: 13px;
        cursor: pointer;
        transition: all 0.15s;
        margin-bottom: 2px;
        border-left: 2px solid transparent;
        color: var(--text-secondary);
    }
    
    .nav-item:hover {
        background-color: var(--bg-tertiary);
        color: #ccc;
    }
    
    .nav-item.active {
        background-color: #1e2e1e;
        color: var(--text-primary);
        border-left-color: var(--primary-green);
    }
    
    .user-section {
        margin-top: auto;
        display: flex;
        align-items: center;
        gap: 8px;
        padding: 8px 6px;
        border-radius: 8px;
        cursor: pointer;
        transition: background-color 0.15s;
    }
    
    .user-section:hover {
        background-color: var(--bg-tertiary);
    }
    
    .user-avatar {
        width: 32px;
        height: 32px;
        border-radius: 50%;
        background-color: #1e3a2a;
        display: flex;
        align-items: center;
        justify-content: center;
        font-size: 11px;
        font-weight: 500;
        color: var(--primary-green);
        flex-shrink: 0;
    }
    
    .user-info {
        flex: 1;
        min-width: 0;
    }
    
    .user-name {
        font-size: 12px;
        color: #ccc;
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
    }
    
    .user-email {
        font-size: 11px;
        color: var(--text-secondary);
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
    }
    
    /* Main content styling */
    [data-testid="stMainBlockContainer"] {
        background-color: var(--bg-dark);
    }
    
    h1, h2, h3, h4, h5, h6 {
        color: var(--text-primary);
    }
    
    .page-title {
        font-size: 20px;
        font-weight: 500;
        color: var(--text-primary);
        margin-bottom: 12px;
        margin-top: 0;
    }
    
    /* Card styling */
    [data-testid="stMetricContainer"] {
        background-color: var(--bg-tertiary);
        border: 0.5px solid #2a2a2a;
        border-radius: 10px;
        padding: 14px;
    }
    
    .welcome-card {
        background-color: #1e2e1e;
        border: 0.5px solid rgba(34, 197, 94, 0.2);
        border-radius: 12px;
        padding: 20px;
        text-align: center;
        margin-bottom: 16px;
    }
    
    .welcome-card h2 {
        font-size: 18px;
        margin-bottom: 8px;
    }
    
    .welcome-card p {
        font-size: 13px;
        color: var(--text-secondary);
        margin-bottom: 12px;
    }
    
    .tip-card {
        background-color: #1e2e1e;
        border: 0.5px solid rgba(34, 197, 94, 0.3);
        border-radius: 10px;
        padding: 16px;
        margin-bottom: 16px;
    }
    
    .tip-card-title {
        font-size: 12px;
        color: var(--primary-green);
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        margin-bottom: 8px;
    }
    
    .tip-card-content {
        font-size: 13px;
        color: #ccc;
        line-height: 1.6;
    }
    
    /* Event card styling */
    .event-card {
        background-color: var(--bg-tertiary);
        border: 0.5px solid #2a2a2a;
        border-radius: 10px;
        border-left: 3px solid var(--primary-green);
        padding: 14px;
        margin-bottom: 10px;
    }
    
    .card-sport {
        display: flex;
        align-items: center;
        gap: 7px;
        font-size: 14px;
        font-weight: 500;
        color: var(--text-primary);
        margin-bottom: 8px;
    }
    
    .card-venue {
        font-size: 13px;
        color: var(--text-primary);
        margin-bottom: 2px;
    }
    
    .card-address {
        font-size: 11px;
        color: #666;
        margin-bottom: 6px;
        margin-left: 17px;
    }
    
    .card-time {
        font-size: 12px;
        color: var(--text-secondary);
        margin-bottom: 8px;
    }
    
    .card-meta {
        display: flex;
        align-items: center;
        gap: 8px;
        flex-wrap: wrap;
        font-size: 12px;
        color: #ccc;
    }
    
    .skill-pill {
        font-size: 10px;
        padding: 2px 8px;
        border-radius: 10px;
        background-color: #2a2a2a;
        color: var(--text-secondary);
    }
    
    /* Search bar */
    [data-testid="stTextInput"] input {
        background-color: var(--bg-tertiary) !important;
        border: 0.5px solid #2a2a2a !important;
        color: var(--text-primary) !important;
        border-radius: 8px !important;
    }
    
    /* Buttons */
    .stButton > button {
        background-color: var(--primary-green);
        color: #000;
        font-weight: 500;
        border: none;
        border-radius: 8px;
        padding: 7px 16px;
        font-size: 13px;
    }
    
    .stButton > button:hover {
        background-color: #16a34a;
    }
    
    /* Tables */
    [data-testid="stDataFrame"] {
        background-color: var(--bg-secondary);
        border: 0.5px solid #222;
        border-radius: 10px;
    }
    
    /* Expander */
    [data-testid="stExpander"] {
        background-color: var(--bg-tertiary);
        border: 0.5px solid #2a2a2a;
        border-radius: 10px;
    }
    
    /* Selectbox and multiselect */
    [data-testid="stSelectbox"] > div > div,
    [data-testid="stMultiSelect"] > div > div {
        background-color: var(--bg-tertiary);
        border: 0.5px solid #2a2a2a;
        border-radius: 8px;
    }
    
    /* Divider */
    hr {
        border-color: var(--border-color);
    }
    
    /* Section labels */
    .section-label {
        font-size: 12px;
        color: #666;
        font-weight: 500;
        margin-bottom: 8px;
        margin-top: 4px;
        text-transform: uppercase;
        letter-spacing: 0.06em;
    }
    
    .stat-label {
        font-size: 12px;
        color: var(--text-secondary);
    }
    
    /* Metrics */
    .metric-value {
        font-size: 26px;
        font-weight: 500;
        color: var(--text-primary);
    }
</style>
""", unsafe_allow_html=True)

# ── SESSION STATE ──
if "user_id" not in st.session_state:
    st.session_state["user_id"] = "user1"

if "current_page" not in st.session_state:
    st.session_state["current_page"] = "home"

if "joined_events" not in st.session_state:
    st.session_state["joined_events"] = set()

# ── MOCK DATA ──
SPORTS = ["Soccer", "Basketball", "Volleyball", "Tennis"]

MOCK_EVENTS = [
    {
        "id": "e1",
        "sport": "Soccer",
        "emoji": "⚽",
        "venue": "Central Park Field 3",
        "address": "123 Park Ave, Manhattan",
        "time": "Sat Mar 28 · 10:00–12:00",
        "duration": "2h",
        "joined": 6,
        "total": 10,
        "skill": "Intermediate",
        "status": "Open",
        "dist": 1.2,
        "color": "#22c55e"
    },
    {
        "id": "e2",
        "sport": "Basketball",
        "emoji": "🏀",
        "venue": "Downtown Court",
        "address": "456 Main St, Brooklyn",
        "time": "Sun Mar 29 · 14:00–16:00",
        "duration": "2h",
        "joined": 8,
        "total": 10,
        "skill": "Advanced",
        "status": "Open",
        "dist": 2.8,
        "color": "#f97316"
    },
    {
        "id": "e3",
        "sport": "Volleyball",
        "emoji": "🏐",
        "venue": "Beach Arena",
        "address": "789 Beach Rd, Queens",
        "time": "Mon Mar 30 · 18:00–21:00",
        "duration": "3h",
        "joined": 10,
        "total": 10,
        "skill": "Beginner",
        "status": "Full",
        "dist": 4.1,
        "color": "#a78bfa"
    },
    {
        "id": "e4",
        "sport": "Tennis",
        "emoji": "🎾",
        "venue": "City Club",
        "address": "22 Club Dr, Manhattan",
        "time": "Tue Mar 31 · 09:00–10:30",
        "duration": "1.5h",
        "joined": 3,
        "total": 4,
        "skill": "Advanced",
        "status": "Open",
        "dist": 3.5,
        "color": "#facc15"
    },
]

MOCK_FRIENDS = [
    {"name": "Alex Johnson", "initials": "AJ", "sport": "Soccer", "emoji": "⚽", "online": True},
    {"name": "Maria Garcia", "initials": "MG", "sport": "Basketball", "emoji": "🏀", "online": True},
    {"name": "David Kim", "initials": "DK", "sport": "Tennis", "emoji": "🎾", "online": False},
    {"name": "Sarah Williams", "initials": "SW", "sport": "Volleyball", "emoji": "🏐", "online": True},
]

# ── SIDEBAR NAVIGATION ──
with st.sidebar:
    st.markdown("""
    <div class="sidebar-content">
        <div class="logo-section">
            <div class="logo-mark">SC</div>
            <div class="logo-text">Sports Connect</div>
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("---")
    
    pages = ["Home", "Find a Game", "Messages", "Activity", "Profile"]
    selected = st.radio("Navigation", pages, index=0, label_visibility="collapsed")
    
    st.markdown("---")
    st.markdown("""
    <div class="user-section">
        <div class="user-avatar">CM</div>
        <div class="user-info">
            <div class="user-name">Carlos Martinez</div>
            <div class="user-email">carlos@email.com</div>
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    st.session_state["current_page"] = selected.lower().replace(" ", "_")

# ── HELPER FUNCTIONS ──
def get_sport_icon(sport):
    icons = {"Soccer": "⚽", "Basketball": "🏀", "Volleyball": "🏐", "Tennis": "🎾"}
    return icons.get(sport, "🏃")

def render_event_card(event):
    """Render a single event card"""
    is_joined = event["id"] in st.session_state["joined_events"]
    is_full = event["status"] == "Full"
    spots = event["total"] - event["joined"]
    
    col1, col2 = st.columns([4, 1])
    with col1:
        st.markdown(f"<div class='card-sport'><span style='font-size:17px'>{event['emoji']}</span>{event['sport']}</div>", unsafe_allow_html=True)
        st.markdown(f"<div class='card-venue'>📍 {event['venue']}</div>", unsafe_allow_html=True)
        st.markdown(f"<div class='card-address'>{event['address']}</div>", unsafe_allow_html=True)
        st.markdown(f"<div class='card-time'>📅 {event['time']} · {event['duration']}</div>", unsafe_allow_html=True)
        
        col_meta1, col_meta2, col_meta3 = st.columns([1, 1, 1])
        with col_meta1:
            st.markdown(f"<div class='card-meta'>👥 {event['joined']}/{event['total']}</div>", unsafe_allow_html=True)
        with col_meta2:
            st.markdown(f"<span class='skill-pill'>{event['skill']}</span>", unsafe_allow_html=True)
        with col_meta3:
            st.markdown(f"<div class='card-meta'>{event['dist']} km</div>", unsafe_allow_html=True)
    
    with col2:
        if is_full:
            st.button("Full", disabled=True, use_container_width=True, key=f"btn_{event['id']}")
        elif is_joined:
            if st.button("Cancel", use_container_width=True, key=f"cancel_{event['id']}"):
                st.session_state["joined_events"].discard(event["id"])
                st.rerun()
        else:
            if st.button("Join", use_container_width=True, key=f"join_{event['id']}"):
                st.session_state["joined_events"].add(event["id"])
                st.rerun()

def display_map(user_location):
    """Display map with nearby courts and fields"""
    st.subheader("📍 Nearby Courts and Fields")
    mock_fields = [
        {"name": "Central Park Soccer Field", "sport": "Soccer", "lat": user_location["lat"] + 0.002, "lon": user_location["lng"] + 0.002},
        {"name": "Downtown Basketball Court", "sport": "Basketball", "lat": user_location["lat"] - 0.0015, "lon": user_location["lng"] + 0.001},
        {"name": "Beach Volleyball Court", "sport": "Volleyball", "lat": user_location["lat"] + 0.001, "lon": user_location["lng"] - 0.002},
    ]
    st.map(pd.DataFrame(mock_fields), zoom=14)

def get_daily_tip(user_id):
    """Fetch daily motivational tip from Claude AI via data_fetcher"""
    try:
        tip = data_fetcher.get_genai_advice(user_id)
        return tip
    except Exception as e:
        return f"Keep pushing your limits and connecting with fellow athletes! 💪"

# ── PAGES ──
page = st.session_state["current_page"]

if page == "home":
    st.markdown("<h1 class='page-title'>Home</h1>", unsafe_allow_html=True)
    
    st.markdown("""
    <div class="welcome-card">
        <h2>Welcome back, Carlos! 👋</h2>
        <p>Ready to find your next game? Browse upcoming events or invite your friends to play.</p>
    </div>
    """, unsafe_allow_html=True)
    
    col1, col2, col3 = st.columns(3)
    with col1:
        if st.button("🎯 Find a Game", use_container_width=True):
            st.session_state["current_page"] = "find_a_game"
            st.rerun()
    with col2:
        if st.button("💬 Messages", use_container_width=True):
            st.session_state["current_page"] = "messages"
            st.rerun()
    with col3:
        if st.button("📊 Activity", use_container_width=True):
            st.session_state["current_page"] = "activity"
            st.rerun()
    
    # Daily Tip Section
    st.markdown("<div class='section-label'>💡 Daily Tip</div>", unsafe_allow_html=True)
    with st.spinner("Loading your personalized tip..."):
        daily_tip = get_daily_tip(st.session_state["user_id"])
    st.markdown(f"""
    <div class="tip-card">
        <div class="tip-card-title">🌟 Today's Motivation</div>
        <div class="tip-card-content">{daily_tip}</div>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("<div class='section-label'>⚽ Upcoming Games</div>", unsafe_allow_html=True)
    for event in MOCK_EVENTS[:3]:
        render_event_card(event)

elif page == "find_a_game":
    st.markdown("<h1 class='page-title'>Find a Game</h1>", unsafe_allow_html=True)
    
    search = st.text_input("Search sport, venue, or location…", placeholder="Search…")
    st.markdown("---")
    
    # Two-column layout: events on left, map on right
    col_events, col_map = st.columns([1.5, 1])
    
    with col_events:
        st.markdown("<div class='section-label'>Available Games</div>", unsafe_allow_html=True)
        
        filtered_events = MOCK_EVENTS
        if search:
            filtered_events = [e for e in MOCK_EVENTS if search.lower() in e["venue"].lower() or search.lower() in e["sport"].lower()]
        
        if not filtered_events:
            st.info("No games match your filters")
        else:
            for event in filtered_events:
                render_event_card(event)
    
    with col_map:
        # Display map with default location
        user_location = {"lat": 25.7617, "lng": -80.1918}
        display_map(user_location)

elif page == "messages":
    st.markdown("<h1 class='page-title'>Messages</h1>", unsafe_allow_html=True)
    
    search = st.text_input("Search players by name or sport…", placeholder="Search…")
    
    st.markdown("---")
    st.markdown("<div class='section-label'>Your Friends</div>", unsafe_allow_html=True)
    
    filtered_friends = MOCK_FRIENDS
    if search:
        filtered_friends = [f for f in MOCK_FRIENDS if search.lower() in f["name"].lower() or search.lower() in f["sport"].lower()]
    
    for friend in filtered_friends:
        col1, col2, col3 = st.columns([3, 2, 1])
        with col1:
            status = "🟢 Online" if friend["online"] else "⚫ Offline"
            st.markdown(f"**{friend['name']}** ({status})")
            st.caption(f"{friend['emoji']} {friend['sport']}")
        with col2:
            st.button("Message", key=f"msg_{friend['name']}", use_container_width=True)
        with col3:
            st.button("Invite", key=f"inv_{friend['name']}", use_container_width=True)

elif page == "activity":
    st.markdown("<h1 class='page-title'>Activity</h1>", unsafe_allow_html=True)
    
    st.markdown("<div class='section-label'>Statistics</div>", unsafe_allow_html=True)
    
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Sessions", "48", delta=None)
    with col2:
        st.metric("Total Hours", "96", delta=None)
    with col3:
        st.metric("Friends", "24", delta=None)
    
    st.markdown("---")
    st.markdown("<div class='section-label'>Activity History</div>", unsafe_allow_html=True)
    
    activity_data = {
        "Sport": ["⚽ Soccer", "🏀 Basketball", "🏐 Volleyball", "🎾 Tennis"],
        "Type": ["Joined", "Joined", "Created", "Joined"],
        "Venue": ["Central Park", "Downtown Court", "Beach Arena", "City Club"],
        "Date": ["Mar 25, 2026", "Mar 23, 2026", "Mar 20, 2026", "Mar 18, 2026"]
    }
    st.dataframe(pd.DataFrame(activity_data), use_container_width=True, hide_index=True)

elif page == "profile":
    st.markdown("<h1 class='page-title'>Profile</h1>", unsafe_allow_html=True)
    
    st.markdown("""
    <div style="background-color: #1e1e1e; border: 0.5px solid #2a2a2a; border-radius: 12px; padding: 18px; margin-bottom: 24px;">
        <div style="display: flex; gap: 14px; align-items: flex-start; flex-wrap: wrap;">
            <div style="width: 56px; height: 56px; border-radius: 50%; background-color: #1e3a2a; display: flex; align-items: center; justify-content: center; font-size: 18px; font-weight: 500; color: #22c55e; flex-shrink: 0;">CM</div>
            <div style="flex: 1; min-width: 0;">
                <div style="font-size: 17px; font-weight: 500; color: #fff; margin-bottom: 3px;">Carlos Martinez</div>
                <div style="font-size: 13px; color: #666; margin-bottom: 10px;">carlos.martinez@email.com</div>
                <div style="display: flex; gap: 8px; flex-wrap: wrap;">
                    <span style="font-size: 12px; padding: 4px 10px; border-radius: 20px; background-color: #2a2a2a; border: 0.5px solid #333; color: #ccc;">⚽ Soccer · Intermediate</span>
                    <span style="font-size: 12px; padding: 4px 10px; border-radius: 20px; background-color: #2a2a2a; border: 0.5px solid #333; color: #ccc;">🏀 Basketball · Beginner</span>
                </div>
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("---")
    st.markdown("<div class='section-label'>Settings</div>", unsafe_allow_html=True)
    
    col1, col2 = st.columns([3, 1])
    with col1:
        st.markdown("**Notifications**")
        st.caption("Game invites, messages, reminders")
    with col2:
        st.toggle("", value=True, key="notif_toggle")
    
    col1, col2 = st.columns([3, 1])
    with col1:
        st.markdown("**Location Services**")
        st.caption("Used for nearby game discovery")
    with col2:
        st.toggle("", value=True, key="loc_toggle")
    
    col1, col2 = st.columns([3, 1])
    with col1:
        st.markdown("**Dark Mode**")
        st.caption("Always on dark theme")
    with col2:
        st.toggle("", value=True, key="dark_toggle")
    
    st.markdown("---")
    if st.button("Sign Out", use_container_width=True):
        st.info("Signed out successfully")