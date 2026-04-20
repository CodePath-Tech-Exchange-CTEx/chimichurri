import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from collections import Counter
import local_data as data_fetcher
import data_fetcher as real_db

USER_LAT, USER_LNG = 25.7617, -80.1918
MOCK_EVENTS  = data_fetcher.get_events_for_ui(USER_LAT, USER_LNG)
MOCK_FRIENDS = data_fetcher.get_friends_for_ui(data_fetcher.CURRENT_USER_ID)

# Ensure there are at least 3 friends for testing the messaging feature
if len(MOCK_FRIENDS) < 3:
    MOCK_FRIENDS.extend([
        {"name": "Alice Smith", "initials": "AS", "sport": "Soccer", "emoji": "⚽", "online": True, "user_id": "mock_1"},
        {"name": "Bob Johnson", "initials": "BJ", "sport": "Basketball", "emoji": "🏀", "online": False, "user_id": "mock_2"},
        {"name": "Charlie Davis", "initials": "CD", "sport": "Tennis", "emoji": "🎾", "online": True, "user_id": "mock_3"}
    ])

# Use the generated user instead of hardcoded "Carlos Martinez"
if "user_id" not in st.session_state:
    st.session_state["user_id"] = data_fetcher.CURRENT_USER_ID

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
    [data-testid="stSidebarNav"] {
        display: none !important;
    }
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

if "radius_km" not in st.session_state:
    st.session_state["radius_km"] = 5.0

if "show_invite_dialog" not in st.session_state:
    st.session_state["show_invite_dialog"] = None


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

    pages = ["Home", "Find a Game", "Messages", "Activity"]
    
    current_page = st.session_state.get("current_page", "home")
    page_ids = [p.lower().replace(" ", "_") for p in pages]
    idx = page_ids.index(current_page) if current_page in page_ids else None
    
    selected = st.radio("Navigation", pages, index=idx, label_visibility="collapsed")
    
    if selected:
        selected_id = selected.lower().replace(" ", "_")
        if selected_id != current_page:
            st.session_state["current_page"] = selected_id
            st.rerun()

    st.markdown("---")
    col_user, col_gear = st.columns([4, 1])
    with col_user:
        st.markdown("""
        <div class="user-section">
            <div class="user-avatar">CM</div>
            <div class="user-info">
                <div class="user-name">Carlos Martinez</div>
                <div class="user-email">carlos@email.com</div>
            </div>
        </div>
        """, unsafe_allow_html=True)
    with col_gear:
        if st.button("⚙️", help="Settings / Profile", key="settings_btn"):
            st.session_state["current_page"] = "settings"
            st.rerun()

# ── HELPER FUNCTIONS ──
def get_sport_icon(sport):
    icons = {"Soccer": "⚽", "Basketball": "🏀", "Volleyball": "🏐", "Tennis": "🎾"}
    return icons.get(sport, "🏃")

def render_event_card(event):
    """Render a single event card"""
    is_joined = event["id"] in st.session_state["joined_events"]

    # If the user joined, reflect that in the displayed count
    display_joined = event["joined"] + (1 if is_joined else 0)
    spots_left = event["total"] - display_joined

    is_full = spots_left <= 0

    col1, col2 = st.columns([4, 1])
    with col1:
        st.markdown(f"<div class='card-sport'><span style='font-size:17px'>{event['emoji']}</span>{event['sport']}</div>", unsafe_allow_html=True)
        st.markdown(f"<div class='card-venue'>📍 {event['venue']}</div>", unsafe_allow_html=True)
        st.markdown(f"<div class='card-address'>{event['address']}</div>", unsafe_allow_html=True)
        st.markdown(f"<div class='card-time'>📅 {event['time']} · {event['duration']}</div>", unsafe_allow_html=True)

        col_meta1, col_meta2, col_meta3 = st.columns([1, 1, 1])
        with col_meta1:
            # Show updated player count in green if user joined
            count_color = "#22c55e" if is_joined else "#ccc"
            st.markdown(
                f"<div class='card-meta' style='color:{count_color}'>👥 {display_joined}/{event['total']}</div>",
                unsafe_allow_html=True
            )
        with col_meta2:
            st.markdown(f"<span class='skill-pill'>{event['skill']}</span>", unsafe_allow_html=True)
        with col_meta3:
            st.markdown(f"<div class='card-meta'>{event['dist']} km</div>", unsafe_allow_html=True)

    with col2:
        if is_full and not is_joined:
            st.button("Full", disabled=True, use_container_width=True, key=f"btn_{event['id']}")
        elif is_joined:
            if st.button("✓ Joined", use_container_width=True, key=f"cancel_{event['id']}",
                         help="Click to cancel"):
                st.session_state["joined_events"].discard(event["id"])
                st.toast(f"You left {event['venue']}", icon="👋")
                st.rerun()
        else:
            if spots_left <= 2 and spots_left > 0:
                st.caption(f"⚠️ {spots_left} spot{'s' if spots_left > 1 else ''} left")
            if st.button("Join", use_container_width=True, key=f"join_{event['id']}"):
                st.session_state["joined_events"].add(event["id"])
                st.session_state["show_invite_dialog"] = event["venue"]
                st.toast(f"You joined {event['venue']}! 🎉", icon="✅")
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

@st.dialog("Invite Friends")
def invite_friends_dialog(venue_name):
    st.write(f"You just joined **{venue_name}**! Would you like to invite your friends to play?")
    col1, col2 = st.columns(2)
    with col1:
        if st.button("Invite Friends", use_container_width=True, type="primary"):
            st.session_state["current_page"] = "messages"
            st.session_state["show_invite_dialog"] = None
            st.rerun()
    with col2:
        if st.button("Maybe Later", use_container_width=True):
            st.session_state["show_invite_dialog"] = None
            st.rerun()

# ── PAGES ──
page = st.session_state["current_page"]

if st.session_state["show_invite_dialog"]:
    invite_friends_dialog(st.session_state["show_invite_dialog"])

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
    
    all_sports = ["All"] + sorted(list(set(e["sport"] for e in MOCK_EVENTS)))
    selected_sport = st.selectbox("Filter by Game", all_sports)
    
    st.markdown("---")

    col_map, col_events = st.columns([1, 1.5])

    with col_map:
        st.markdown("<div class='section-label'>📍 Search Radius</div>", unsafe_allow_html=True)
        st.session_state["radius_km"] = st.slider(
            "Radius (km)", min_value=1.0, max_value=20.0,
            value=st.session_state["radius_km"], step=0.5,
            label_visibility="collapsed",
        )
        st.caption(f"Showing games within **{st.session_state['radius_km']} km**")
        st.markdown("---")
        user_location = {"lat": 25.7617, "lng": -80.1918}
        display_map(user_location)
        st.caption("📌 Map shows all venues. Use the radius slider to filter the game list.")

    with col_events:
        radius = st.session_state["radius_km"]
        st.markdown(
            f"<div class='section-label'>Games within {radius} km</div>",
            unsafe_allow_html=True
        )

        # Apply radius filter first, then text search
        filtered_events = [e for e in MOCK_EVENTS if e["dist"] <= radius]
        if search:
            filtered_events = [
                e for e in filtered_events
                if search.lower() in e["venue"].lower()
                or search.lower() in e["sport"].lower()
            ]
            
        if selected_sport != "All":
            filtered_events = [e for e in filtered_events if e["sport"] == selected_sport]

        if not filtered_events:
            st.info(f"No games found within {radius} km. Try increasing the radius in the sidebar.")
        else:
            for event in filtered_events:
                render_event_card(event)

elif page == "messages":
    st.markdown("<h1 class='page-title'>Messages</h1>", unsafe_allow_html=True)

    user_id = st.session_state["user_id"]

    if "active_chat" not in st.session_state:
        st.session_state["active_chat"] = None  # stores friend's user_id

    # Load real friends from database
    try:
        raw_friends = real_db.get_friends(user_id)

    except Exception as e:
        st.error(f"Could not load friends: {e}")
        raw_friends = []

    # Convert to UI format
    sport_emoji = {
        "Soccer": "⚽", "Basketball": "🏀", "Volleyball": "🏐",
        "Tennis": "🎾", "Baseball": "⚾", "Pickleball": "🏓",
    }
    friends = []
    for f in raw_friends:
        email = f.get("email", "")
        name = email.split("@")[0].replace(".", " ").title() if email else f["friend_id"][:8]
        friends.append({
            "user_id": f["friend_id"],
            "name":    name,
            "email":   email,
        })

    list_col, chat_col = None, None
    if st.session_state["active_chat"]:
        list_col, chat_col = st.columns([1, 1.5])
    else:
        list_col = st.container()

    with list_col:
        search = st.text_input("Search friends…", placeholder="Search…")
        st.markdown("---")
        st.markdown("<div class='section-label'>Your Friends</div>", unsafe_allow_html=True)

        filtered_friends = friends
        if search:
            filtered_friends = [
                f for f in friends
                if search.lower() in f["name"].lower()
                or search.lower() in f["email"].lower()
            ]

        if not filtered_friends:
            st.info("No friends yet. Find a game and connect with other players!")

        for friend in filtered_friends:
            col1, col2, col3 = st.columns([3, 2, 1])
            with col1:
                st.markdown(f"**{friend['name']}**")
                st.caption(friend["email"])
            with col2:
                if st.button("Message", key=f"msg_{friend['user_id']}", use_container_width=True):
                    st.session_state["active_chat"] = friend["user_id"]
                    st.rerun()
            with col3:
                if st.button("Invite", key=f"inv_{friend['user_id']}", use_container_width=True):
                    try:
                        real_db.send_message(
                            user_id,
                            friend["user_id"],
                            "Hey! I just joined a game. Do you want to come along?"
                        )
                        st.toast(f"Invitation sent to {friend['name']}!", icon="📩")
                    except Exception as e:
                        st.error(f"Could not send invite: {e}")

    if st.session_state["active_chat"]:
        with chat_col:
            active_friend_id = st.session_state["active_chat"]

            # Find friend name for display
            active_friend_name = next(
                (f["name"] for f in friends if f["user_id"] == active_friend_id),
                active_friend_id
            )

            col_title, col_btn = st.columns([4, 1])
            with col_title:
                st.markdown(f"### Chat with {active_friend_name}")
            with col_btn:
                if st.button("✖", key="close_chat", help="Close Chat"):
                    st.session_state["active_chat"] = None
                    st.rerun()

            # Load real messages from database
            try:
                messages = real_db.get_messages(user_id, active_friend_id)
            except Exception as e:
                st.error(f"Could not load messages: {e}")
                messages = []

            chat_container = st.container(height=400)
            with chat_container:
                if not messages:
                    st.caption("No messages yet. Say hello! 👋")
                for msg in messages:
                    role = "user" if msg["sender_id"] == user_id else "assistant"
                    with st.chat_message(role):
                        st.markdown(msg["content"])

            # Send new message
            if prompt := st.chat_input("Type your message..."):
                try:
                    real_db.send_message(user_id, active_friend_id, prompt)
                    st.rerun()
                except Exception as e:
                    st.error(f"Could not send message: {e}")

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

elif page == "settings":
    st.markdown("<h1 class='page-title'>Settings & Profile</h1>", unsafe_allow_html=True)

    # --- Profile card (same as before) ---
    st.markdown("""
    <div style="background-color: #1e1e1e; border: 0.5px solid #2a2a2a; border-radius: 12px;
                padding: 18px; margin-bottom: 24px;">
        <div style="display: flex; gap: 14px; align-items: flex-start; flex-wrap: wrap;">
            <div style="width: 56px; height: 56px; border-radius: 50%; background-color: #1e3a2a;
                        display: flex; align-items: center; justify-content: center;
                        font-size: 18px; font-weight: 500; color: #22c55e; flex-shrink: 0;">CM</div>
            <div style="flex: 1; min-width: 0;">
                <div style="font-size: 17px; font-weight: 500; color: #fff; margin-bottom: 3px;">Carlos Martinez</div>
                <div style="font-size: 13px; color: #666; margin-bottom: 10px;">carlos.martinez@email.com</div>
                <div style="display: flex; gap: 8px; flex-wrap: wrap;">
                    <span style="font-size: 12px; padding: 4px 10px; border-radius: 20px;
                                 background-color: #2a2a2a; border: 0.5px solid #333; color: #ccc;">
                        ⚽ Soccer · Intermediate</span>
                    <span style="font-size: 12px; padding: 4px 10px; border-radius: 20px;
                                 background-color: #2a2a2a; border: 0.5px solid #333; color: #ccc;">
                        🏀 Basketball · Beginner</span>
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