"""
activity_page.py
 
Activity page for Sports Connect.
Displays the user's 3 most recent sessions, an activity summary,
and a share button to post a chosen statistic to the community.
"""
 
import streamlit as st
from collections import Counter
import data_fetcher
import modules
 
 
def show_activity_page(user_id: str) -> None:
    """
    Render the Activity page.
 
    Parameters
    ----------
    user_id : The currently logged-in user's ID (from session state).
    """
    st.title("🏃 My Activity")
 
    # -----------------------------------------------------------------------
    # Fetch recent activity (limit 3 for display, 50 for summary stats)
    # -----------------------------------------------------------------------
    try:
        all_activity   = data_fetcher.get_user_activity(user_id, limit=50)
        recent_3       = all_activity[:3]
    except Exception as e:
        st.error(f"Could not load activity: {e}")
        return
 
    # -----------------------------------------------------------------------
    # Section 1: Recent 3 Sessions
    # -----------------------------------------------------------------------
    st.subheader("🕒 Recent Sessions")
 
    if not recent_3:
        st.write("No activity recorded yet. Join an event to get started!")
    else:
        for activity in recent_3:
            with st.container(border=True):
                col1, col2, col3 = st.columns([2, 2, 2])
                with col1:
                    sport = activity.get("sport") or "Unknown Sport"
                    icon  = modules.get_sport_icon(sport)
                    st.markdown(f"**{icon} {sport}**")
                with col2:
                    ts = activity.get("timestamp")
                    st.caption(str(ts)[:16] if ts else "—")
                with col3:
                    mins = activity.get("duration_minutes")
                    st.write(f"⏱️ {mins} min" if mins else "⏱️ Duration N/A")
 
                loc = activity.get("location")
                if loc and isinstance(loc, dict) and loc.get("name"):
                    st.caption(f"📍 {loc['name']}")
 
    st.divider()
 
    # -----------------------------------------------------------------------
    # Section 2: Activity Summary
    # -----------------------------------------------------------------------
    st.subheader("📊 Activity Summary")
 
    if not all_activity:
        st.write("No data yet.")
    else:
        total_sessions = len(all_activity)
        total_minutes  = sum(a.get("duration_minutes") or 0 for a in all_activity)
        sports_played  = [a["sport"] for a in all_activity if a.get("sport")]
        fav_sport      = Counter(sports_played).most_common(1)[0][0] if sports_played else "N/A"
 
        col1, col2, col3 = st.columns(3)
        col1.metric("Total Sessions", total_sessions)
        col2.metric("Total Minutes",  f"{total_minutes} min")
        col3.metric("Favourite Sport", f"{modules.get_sport_icon(fav_sport)} {fav_sport}")
 
    st.divider()
 
    # -----------------------------------------------------------------------
    # Section 3: Share a Stat
    # -----------------------------------------------------------------------
    st.subheader("📣 Share with the Community")
 
    if not all_activity:
        st.write("Complete a session first before sharing!")
        return
 
    stat_options = {
        "Total sessions":        f"I've completed {total_sessions} sports sessions so far! 💪",
        "Total minutes played":  f"I've played for {total_minutes} minutes total! ⏱️",
        "Favourite sport":       f"My favourite sport right now is {modules.get_sport_icon(fav_sport)} {fav_sport}! 🔥",
        "Most recent sport":     (
            f"Just finished a {modules.get_sport_icon(recent_3[0].get('sport','sport'))} "
            f"{recent_3[0].get('sport','sport')} session! 🎉"
            if recent_3 else "Just got active on Sports Connect!"
        ),
    }
 
    chosen_label = st.selectbox(
        "Choose a stat to share:",
        options=list(stat_options.keys()),
    )
    post_content = stat_options[chosen_label]
 
    st.write("**Preview:**")
    st.info(post_content)
 
    if st.button("📤 Share to Community"):
        try:
            data_fetcher.create_post(user_id, post_content)
            st.success("Post shared! Your friends can now see it in the Community feed.")
        except Exception as e:
            st.error(f"Could not share post: {e}")
 
 
# ---------------------------------------------------------------------------
# Streamlit entry point
# ---------------------------------------------------------------------------
 
if __name__ == "__main__":
    st.set_page_config(page_title="My Activity", page_icon="🏃", layout="wide")
 
    if "user_id" not in st.session_state:
        st.warning("Please log in first.")
        st.stop()
 
    show_activity_page(st.session_state["user_id"])