"""
community_page.py
 
Community page for Sports Connect.
Displays the 10 most recent posts from the current user's friends,
plus one piece of GenAI advice and encouragement.
"""
 
import streamlit as st
import data_fetcher
 
 
def show_community_page(user_id: str) -> None:
    """
    Render the Community page.
 
    Parameters
    ----------
    user_id : The currently logged-in user's ID (from session state).
    """
    st.title("🏟️ Community")
    st.write("See what your friends have been up to!")
 
    # -----------------------------------------------------------------------
    # Section 1: GenAI Advice
    # -----------------------------------------------------------------------
    st.subheader("💡 Your Daily Tip")
 
    with st.spinner("Getting your personalized advice..."):
        try:
            advice = data_fetcher.get_genai_advice(user_id)
            st.info(advice)
        except Exception as e:
            st.warning(f"Could not load advice right now: {e}")
 
    st.divider()
 
    # -----------------------------------------------------------------------
    # Section 2: Friend Posts (10 most recent)
    # -----------------------------------------------------------------------
    st.subheader("📰 Friend Activity Feed")
 
    try:
        posts = data_fetcher.get_friend_posts(user_id, limit=10)
    except Exception as e:
        st.error(f"Could not load posts: {e}")
        posts = []
 
    if not posts:
        st.write("No posts from friends yet. Invite some friends to get started!")
    else:
        for post in posts:
            with st.container(border=True):
                col1, col2 = st.columns([3, 1])
                with col1:
                    author = post.get("email", post.get("user_id", "Unknown"))
                    st.markdown(f"**{author}**")
                    st.write(post.get("content", ""))
                with col2:
                    ts = post.get("created_at")
                    if ts:
                        st.caption(str(ts)[:16])
 
 
# ---------------------------------------------------------------------------
# Streamlit entry point
# ---------------------------------------------------------------------------
 
if __name__ == "__main__":
    st.set_page_config(page_title="Community", page_icon="🏟️", layout="wide")
 
    if "user_id" not in st.session_state:
        st.warning("Please log in first.")
        st.stop()
 
    show_community_page(st.session_state["user_id"])