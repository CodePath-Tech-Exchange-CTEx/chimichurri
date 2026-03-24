#############################################################################
# modules_test.py
#
# This file contains tests for modules.py.
#############################################################################

import unittest
from datetime import datetime, timedelta
from streamlit.testing.v1 import AppTest
import modules


# ------------------------------------------------------------
# Helper Test Data
# ------------------------------------------------------------

def sample_sessions():
    now = datetime.now()
    return [
        {
            "sport": "Soccer",
            "start_time": now - timedelta(hours=2),
            "end_time": now - timedelta(hours=1),
            "location": "Central Park"
        },
        {
            "sport": "Basketball",
            "start_time": now - timedelta(hours=4),
            "end_time": now - timedelta(hours=3),
            "location": "Downtown Court"
        }
    ]


def sample_friends():
    return [
        {"friend_id": "u2", "email": "alice@example.com"},
        {"friend_id": "u3", "email": "bob@example.com"},
    ]
 
 
# ==========================================================
# get_sport_icon Tests
# ==========================================================
 
class TestGetSportIcon(unittest.TestCase):
 
    def test_known_sport_soccer(self):
        self.assertEqual(modules.get_sport_icon("Soccer"), "⚽")
 
    def test_known_sport_basketball(self):
        self.assertEqual(modules.get_sport_icon("Basketball"), "🏀")
 
    def test_known_sport_volleyball(self):
        self.assertEqual(modules.get_sport_icon("Volleyball"), "🏐")
 
    def test_known_sport_tennis(self):
        self.assertEqual(modules.get_sport_icon("Tennis"), "🎾")
 
    def test_unknown_sport_defaults_to_runner(self):
        self.assertEqual(modules.get_sport_icon("Cricket"), "🏃")
 
    def test_empty_string_defaults(self):
        self.assertEqual(modules.get_sport_icon(""), "🏃")
 
 
# ==========================================================
# display_map Tests
# ==========================================================
 
class TestDisplayMap(unittest.TestCase):
 
    def test_display_map_renders_without_exception(self):
        user_location = {"lat": 30.0900, "lng": -95.9900}
        at = AppTest.from_function(
            lambda: modules.display_map(user_location)
        ).run()
        self.assertEqual(len(at.exception), 0)
 
    def test_display_map_renders_different_location(self):
        user_location = {"lat": 18.4655, "lng": -66.1057}
        at = AppTest.from_function(
            lambda: modules.display_map(user_location)
        ).run()
        self.assertEqual(len(at.exception), 0)
 
 
# ==========================================================
# display_session_summary Tests
# ==========================================================
 
class TestDisplaySessionSummary(unittest.TestCase):
 
    def test_summary_with_data_renders(self):
        sessions = sample_sessions()
        at = AppTest.from_function(
            lambda: modules.display_session_summary(sessions)
        ).run()
        self.assertEqual(len(at.exception), 0)
 
    def test_summary_empty_sessions_renders(self):
        at = AppTest.from_function(
            lambda: modules.display_session_summary([])
        ).run()
        self.assertEqual(len(at.exception), 0)
 
    def test_summary_single_session(self):
        now = datetime.now()
        sessions = [{
            "sport": "Tennis",
            "start_time": now - timedelta(hours=1),
            "end_time": now,
            "location": "Club Court"
        }]
        at = AppTest.from_function(
            lambda: modules.display_session_summary(sessions)
        ).run()
        self.assertEqual(len(at.exception), 0)
 
 
# ==========================================================
# display_recent_games Tests
# ==========================================================
 
class TestDisplayRecentGames(unittest.TestCase):
 
    def test_recent_games_with_data_renders(self):
        sessions = sample_sessions()
        at = AppTest.from_function(
            lambda: modules.display_recent_games(sessions)
        ).run()
        self.assertEqual(len(at.exception), 0)
 
    def test_recent_games_empty_list_renders(self):
        at = AppTest.from_function(
            lambda: modules.display_recent_games([])
        ).run()
        self.assertEqual(len(at.exception), 0)
 
    def test_recent_games_single_session(self):
        now = datetime.now()
        sessions = [{
            "sport": "Soccer",
            "start_time": now - timedelta(hours=1),
            "end_time": now,
            "location": "Riverside Field"
        }]
        at = AppTest.from_function(
            lambda: modules.display_recent_games(sessions)
        ).run()
        self.assertEqual(len(at.exception), 0)
 
 
# ==========================================================
# display_personalized_recommendations Tests
# ==========================================================
 
class TestDisplayPersonalizedRecommendations(unittest.TestCase):
 
    def test_recommendations_renders_with_data(self):
        sessions = sample_sessions()
        friends = sample_friends()
        at = AppTest.from_function(
            lambda: modules.display_personalized_recommendations(sessions, friends)
        ).run()
        self.assertEqual(len(at.exception), 0)
 
    def test_recommendations_renders_with_no_friends(self):
        sessions = sample_sessions()
        at = AppTest.from_function(
            lambda: modules.display_personalized_recommendations(sessions, [])
        ).run()
        self.assertEqual(len(at.exception), 0)
 
    def test_recommendations_renders_with_string_friends(self):
        """Backwards-compatible: friends list as plain strings also works."""
        sessions = sample_sessions()
        friends = ["Alice", "Bob"]
        at = AppTest.from_function(
            lambda: modules.display_personalized_recommendations(sessions, friends)
        ).run()
        self.assertEqual(len(at.exception), 0)
 
 
if __name__ == "__main__":
    unittest.main()