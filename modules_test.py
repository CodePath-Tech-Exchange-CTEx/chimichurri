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


# ==========================================================
# get_sport_icon Tests (True Unit Tests)
# ==========================================================

class TestGetSportIcon(unittest.TestCase):

    def test_known_sport(self):
        self.assertEqual(modules.get_sport_icon("Soccer"), "⚽")

    def test_unknown_sport_defaults(self):
        self.assertEqual(modules.get_sport_icon("Cricket"), "🏃")


# ==========================================================
# display_map Tests
# ==========================================================

class TestDisplayMap(unittest.TestCase):

    def test_display_map_renders(self):
        user_location = {
            "lat": 30.0900,
            "lng": -95.9900
        }

        at = AppTest.from_function(
            lambda: modules.display_map(user_location)
        ).run()

        self.assertEqual(len(at.exception), 0)


# ==========================================================
# display_session_summary Tests
# ==========================================================

class TestDisplaySessionSummary(unittest.TestCase):

    def test_summary_with_data(self):
        sessions = sample_sessions()

        at = AppTest.from_function(
            lambda: modules.display_session_summary(sessions)
        ).run()

        self.assertEqual(len(at.exception), 0)

    def test_summary_empty(self):
        at = AppTest.from_function(
            lambda: modules.display_session_summary([])
        ).run()

        self.assertEqual(len(at.exception), 0)


# ==========================================================
# display_recent_games Tests
# ==========================================================

class TestDisplayRecentGames(unittest.TestCase):

    def test_recent_games_with_data(self):
        sessions = sample_sessions()

        at = AppTest.from_function(
            lambda: modules.display_recent_games(sessions)
        ).run()

        self.assertEqual(len(at.exception), 0)

    def test_recent_games_empty(self):
        at = AppTest.from_function(
            lambda: modules.display_recent_games([])
        ).run()

        self.assertEqual(len(at.exception), 0)


# ==========================================================
# display_personalized_recommendations Tests
# ==========================================================

class TestDisplayPersonalizedRecommendations(unittest.TestCase):

    def test_recommendations_renders(self):
        sessions = sample_sessions()
        friends = ["Alice", "Bob"]

        at = AppTest.from_function(
            lambda: modules.display_personalized_recommendations(sessions, friends)
        ).run()

        self.assertEqual(len(at.exception), 0)


if __name__ == "__main__":
    unittest.main()