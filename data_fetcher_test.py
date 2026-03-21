"""
data_fetcher_test.py

Unit tests for data_fetcher.py.

Strategy
--------
All BigQuery I/O is mocked via unittest.mock.patch so that tests run
offline with no GCP credentials and no billing.

Each test patches `data_fetcher.run_query` (the single choke-point)
and verifies:
  1. The correct SQL keywords / table names appear in the query string.
  2. The correct parameters are passed.
  3. Return values are forwarded correctly.
  4. Guard-rail ValueError cases are raised when expected.
"""

import unittest
from unittest.mock import patch, call, MagicMock
import uuid

import data_fetcher as df


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _param_map(params: list) -> dict:
    """Convert a list of ScalarQueryParameter into {name: value} for assertions."""
    return {p.name: p.value for p in params}


# ---------------------------------------------------------------------------
# USER TESTS
# ---------------------------------------------------------------------------

class TestGetUser(unittest.TestCase):

    @patch("data_fetcher.run_query")
    def test_returns_user_when_found(self, mock_rq):
        mock_rq.return_value = [{"user_id": "u1", "email": "a@b.com", "sports": []}]
        result = df.get_user("u1")
        self.assertEqual(result["user_id"], "u1")
        query, params = mock_rq.call_args[0]
        self.assertIn("users", query)
        self.assertEqual(_param_map(params)["user_id"], "u1")

    @patch("data_fetcher.run_query")
    def test_returns_none_when_not_found(self, mock_rq):
        mock_rq.return_value = []
        result = df.get_user("nonexistent")
        self.assertIsNone(result)

    @patch("data_fetcher.run_query")
    def test_query_contains_limit_1(self, mock_rq):
        mock_rq.return_value = []
        df.get_user("u1")
        query = mock_rq.call_args[0][0]
        self.assertIn("LIMIT 1", query)


class TestGetUsersBySport(unittest.TestCase):

    @patch("data_fetcher.run_query")
    def test_passes_sport_param(self, mock_rq):
        mock_rq.return_value = []
        df.get_users_by_sport("Soccer")
        _, params = mock_rq.call_args[0]
        self.assertEqual(_param_map(params)["sport"], "Soccer")

    @patch("data_fetcher.run_query")
    def test_query_unnests_sports_array(self, mock_rq):
        mock_rq.return_value = []
        df.get_users_by_sport("Basketball")
        query = mock_rq.call_args[0][0]
        self.assertIn("UNNEST", query)

    @patch("data_fetcher.run_query")
    def test_returns_list(self, mock_rq):
        mock_rq.return_value = [{"user_id": "u1", "sport": "Soccer"}]
        result = df.get_users_by_sport("Soccer")
        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 1)


# ---------------------------------------------------------------------------
# FRIENDSHIP TESTS
# ---------------------------------------------------------------------------

class TestGetFriends(unittest.TestCase):

    @patch("data_fetcher.run_query")
    def test_filters_accepted_status(self, mock_rq):
        mock_rq.return_value = []
        df.get_friends("u1")
        query = mock_rq.call_args[0][0]
        self.assertIn("accepted", query)

    @patch("data_fetcher.run_query")
    def test_passes_user_id_param(self, mock_rq):
        mock_rq.return_value = []
        df.get_friends("u1")
        _, params = mock_rq.call_args[0]
        self.assertEqual(_param_map(params)["user_id"], "u1")

    @patch("data_fetcher.run_query")
    def test_joins_users_table(self, mock_rq):
        mock_rq.return_value = []
        df.get_friends("u1")
        query = mock_rq.call_args[0][0]
        self.assertIn("users", query)


class TestSendFriendRequest(unittest.TestCase):

    @patch("data_fetcher.run_query")
    def test_inserts_two_rows(self, mock_rq):
        # First call is the guard check (returns 0), second is the INSERT
        mock_rq.side_effect = [[{"cnt": 0}], []]
        df.send_friend_request("u1", "u2")
        self.assertEqual(mock_rq.call_count, 2)
        insert_query = mock_rq.call_args_list[1][0][0]
        self.assertIn("INSERT", insert_query)
        # Both directions appear in the VALUES clause
        self.assertIn("@user_id", insert_query)
        self.assertIn("@friend_id", insert_query)

    @patch("data_fetcher.run_query")
    def test_raises_if_relationship_exists(self, mock_rq):
        mock_rq.return_value = [{"cnt": 2}]
        with self.assertRaises(ValueError):
            df.send_friend_request("u1", "u2")

    @patch("data_fetcher.run_query")
    def test_insert_status_is_pending(self, mock_rq):
        mock_rq.side_effect = [[{"cnt": 0}], []]
        df.send_friend_request("u1", "u2")
        insert_query = mock_rq.call_args_list[1][0][0]
        self.assertIn("pending", insert_query)


class TestAcceptFriendRequest(unittest.TestCase):

    @patch("data_fetcher._update_friendship_status")
    def test_calls_update_with_accepted(self, mock_update):
        df.accept_friend_request("u1", "u2")
        mock_update.assert_called_once_with("u1", "u2", "accepted")


class TestRejectFriendRequest(unittest.TestCase):

    @patch("data_fetcher.run_query")
    def test_issues_delete(self, mock_rq):
        mock_rq.return_value = []
        df.reject_friend_request("u1", "u2")
        query = mock_rq.call_args[0][0]
        self.assertIn("DELETE", query)

    @patch("data_fetcher.run_query")
    def test_delete_covers_both_directions(self, mock_rq):
        mock_rq.return_value = []
        df.reject_friend_request("u1", "u2")
        query = mock_rq.call_args[0][0]
        # The WHERE clause must reference both (u1→u2) and (u2→u1)
        self.assertIn("@user_id", query)
        self.assertIn("@friend_id", query)


# ---------------------------------------------------------------------------
# EVENT TESTS
# ---------------------------------------------------------------------------

class TestGetEvent(unittest.TestCase):

    @patch("data_fetcher.run_query")
    def test_returns_event_when_found(self, mock_rq):
        mock_rq.return_value = [{"event_id": "e1", "sport": "Soccer"}]
        result = df.get_event("e1")
        self.assertIsNotNone(result)
        self.assertEqual(result["event_id"], "e1")

    @patch("data_fetcher.run_query")
    def test_returns_none_when_not_found(self, mock_rq):
        mock_rq.return_value = []
        self.assertIsNone(df.get_event("ghost"))

    @patch("data_fetcher.run_query")
    def test_passes_event_id_param(self, mock_rq):
        mock_rq.return_value = []
        df.get_event("e1")
        _, params = mock_rq.call_args[0]
        self.assertEqual(_param_map(params)["event_id"], "e1")


class TestGetEventsBySport(unittest.TestCase):

    @patch("data_fetcher.run_query")
    def test_filters_public_and_open(self, mock_rq):
        mock_rq.return_value = []
        df.get_events_by_sport("Tennis")
        query = mock_rq.call_args[0][0]
        self.assertIn("public", query)
        self.assertIn("@status", query)

    @patch("data_fetcher.run_query")
    def test_case_insensitive_sport_match(self, mock_rq):
        mock_rq.return_value = []
        df.get_events_by_sport("tennis")
        query = mock_rq.call_args[0][0]
        self.assertIn("LOWER", query)

    @patch("data_fetcher.run_query")
    def test_only_future_events(self, mock_rq):
        mock_rq.return_value = []
        df.get_events_by_sport("Soccer")
        query = mock_rq.call_args[0][0]
        self.assertIn("CURRENT_TIMESTAMP", query)


class TestGetNearbyEvents(unittest.TestCase):

    @patch("data_fetcher.run_query")
    def test_uses_st_distance(self, mock_rq):
        mock_rq.return_value = []
        df.get_nearby_events(25.76, -80.19)
        query = mock_rq.call_args[0][0]
        self.assertIn("ST_DISTANCE", query)
        self.assertIn("ST_GEOGPOINT", query)

    @patch("data_fetcher.run_query")
    def test_passes_radius_param(self, mock_rq):
        mock_rq.return_value = []
        df.get_nearby_events(25.76, -80.19, radius_meters=3000)
        _, params = mock_rq.call_args[0]
        pm = _param_map(params)
        self.assertEqual(pm["radius_meters"], 3000)
        self.assertEqual(pm["lat"], 25.76)
        self.assertEqual(pm["lng"], -80.19)

    @patch("data_fetcher.run_query")
    def test_default_radius_is_5000(self, mock_rq):
        mock_rq.return_value = []
        df.get_nearby_events(25.76, -80.19)
        _, params = mock_rq.call_args[0]
        self.assertEqual(_param_map(params)["radius_meters"], 5000)

    @patch("data_fetcher.run_query")
    def test_orders_by_distance(self, mock_rq):
        mock_rq.return_value = []
        df.get_nearby_events(25.76, -80.19)
        query = mock_rq.call_args[0][0]
        self.assertIn("distance_meters", query)
        self.assertIn("ORDER BY", query)


class TestGetUserCreatedEvents(unittest.TestCase):

    @patch("data_fetcher.run_query")
    def test_filters_by_created_by(self, mock_rq):
        mock_rq.return_value = []
        df.get_user_created_events("u1")
        query = mock_rq.call_args[0][0]
        self.assertIn("created_by", query)

    @patch("data_fetcher.run_query")
    def test_orders_newest_first(self, mock_rq):
        mock_rq.return_value = []
        df.get_user_created_events("u1")
        query = mock_rq.call_args[0][0]
        self.assertIn("DESC", query)


# ---------------------------------------------------------------------------
# EVENT PARTICIPANT TESTS
# ---------------------------------------------------------------------------

class TestJoinEvent(unittest.TestCase):

    def _open_event(self):
        return {"event_id": "e1", "status": "open", "max_players": 10}

    @patch("data_fetcher.run_query")
    @patch("data_fetcher.get_event")
    def test_inserts_joined_row(self, mock_get, mock_rq):
        mock_get.return_value = self._open_event()
        # check existing join → 0; check capacity → 0; insert → []
        mock_rq.side_effect = [[{"cnt": 0}], [{"cnt": 5}], []]
        df.join_event("u1", "e1")
        insert_query = mock_rq.call_args_list[2][0][0]
        self.assertIn("INSERT", insert_query)
        self.assertIn("joined", insert_query)

    @patch("data_fetcher.run_query")
    @patch("data_fetcher.get_event")
    def test_raises_if_event_not_found(self, mock_get, mock_rq):
        mock_get.return_value = None
        with self.assertRaises(ValueError):
            df.join_event("u1", "ghost")

    @patch("data_fetcher.run_query")
    @patch("data_fetcher.get_event")
    def test_raises_if_event_not_open(self, mock_get, mock_rq):
        mock_get.return_value = {"event_id": "e1", "status": "closed", "max_players": 10}
        with self.assertRaises(ValueError):
            df.join_event("u1", "e1")

    @patch("data_fetcher.run_query")
    @patch("data_fetcher.get_event")
    def test_raises_if_already_joined(self, mock_get, mock_rq):
        mock_get.return_value = self._open_event()
        mock_rq.return_value = [{"cnt": 1}]   # already joined
        with self.assertRaises(ValueError):
            df.join_event("u1", "e1")

    @patch("data_fetcher.run_query")
    @patch("data_fetcher.get_event")
    def test_raises_if_at_capacity(self, mock_get, mock_rq):
        mock_get.return_value = {"event_id": "e1", "status": "open", "max_players": 2}
        # not already joined, but capacity full
        mock_rq.side_effect = [[{"cnt": 0}], [{"cnt": 2}]]
        with self.assertRaises(ValueError):
            df.join_event("u1", "e1")


class TestLeaveEvent(unittest.TestCase):

    @patch("data_fetcher.run_query")
    def test_inserts_left_row_not_delete(self, mock_rq):
        mock_rq.return_value = []
        df.leave_event("u1", "e1")
        query = mock_rq.call_args[0][0]
        self.assertIn("INSERT", query)
        self.assertIn("left", query)
        self.assertNotIn("DELETE", query)

    @patch("data_fetcher.run_query")
    def test_passes_correct_params(self, mock_rq):
        mock_rq.return_value = []
        df.leave_event("u1", "e1")
        _, params = mock_rq.call_args[0]
        pm = _param_map(params)
        self.assertEqual(pm["user_id"],  "u1")
        self.assertEqual(pm["event_id"], "e1")


class TestGetEventParticipants(unittest.TestCase):

    @patch("data_fetcher.run_query")
    def test_uses_row_number_for_dedup(self, mock_rq):
        mock_rq.return_value = []
        df.get_event_participants("e1")
        query = mock_rq.call_args[0][0]
        self.assertIn("ROW_NUMBER", query)

    @patch("data_fetcher.run_query")
    def test_only_returns_joined_status(self, mock_rq):
        mock_rq.return_value = []
        df.get_event_participants("e1")
        query = mock_rq.call_args[0][0]
        self.assertIn("joined", query)

    @patch("data_fetcher.run_query")
    def test_returns_list(self, mock_rq):
        mock_rq.return_value = [{"user_id": "u1", "email": "a@b.com"}]
        result = df.get_event_participants("e1")
        self.assertEqual(len(result), 1)


# ---------------------------------------------------------------------------
# ACTIVITY TESTS
# ---------------------------------------------------------------------------

class TestLogActivity(unittest.TestCase):

    @patch("data_fetcher.run_query")
    def test_inserts_activity_row(self, mock_rq):
        mock_rq.return_value = []
        df.log_activity("u1", "join_event", event_id="e1", sport="Soccer")
        query = mock_rq.call_args[0][0]
        self.assertIn("INSERT", query)
        self.assertIn("user_activity", query)

    @patch("data_fetcher.run_query")
    def test_generates_uuid_in_python(self, mock_rq):
        mock_rq.return_value = []
        df.log_activity("u1", "view_event")
        _, params = mock_rq.call_args[0]
        pm = _param_map(params)
        # activity_id must be a valid UUID string
        try:
            uuid.UUID(pm["activity_id"])
            valid = True
        except (ValueError, KeyError):
            valid = False
        self.assertTrue(valid)

    @patch("data_fetcher.run_query")
    def test_location_uses_st_geogpoint(self, mock_rq):
        mock_rq.return_value = []
        loc = {"location_id": "loc1", "name": "Park", "lat": 25.76, "lng": -80.19}
        df.log_activity("u1", "join_event", location=loc)
        query = mock_rq.call_args[0][0]
        self.assertIn("ST_GEOGPOINT", query)

    @patch("data_fetcher.run_query")
    def test_works_without_location(self, mock_rq):
        mock_rq.return_value = []
        df.log_activity("u1", "search")   # no location arg
        query = mock_rq.call_args[0][0]
        self.assertIn("INSERT", query)
        self.assertNotIn("ST_GEOGPOINT", query)


class TestGetUserActivity(unittest.TestCase):

    @patch("data_fetcher.run_query")
    def test_orders_newest_first(self, mock_rq):
        mock_rq.return_value = []
        df.get_user_activity("u1")
        query = mock_rq.call_args[0][0]
        self.assertIn("DESC", query)

    @patch("data_fetcher.run_query")
    def test_default_limit_50(self, mock_rq):
        mock_rq.return_value = []
        df.get_user_activity("u1")
        _, params = mock_rq.call_args[0]
        self.assertEqual(_param_map(params)["limit"], 50)

    @patch("data_fetcher.run_query")
    def test_optional_activity_type_filter(self, mock_rq):
        mock_rq.return_value = []
        df.get_user_activity("u1", activity_type="join_event")
        query, params = mock_rq.call_args[0]
        self.assertIn("activity_type", query)
        self.assertEqual(_param_map(params)["activity_type"], "join_event")

    @patch("data_fetcher.run_query")
    def test_no_activity_type_filter_by_default(self, mock_rq):
        mock_rq.return_value = []
        df.get_user_activity("u1")
        query = mock_rq.call_args[0][0]
        # The activity_type WHERE clause should NOT be present
        self.assertNotIn("AND activity_type", query)


# ---------------------------------------------------------------------------
# RECOMMENDATION TESTS
# ---------------------------------------------------------------------------

class TestGetRecommendedEvents(unittest.TestCase):

    @patch("data_fetcher.run_query")
    def test_joins_events_table(self, mock_rq):
        mock_rq.return_value = []
        df.get_recommended_events("u1")
        query = mock_rq.call_args[0][0]
        self.assertIn("user_recommendations", query)
        self.assertIn("events", query)
        self.assertIn("JOIN", query)

    @patch("data_fetcher.run_query")
    def test_orders_by_score_desc(self, mock_rq):
        mock_rq.return_value = []
        df.get_recommended_events("u1")
        query = mock_rq.call_args[0][0]
        self.assertIn("score", query)
        self.assertIn("DESC", query)

    @patch("data_fetcher.run_query")
    def test_default_limit_10(self, mock_rq):
        mock_rq.return_value = []
        df.get_recommended_events("u1")
        _, params = mock_rq.call_args[0]
        self.assertEqual(_param_map(params)["limit"], 10)

    @patch("data_fetcher.run_query")
    def test_filters_open_public_future(self, mock_rq):
        mock_rq.return_value = []
        df.get_recommended_events("u1")
        query = mock_rq.call_args[0][0]
        self.assertIn("open", query)
        self.assertIn("public", query)
        self.assertIn("CURRENT_TIMESTAMP", query)

    @patch("data_fetcher.run_query")
    def test_returns_list(self, mock_rq):
        mock_rq.return_value = [{"event_id": "e1", "score": 0.95}]
        result = df.get_recommended_events("u1")
        self.assertIsInstance(result, list)
        self.assertEqual(result[0]["score"], 0.95)


# ---------------------------------------------------------------------------
# run_query UNIT TEST
# ---------------------------------------------------------------------------

class TestRunQuery(unittest.TestCase):

    @patch("data_fetcher.client")
    def test_returns_list_of_dicts(self, mock_client):
        """run_query must convert BigQuery Row objects to plain dicts."""
        fake_row = {"user_id": "u1", "email": "a@b.com"}
        mock_job = MagicMock()
        mock_job.result.return_value = [fake_row]
        mock_client.query.return_value = mock_job

        result = df.run_query("SELECT 1")
        self.assertIsInstance(result, list)
        self.assertEqual(result[0]["user_id"], "u1")

    @patch("data_fetcher.client")
    def test_passes_params_to_job_config(self, mock_client):
        from google.cloud.bigquery import ScalarQueryParameter
        mock_job = MagicMock()
        mock_job.result.return_value = []
        mock_client.query.return_value = mock_job

        param = ScalarQueryParameter("x", "STRING", "hello")
        df.run_query("SELECT @x", [param])

        _, kwargs = mock_client.query.call_args
        # job_config must have been passed
        self.assertIn("job_config", kwargs)


if __name__ == "__main__":
    unittest.main()