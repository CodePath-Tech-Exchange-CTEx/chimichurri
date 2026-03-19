from google.cloud import bigquery

client = bigquery.Client()

PROJECT_ID = "carlos-negron-uprm" #Needs to be updated with the actual project ID
DATASET = "database" #Needs to be updated with the actual dataset name when created

def run_query(query, params=None):
    """
    Helper function to execute parameterized queries.
    """
    job_config = None

    if params:
        job_config = bigquery.QueryJobConfig(query_parameters=params)

    query_job = client.query(query, job_config=job_config)
    return query_job.result()

    return [dict(row) for row in result]

# --------------------------------------------------
# USER FUNCTIONS
# --------------------------------------------------

def get_user(user_id):
    pass

def get_users_by_sport(sport):
    pass

def get_friends(user_id):
    pass

def send_friend_request(user_id, friend_id):
    pass

def accept_friend_request(friendship_id):
    pass

def reject_friend_request(friend_id):
    pass

def get_event(event_id):
    pass

def get_events_by_sport(sport):
    pass

def get_nearby_events(lat, lng, radius_meters=5000):
    pass

def get_user_created_events(user_id):
    pass

def join_event(user_id, event_id):
    pass

def leave_event(user_id, event_id):
    pass

def get_event_participants(event_id):
    pass

def log_activity(user_id, event_id, activity_type):
    pass

def get_user_activity(user_id):
    pass

def get_recommended_events(user_id):
    pass