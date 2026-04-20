"""
api/db.py — BigQuery client singleton
=======================================
Import _bq() from any route file to get a shared, lazily-initialised
BigQuery client. Cloud Run authenticates automatically via the attached
service account; locally you need GOOGLE_APPLICATION_CREDENTIALS set.
"""

from google.cloud import bigquery

_client: bigquery.Client | None = None

PROJECT_ID = "carlos-negron-uprm"
DATASET    = "database"


def _bq() -> bigquery.Client:
    global _client
    if _client is None:
        _client = bigquery.Client(project=PROJECT_ID)
    return _client


def tbl(name: str) -> str:
    """Return a fully-qualified back-tick-quoted BigQuery table path."""
    return f"`{PROJECT_ID}.{DATASET}.{name}`"


def run_query(sql: str, params: list | None = None) -> list[dict]:
    """Execute a parameterised query and return rows as plain dicts."""
    cfg    = bigquery.QueryJobConfig(query_parameters=params or [])
    result = _bq().query(sql, job_config=cfg).result()
    return [dict(row) for row in result]