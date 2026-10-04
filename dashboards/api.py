import os

import duckdb
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from anomaly import LAKE_ROOT, find_anomalies

LAKE_PATH = f"{LAKE_ROOT}/silver/ride_window_agg/**/*.parquet"
DLQ_PATH = f"{LAKE_ROOT}/dlq/*.parquet"
ALERT_FILE = f"{LAKE_ROOT}/alerts/alerts.jsonl"

app = FastAPI()

# Grafana's browser-based JSON datasource plugin needs CORS enabled
# to call this API from a different port.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


def run_query(sql: str):
    con = duckdb.connect()
    result = con.execute(sql).fetchall()
    columns = [desc[0] for desc in con.description]
    con.close()
    return [dict(zip(columns, row)) for row in result]


@app.get("/")
def health():
    return {"status": "ok"}


@app.get("/rides_per_zone")
def rides_per_zone():
    """
    Total rides completed per zone, deduped to the latest revision of each
    window (since outputMode('update') + append-mode Parquet writes mean
    the same window can appear multiple times as it gets revised by late
    data). We take the max rides_completed per (window_start, zone) as the
    final, most-complete value for that window.
    """
    sql = f"""
        SELECT
            zone,
            SUM(rides_completed) AS total_rides,
            AVG(avg_fare) AS avg_fare
        FROM (
            SELECT
                window_start,
                zone,
                MAX(rides_completed) AS rides_completed,
                MAX(avg_fare) AS avg_fare
            FROM read_parquet('{LAKE_PATH}', hive_partitioning=1)
            GROUP BY window_start, zone
        )
        GROUP BY zone
        ORDER BY total_rides DESC
    """
    return run_query(sql)


@app.get("/rides_over_time")
def rides_over_time():
    """
    Rides completed per 5-minute window per zone, deduped to the latest
    revision per window, ordered by time, for a time series panel.
    """
    sql = f"""
        SELECT
            window_start,
            zone,
            MAX(rides_completed) AS rides_completed,
            MAX(avg_fare) AS avg_fare
        FROM read_parquet('{LAKE_PATH}', hive_partitioning=1)
        GROUP BY window_start, zone
        ORDER BY window_start ASC
    """
    return run_query(sql)


@app.get("/anomalies")
def anomalies():
    """Live anomaly scan of settled windows (see anomalies.py)."""
    return find_anomalies()


@app.get("/alerts")
def alerts():
    """Durable alert history written by alert_job.py, newest first."""
    if not os.path.exists(ALERT_FILE):
        return []
    return run_query(
        f"SELECT * FROM read_json_auto('{ALERT_FILE}') ORDER BY window_start DESC"
    )


@app.get("/dlq_summary")
def dlq_summary():
    """Dead-letter counts by reason."""
    try:
        return run_query(f"""
            SELECT error_reason, COUNT(*) AS n
            FROM read_parquet('{DLQ_PATH}')
            GROUP BY error_reason ORDER BY n DESC
        """)
    except duckdb.IOException:
        return []