"""
Anomaly detection over the silver ride_window_agg lake.

Method: per-zone rolling z-score. Each settled 5-minute window's ride count is
compared with the mean/spread of the same zone's previous 12 windows (the
hour before it, excluding the window itself).

Used by both api.py (/anomalies endpoint) and alert_job.py (alert output).
"""

import os

import duckdb

LAKE_ROOT = os.path.expanduser(os.getenv("LAKE_ROOT", "~/rides-lake"))
AGG_GLOB = f"{LAKE_ROOT}/silver/ride_window_agg/**/*.parquet"

# These must match windowed_aggregation_to_parquet.py
WINDOW_MINUTES = 5
WATERMARK_MINUTES = 2

# Detection settings
BASELINE_MINUTES = 60   # how far back the baseline looks
MIN_HISTORY = 6         # need at least this many baseline windows
Z_THRESHOLD = 3.0       # how many "standard deviations" counts as unusual
MIN_ABS_CHANGE = 3      # ignore tiny swings in very quiet zones
MIN_ACTIVE_ZONES = 2    # a slot only counts as "pipeline alive" if this many
                        # zones had data in it (see active_slots below)


def _query(sql):
    con = duckdb.connect()
    try:
        cur = con.execute(sql)
        columns = [d[0] for d in cur.description]
        return [dict(zip(columns, row)) for row in cur.fetchall()]
    finally:
        con.close()


def find_anomalies(
    z_threshold=Z_THRESHOLD,
    min_history=MIN_HISTORY,
    min_abs_change=MIN_ABS_CHANGE,
):
    """Return anomalous windows, newest first. Empty list if no data yet."""
    sql = f"""
    WITH dedup AS (
        -- update-mode output revises a window several times; counts only
        -- grow, so MAX is the final value (same rule api.py uses).
        SELECT CAST(window_start AS TIMESTAMP) AS window_start,
               zone,
               MAX(rides_completed) AS rides
        FROM read_parquet('{AGG_GLOB}', hive_partitioning=1)
        GROUP BY 1, 2
    ),
    cutoff AS (
        -- A window is final once the watermark passes its end. The watermark
        -- trails the newest event by WATERMARK_MINUTES, and the newest event
        -- is at least as late as the newest window start in the lake. So we
        -- only score windows that ended >= WATERMARK_MINUTES before that.
        SELECT MAX(window_start)
               - INTERVAL '{WINDOW_MINUTES + WATERMARK_MINUTES} minutes'
               AS last_settled_start
        FROM dedup
    ),
    zones AS (
        SELECT zone, MIN(window_start) AS first_start FROM dedup GROUP BY zone
    ),
    active_slots AS (
        -- Slots where the pipeline was demonstrably alive: several zones
        -- reported. If the producer was switched off, there are no active
        -- slots, so the gap is not read as "every zone had 0 rides".
        SELECT window_start
        FROM dedup
        GROUP BY window_start
        HAVING COUNT(DISTINCT zone) >= {int(MIN_ACTIVE_ZONES)}
    ),
    spine AS (
        -- a zone that is quiet while others are busy shows up as 0 rides
        -- instead of as a missing row
        SELECT z.zone, a.window_start
        FROM zones z, active_slots a, cutoff c
        WHERE a.window_start >= z.first_start
          AND a.window_start <= c.last_settled_start
    ),
    filled AS (
        SELECT s.zone, s.window_start, COALESCE(d.rides, 0) AS rides
        FROM spine s
        LEFT JOIN dedup d ON d.zone = s.zone AND d.window_start = s.window_start
    ),
    scored AS (
        SELECT *,
               AVG(rides)         OVER w AS baseline_mean,
               STDDEV_SAMP(rides) OVER w AS baseline_sd,
               COUNT(*)           OVER w AS history_n
        FROM filled
        WINDOW w AS (
            PARTITION BY zone ORDER BY window_start
            RANGE BETWEEN INTERVAL '{BASELINE_MINUTES} minutes' PRECEDING
                      AND INTERVAL '{WINDOW_MINUTES} minutes' PRECEDING
        )
    ),
    z AS (
        SELECT *,
               -- ride counts are Poisson-like (spread ~ sqrt(mean)); using that
               -- as a floor stops a flat baseline from producing infinite z
               (rides - baseline_mean)
               / GREATEST(COALESCE(baseline_sd, 0),
                          SQRT(GREATEST(baseline_mean, 1))) AS z_score
        FROM scored
        WHERE history_n >= {int(min_history)}
    )
    SELECT window_start, zone, rides,
           ROUND(baseline_mean, 2) AS baseline_mean,
           ROUND(z_score, 2)       AS z_score,
           CASE WHEN z_score > 0 THEN 'spike' ELSE 'drop' END AS kind
    FROM z
    WHERE ABS(z_score) >= {float(z_threshold)}
      AND ABS(rides - baseline_mean) >= {float(min_abs_change)}
    ORDER BY window_start DESC, zone
    """
    try:
        rows = _query(sql)
    except duckdb.IOException:
        return []  # no parquet files yet
    for r in rows:
        r["window_start"] = r["window_start"].isoformat()
    return rows