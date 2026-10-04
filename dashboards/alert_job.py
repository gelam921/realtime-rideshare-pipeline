"""
Polls for anomalies and appends NEW ones to an alert file (JSON Lines).

Settled windows never change again, so an anomaly found once is stable. We
remember what we've already written (by window_start + zone + kind) so
restarting this job doesn't duplicate alerts.

Run:  python alert_job.py
"""

import json
import os
import time
from datetime import datetime, timezone

from anomaly import LAKE_ROOT, find_anomalies

ALERT_FILE = f"{LAKE_ROOT}/alerts/alerts.jsonl"
POLL_SECONDS = 30


def _load_seen():
    seen = set()
    if os.path.exists(ALERT_FILE):
        with open(ALERT_FILE) as f:
            for line in f:
                a = json.loads(line)
                seen.add((a["window_start"], a["zone"], a["kind"]))
    return seen


def run_once(seen):
    """One detection pass. Returns the list of newly written alerts."""
    os.makedirs(os.path.dirname(ALERT_FILE), exist_ok=True)
    new_alerts = []
    # oldest first so the file reads chronologically
    for a in reversed(find_anomalies()):
        key = (a["window_start"], a["zone"], a["kind"])
        if key in seen:
            continue
        seen.add(key)
        a["detected_at"] = datetime.now(timezone.utc).isoformat()
        with open(ALERT_FILE, "a") as f:
            f.write(json.dumps(a) + "\n")
        new_alerts.append(a)
        print(
            f"ALERT {a['kind'].upper()} zone={a['zone']} "
            f"window={a['window_start']} rides={a['rides']} "
            f"baseline={a['baseline_mean']} z={a['z_score']}"
        )
    return new_alerts


def main():
    seen = _load_seen()
    print(f"alert_job started; writing to {ALERT_FILE}")
    while True:
        run_once(seen)
        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    main()
