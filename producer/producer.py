import json
import random
import time
import uuid
from datetime import datetime, timezone, timedelta

from confluent_kafka import Producer

# ---- Config ----
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "ride-events"

ZONES = ["downtown", "midtown", "uptown", "airport", "suburbs"]
EVENT_TYPES = ["trip_started", "trip_completed", "driver_ping"]

LATE_EVENT_PROBABILITY = 0.05  # 5% of events arrive late
LATE_MIN_SECONDS = 30
LATE_MAX_SECONDS = 90

EVENTS_PER_SECOND = 5


def delivery_report(err, msg):
    """Called once for each produced message to indicate delivery result."""
    if err is not None:
        print(f"Delivery failed for record {msg.key()}: {err}")
    else:
        print(f"Produced to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()} key={msg.key()}")


def generate_event():
    zone = random.choice(ZONES)
    event_type = random.choice(EVENT_TYPES)
    trip_id = str(uuid.uuid4())
    driver_id = f"driver_{random.randint(1, 50)}"
    fare = round(random.uniform(5.0, 75.0), 2) if event_type == "trip_completed" else None

    true_event_time = datetime.now(timezone.utc)

    # Decide if this event is "late" i.e. we delay sending it, but
    # event_time in the payload still reflects when it actually happened.
    is_late = random.random() < LATE_EVENT_PROBABILITY

    event = {
        "trip_id": trip_id,
        "driver_id": driver_id,
        "zone": zone,
        "event_type": event_type,
        "fare": fare,
        "event_time": true_event_time.isoformat(),
    }

    return event, zone, is_late


def send_event(producer, event, key):
    producer.produce(
        TOPIC,
        key=key.encode("utf-8"),
        value=json.dumps(event).encode("utf-8"),
        callback=delivery_report,
    )


def main():
    conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "client.id": "ride-event-producer",
    }
    producer = Producer(conf)

    print(f"Starting producer, sending to topic '{TOPIC}'. Ctrl+C to stop.")

    late_queue = []  # holds (send_at_timestamp, event, key) for delayed sends

    try:
        while True:
            event, zone, is_late = generate_event()

            if is_late:
                delay = random.uniform(LATE_MIN_SECONDS, LATE_MAX_SECONDS)
                send_at = time.time() + delay
                late_queue.append((send_at, event, zone))
                print(f"Queued LATE event (zone={zone}) to send in {delay:.1f}s")
            else:
                send_event(producer, event, zone)

            # Check if any queued late events are ready to send now
            still_pending = []
            now = time.time()
            for send_at, queued_event, queued_key in late_queue:
                if now >= send_at:
                    print(f"Sending LATE event now (zone={queued_key})")
                    send_event(producer, queued_event, queued_key)
                else:
                    still_pending.append((send_at, queued_event, queued_key))
            late_queue = still_pending

            producer.poll(0)
            time.sleep(1.0 / EVENTS_PER_SECOND)

    except KeyboardInterrupt:
        print("\nStopping producer, flushing remaining messages...")
    finally:
        producer.flush()


if __name__ == "__main__":
    main()
