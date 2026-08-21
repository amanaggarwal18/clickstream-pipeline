"""
Clickstream event generator.

Simulates a website emitting user activity events and writes them as
newline-delimited JSON files into a partitioned raw/ folder, mimicking
how real log pipelines land data (e.g. from a web server or a Kafka
consumer dumping to a data lake).

Usage:
    python generate_events.py --num-events 5000 --output-dir ./raw
"""

import argparse
import json
import os
import random
import uuid
from datetime import datetime, timedelta, timezone


# A handful of fake users and pages so the data has repeat structure
# (needed for sessionization and funnel analysis later on).
USER_IDS = [f"user_{i:03d}" for i in range(1, 201)]  # 200 fake users

PAGES = [
    "/home",
    "/product/101",
    "/product/102",
    "/product/103",
    "/product/104",
    "/cart",
    "/checkout",
    "/search",
    "/category/electronics",
    "/category/clothing",
]

# Roughly realistic funnel proportions: lots of page views, fewer clicks,
# even fewer add-to-carts, fewest purchases.
EVENT_TYPE_WEIGHTS = {
    "page_view": 0.55,
    "click": 0.25,
    "add_to_cart": 0.13,
    "purchase": 0.07,
}


def weighted_event_type() -> str:
    types = list(EVENT_TYPE_WEIGHTS.keys())
    weights = list(EVENT_TYPE_WEIGHTS.values())
    return random.choices(types, weights=weights, k=1)[0]


def generate_event(base_time: datetime, user_session_map: dict) -> dict:
    user_id = random.choice(USER_IDS)

    # Reuse an existing session for this user ~70% of the time, to
    # simulate multiple events happening within one browsing session.
    if user_id in user_session_map and random.random() < 0.7:
        session_id = user_session_map[user_id]
    else:
        session_id = f"sess_{uuid.uuid4().hex[:8]}"
        user_session_map[user_id] = session_id

    # Jitter the timestamp a bit within the current minute so events
    # aren't all identical.
    event_time = base_time + timedelta(seconds=random.randint(0, 59))

    return {
        "event_id": str(uuid.uuid4()),
        "user_id": user_id,
        "session_id": session_id,
        "event_type": weighted_event_type(),
        "page_url": random.choice(PAGES),
        "timestamp": event_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


def write_batch(events: list, output_dir: str, batch_time: datetime) -> str:
    """Write a batch of events to a partitioned JSON file:
    raw/dt=YYYY-MM-DD/hour=HH/events_<timestamp>.json
    """
    dt_str = batch_time.strftime("%Y-%m-%d")
    hour_str = batch_time.strftime("%H")
    partition_dir = os.path.join(output_dir, f"dt={dt_str}", f"hour={hour_str}")
    os.makedirs(partition_dir, exist_ok=True)

    filename = f"events_{batch_time.strftime('%Y%m%dT%H%M%S')}_{uuid.uuid4().hex[:6]}.json"
    filepath = os.path.join(partition_dir, filename)

    # Newline-delimited JSON: one event per line. This is the standard
    # format Spark's JSON reader expects for efficient parallel reads.
    with open(filepath, "w") as f:
        for event in events:
            f.write(json.dumps(event) + "\n")

    return filepath


def main():
    parser = argparse.ArgumentParser(description="Generate synthetic clickstream events")
    parser.add_argument("--num-events", type=int, default=5000, help="Total number of events to generate")
    parser.add_argument("--batch-size", type=int, default=500, help="Events per output file")
    parser.add_argument("--output-dir", type=str, default="./raw", help="Root output directory")
    parser.add_argument(
        "--hours-back",
        type=int,
        default=24,
        help="Spread generated events across this many past hours (simulates historical data)",
    )
    args = parser.parse_args()

    random.seed(42)  # reproducible runs while you're learning/debugging

    user_session_map = {}
    now = datetime.now(timezone.utc)
    events_written = 0
    files_written = []

    num_batches = args.num_events // args.batch_size
    for batch_num in range(num_batches):
        # Spread batches across the requested time window so the data
        # looks like it was collected over real time, not all at once.
        hours_offset = random.uniform(0, args.hours_back)
        batch_time = now - timedelta(hours=hours_offset)

        batch = [generate_event(batch_time, user_session_map) for _ in range(args.batch_size)]
        filepath = write_batch(batch, args.output_dir, batch_time)
        files_written.append(filepath)
        events_written += len(batch)

    print(f"Generated {events_written} events across {len(files_written)} files in '{args.output_dir}'")
    print("Sample file:", files_written[0] if files_written else "none")


if __name__ == "__main__":
    main()
