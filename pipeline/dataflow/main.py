from datetime import datetime
import uuid
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.window import TimestampedValue, Sessions
import json
import random
import time
from pprint import pprint

def decode_utf8(element) -> str:
    key, value = element
    return f"Key: {key.decode('utf-8') if key else None}, Value: {value.decode('utf-8') if value else None}"

def generate_synthetic_events(num_events=10):
    user_ids = [f"user_{i}" for i in range(1, 10)]
    page_ids = [f"page_{i}" for i in range(1, 15)]
    base_time = int(time.time() * 1000)
    events = []
    for i in range(num_events):
        # Spread timestamps over the last 5 minutes
        event_time = base_time - random.randint(0, 5 * 60 * 1000)
        user_id =  "user_1" # random.choice(user_ids)
        page_id = random.choice(page_ids)
        # Assign a traffic source for each user_id
        user_traffic_sources = {
            user_id: random.choice(["organic", "referral", "direct"])
            for user_id in user_ids
        }
        event = {
            "event_name": "page_view",
            "user_id": user_id,
            "page_id": page_id,
            "timestamp_ms": event_time,
            "timestamp_dtm": f"{datetime.fromtimestamp(event_time / 1000)}",
            "event_params": {
                "page_title": f"Title for {page_id}",
                "traffic_source": user_traffic_sources[user_id]
            }
        }
        events.append((user_id, json.dumps(event)))     # Return a tuple, key-value pair for grouping events
    random.shuffle(events)  # Shuffle to simulate out-of-order arrival
    for event in events:
        yield event

def add_time_spent_on_page(elements):
    # Assign the tuple into variables
    user_id, events = elements

    parsed_events = [json.loads(e) for e in events]
    parsed_events.sort(key=lambda e: e["timestamp_ms"])  # Sort events by timestamp
    results = []
    session_id = str(uuid.uuid4())

    for i, event in enumerate(parsed_events, start=1):
        if i < len(parsed_events):
            # Time to next event in seconds
            time_spent = (parsed_events[i]["timestamp_ms"] - event["timestamp_ms"]) // 1000
        else:
            # Last event in session, set to 0 or a default (e.g., engaged_time)
            time_spent = event["event_params"].get("engaged_time", 0)
        event["time_spent_on_page_seconds"] = time_spent
        event["previous_time"] = parsed_events[i-2]["timestamp_dtm"]
        event["sequence_number"] = i
        event["session_id"] = session_id
        results.append(event)
    return results


def run():
    options = PipelineOptions(
        runner='DirectRunner',
        streaming=True  # Set to True if you want to use an unbounded source
    )
    with beam.Pipeline(options=options) as p:
        (
            p
            | "GenerateEvents" >> beam.Create(list(generate_synthetic_events(10)))
            | "AddTimestamps" >> beam.Map(lambda kv: TimestampedValue(kv, json.loads(kv[1])["timestamp_ms"] / 1000))
            | "WindowIntoSessions" >> beam.WindowInto(Sessions(gap_size=300))
            | "GroupByUser" >> beam.GroupByKey()
            | "AddTimeSpent" >> beam.FlatMap(add_time_spent_on_page)
            | "PrintEvents" >> beam.Map(print)
        )

if __name__ == "__main__":
    run()

