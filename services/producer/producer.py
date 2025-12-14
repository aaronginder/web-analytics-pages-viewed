import json, time, random, argparse
from kafka import KafkaProducer

def generate_synthetic_events(num_events=10):
    user_ids = [f"user_{i}" for i in range(1, 10)]
    page_ids = [f"page_{i}" for i in range(1, 15)]
    for _ in range(num_events):
        event = {
            "event_name": "page_view",
            "user_id": random.choice(user_ids),
            "page_id": random.choice(page_ids),
            "timestamp_ms": int(time.time() * 1000),
            "event_params": {
                "engaged_time": random.randint(1, 300),
                "page_title": f"Title for {random.choice(page_ids)}",
                "traffic_source": random.choice(["organic", "referral", "direct"])
            }
        }
        yield event

def main():

    parser = argparse.ArgumentParser()
    parser.add_argument('--bootstrap_servers', default='localhost:29092', help='Kafka bootstrap servers')
    parser.add_argument('--topic', default='web-analytics-events', help='Kafka topic name')
    parser.add_argument('--num_events', type=int, default=1000, help='Number of events to produce')
    args = parser.parse_args()
    print(args.bootstrap_servers, args.topic, args.num_events)

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    for event in generate_synthetic_events(args.num_events):
        producer.send(topic=args.topic, value=event, key=event['user_id'].encode('utf-8'))
        print(f"Produced: {event}")
        time.sleep(random.uniform(0.1, 5.0))
    print("done")
    producer.flush()


if __name__ == "__main__":
    main()