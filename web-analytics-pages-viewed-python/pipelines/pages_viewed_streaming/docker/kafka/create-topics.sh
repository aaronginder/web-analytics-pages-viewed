#!/bin/bash

# Wait for Kafka to start
while ! nc -z localhost 9092; do   
  echo "Waiting for Kafka to be ready..."
  sleep 2
done

topics=(products_viewed)

for topic in "${topics[@]}"; do
  kafka-topics --create --topic $topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
done
