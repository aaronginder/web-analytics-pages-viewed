#!/bin/bash

# Wait for Kafka to start
while ! nc -z localhost 9092; do   
  echo "Waiting for Kafka to be ready..."
  sleep 2
done

topics=(transactions customer interactions)

for topic in "${topics[@]}"; do
  kafka-topics --delete --topic $topic --bootstrap-server localhost:9092
done
