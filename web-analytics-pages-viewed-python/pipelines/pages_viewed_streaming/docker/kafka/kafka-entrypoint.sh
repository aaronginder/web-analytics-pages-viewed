#!/bin/bash

# Function to check if a topic exists
check_topic_exists() {
  TOPIC_NAME=$1
  if kafka-topics --bootstrap-server localhost:9092 --list | grep -q "^$TOPIC_NAME$"; then
    echo "Topic $TOPIC_NAME already exists"
    return 0
  else
    echo "Topic $TOPIC_NAME does not exist"
    return 1
  fi
}

# Function to create a Kafka topic
create_topic() {
  TOPIC_NAME=$1
  PARTITIONS=$2
  REPLICATION_FACTOR=$3

  kafka-topics --bootstrap-server localhost:9092 \
    --create \
    --topic "$TOPIC_NAME" \
    --partitions "$PARTITIONS" \
    --replication-factor "$REPLICATION_FACTOR"
}

# List of topics, partitions, and replication factor (format: topic_name:partitions:replication_factor)
TOPICS_LIST=(
  "transactions:1:1"
  "customers:2:1"
  "interactions:3:1"
)

# Wait for Kafka to be ready (using localhost, but adjust as necessary for your Docker setup)
echo "Waiting for Kafka to start..."
sleep 20  # Adjust this sleep to give Kafka time to start

# Check and create each topic
for TOPIC in "${TOPICS_LIST[@]}"; do
  IFS=":" read -r TOPIC_NAME PARTITIONS REPLICATION_FACTOR <<< "$TOPIC"
  
  if ! check_topic_exists "$TOPIC_NAME"; then
    echo "Creating topic $TOPIC_NAME with $PARTITIONS partitions and $REPLICATION_FACTOR replication factor"
    create_topic "$TOPIC_NAME" "$PARTITIONS" "$REPLICATION_FACTOR"
  fi
done

# Start Kafka server after topics are created
echo "Starting Kafka server..."
exec /etc/confluent/docker/run  # This starts the Kafka broker