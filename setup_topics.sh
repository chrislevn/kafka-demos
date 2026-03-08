#!/bin/bash
# =============================================================
# Topic Setup Script
# Creates the Kafka topics needed for each demo level.
# Run this AFTER docker-compose up and BEFORE running any demos.
# =============================================================

set -e

BROKER="localhost:9092"

echo "============================================"
echo "  Creating Kafka Topics for All Levels"
echo "============================================"
echo

# Wait for Kafka to be ready
echo "Waiting for Kafka to be ready..."
until docker exec kafka kafka-topics --bootstrap-server $BROKER --list > /dev/null 2>&1; do
    sleep 1
    printf "."
done
echo " Ready!"
echo

# Level 1: Single partition (ordering guaranteed)
echo "Creating Level 1 topic: orders-level1 (1 partition)"
docker exec kafka kafka-topics --create \
    --bootstrap-server $BROKER \
    --topic orders-level1 \
    --partitions 1 \
    --replication-factor 1 \
    --if-not-exists
echo

# Level 2: Multiple partitions (ordering lost)
echo "Creating Level 2 topic: orders-level2 (4 partitions)"
docker exec kafka kafka-topics --create \
    --bootstrap-server $BROKER \
    --topic orders-level2 \
    --partitions 4 \
    --replication-factor 1 \
    --if-not-exists
echo

# Level 3: Multiple partitions with key-based routing (ordering restored)
echo "Creating Level 3 topic: orders-level3 (4 partitions)"
docker exec kafka kafka-topics --create \
    --bootstrap-server $BROKER \
    --topic orders-level3 \
    --partitions 4 \
    --replication-factor 1 \
    --if-not-exists
echo

# Verify
echo "============================================"
echo "  Topics Created:"
echo "============================================"
docker exec kafka kafka-topics --list --bootstrap-server $BROKER
echo
echo "Done! You can now run the demos."
