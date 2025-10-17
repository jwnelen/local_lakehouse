#!/bin/bash
# Test script to debug Airflow connection creation

# Find scheduler container
SCHEDULER_CONTAINER=$(docker ps --filter "name=scheduler" --format "{{.Names}}" | head -n 1)

if [ -z "$SCHEDULER_CONTAINER" ]; then
    echo "ERROR: Could not find Airflow scheduler container"
    echo "Running containers:"
    docker ps --format "table {{.Names}}\t{{.Status}}"
    exit 1
fi

echo "Found scheduler container: $SCHEDULER_CONTAINER"
echo ""

# Try to create connection with full error output
echo "Attempting to create postgres_serving connection..."
docker exec "$SCHEDULER_CONTAINER" airflow connections add 'postgres_serving' \
    --conn-type 'postgres' \
    --conn-host 'postgres_serving' \
    --conn-schema 'analytics_db' \
    --conn-login 'lakehouse_user' \
    --conn-password 'lakehouse_pass' \
    --conn-port 5432

EXIT_CODE=$?
echo ""

if [ $EXIT_CODE -eq 0 ]; then
    echo "✓ Connection created successfully"
else
    echo "✗ Connection creation failed with exit code: $EXIT_CODE"
    echo ""
    echo "Checking if connection already exists..."
    docker exec "$SCHEDULER_CONTAINER" airflow connections get 'postgres_serving'
fi
