#!/bin/bash

# Chaos Engineering Simulation Script

case $1 in
  "payment")
    echo "Simulating Payment Service downtime..."
    docker-compose stop payment-service
    sleep 10
    docker-compose start payment-service
    echo "Payment service restored. Check DLQ messages."
    ;;
  "kafka")
    echo "Simulating Kafka broker outage..."
    docker-compose stop kafka
    sleep 5
    docker-compose start kafka
    echo "Kafka restored. Verify consumer reconnections."
    ;;
  "load")
    echo "Starting high-traffic load test (1000 RPS target)..."
    locust -f load-test/locustfile.py --headless -u 100 -r 10 --run-time 1m --host http://localhost:8001
    ;;
  *)
    echo "Usage: $0 {payment|kafka|load}"
    ;;
esac
