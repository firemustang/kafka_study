#!/bin/bash

set -e

echo "Starting Kafka Connect and CDC setup..."

echo "Starting Docker infrastructure..."
docker-compose down --remove-orphans 2>/dev/null || true
docker-compose up -d

echo "Waiting for services to be ready..."

echo "Waiting for PostgreSQL..."
until docker exec postgres pg_isready -U postgres -d orderdb > /dev/null 2>&1; do
    echo "   PostgreSQL is not ready yet, waiting..."
    sleep 2
done
echo "PostgreSQL is ready!"

echo "Waiting for Kafka..."
until docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list > /dev/null 2>&1; do
    echo " Kafka is not ready yet, waiting..."
    sleep 2
done
echo "Kafka is ready!"

echo "Waiting for Kafka Connect (this may take up to 2 minutes for plugin loading)..."
KAFKA_CONNECT_TIMEOUT=60
KAFKA_CONNECT_COUNTER=0

until curl -s http://localhost:8083/connectors > /dev/null 2>&1; do
    echo "  Kafka Connect is not ready yet, waiting... ($KAFKA_CONNECT_COUNTER/$KAFKA_CONNECT_TIMEOUT)"
    sleep 3
    KAFKA_CONNECT_COUNTER=$((KAFKA_CONNECT_COUNTER + 1))

    if [ $KAFKA_CONNECT_COUNTER -ge $KAFKA_CONNECT_TIMEOUT ]; then
        echo "Kafka Connect failed to start within timeout period"
        echo "Check Kafka Connect logs: docker logs kafka-connect"
        exit 1
    fi
done
echo "Kafka Connect is ready!"

echo "Waiting for Spring Boot application to be ready..."
SPRING_TIMEOUT=30
SPRING_COUNTER=0

until curl -s http://localhost:8080/actuator/health > /dev/null 2>&1; do
    echo " Waiting for Spring Boot application... ($SPRING_COUNTER/$SPRING_TIMEOUT)"
    sleep 2
    SPRING_COUNTER=$((SPRING_COUNTER + 1))

    if [ $SPRING_COUNTER -ge $SPRING_TIMEOUT ]; then
        echo "Spring Boot application is not ready. Please start it first!"
        echo "Run: ./gradlew bootRun"
        exit 1
    fi
done
echo "Spring Boot application is ready!"

echo "Setting up PostgreSQL for CDC..."
docker exec -i postgres psql -U postgres -d orderdb << 'EOF'
DROP PUBLICATION IF EXISTS debezium_publication;
CREATE PUBLICATION debezium_publication FOR TABLE public.orders;

DO $$
BEGIN
    RAISE NOTICE 'Created publication: debezium_publication';
END $$;

SELECT 'wal_level: ' || setting FROM pg_settings WHERE name = 'wal_level';
SELECT 'max_wal_senders: ' || setting FROM pg_settings WHERE name = 'max_wal_senders';
SELECT 'max_replication_slots: ' || setting FROM pg_settings WHERE name = 'max_replication_slots';

SELECT 'Publications: ' || string_agg(pubname, ', ') FROM pg_publication;
SELECT 'Publication tables: ' || string_agg(schemaname||'.'||tablename, ', ') FROM pg_publication_tables WHERE pubname = 'debezium_publication';
EOF

# 이 방식이 정상적 처리 확인인지 모르겠는데... 우선 이렇게 확인 체크가 가능하기 떄문에 이런 식으로 검증
# 단순히 자동화를 위해서 존재하는 스크립트라는걸 명심!
if [ $? -eq 0 ]; then
    echo "PostgreSQL CDC setup completed!"
else
    echo "PostgreSQL CDC setup failed!"
    exit 1
fi

echo "Registering Debezium Connector..."

curl -s -X DELETE http://localhost:8083/connectors/order-postgres-connector > /dev/null 2>&1 || true
sleep 2

CONNECTOR_RESPONSE=$(curl -s -X POST \
  http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d '{
    "name": "order-postgres-connector",
    "config": {
      "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
      "database.hostname": "postgres",
      "database.port": "5432",
      "database.user": "postgres",
      "database.password": "password",
      "database.dbname": "orderdb",
      "database.server.name": "dbserver1",
      "table.include.list": "public.orders",
      "publication.name": "debezium_publication",
      "slot.name": "debezium_slot",
      "plugin.name": "pgoutput",
      "key.converter": "org.apache.kafka.connect.storage.StringConverter",
      "value.converter": "org.apache.kafka.connect.json.JsonConverter",
      "value.converter.schemas.enable": "false",
      "transforms": "unwrap",
      "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
      "transforms.unwrap.drop.tombstones": "false",
      "topic.prefix": "dbserver1",
      "schema.history.internal.kafka.topic": "dbserver1.schema-changes",
      "schema.history.internal.kafka.bootstrap.servers": "kafka:29092",
      "include.schema.changes": "true",
      "decimal.handling.mode": "double",
      "time.precision.mode": "adaptive"
    }
  }')

if echo "$CONNECTOR_RESPONSE" | grep -q '"name"'; then
    echo "Debezium Connector registered successfully!"
else
    echo "Failed to register Debezium Connector!"
    echo "Response: $CONNECTOR_RESPONSE"
    exit 1
fi

echo "Checking connector status..."
for i in {1..10}; do
    CONNECTOR_STATUS=$(curl -s http://localhost:8083/connectors/order-postgres-connector/status)

    if echo "$CONNECTOR_STATUS" | grep -q '"state":"RUNNING"'; then
        echo "Connector is RUNNING!"
        break
    elif echo "$CONNECTOR_STATUS" | grep -q '"state":"FAILED"'; then
        echo "Connector FAILED!"
        echo "$CONNECTOR_STATUS"
        exit 1
    else
        echo "   Connector status: $(echo "$CONNECTOR_STATUS" | grep -o '"state":"[^"]*"' || echo 'Unknown')"
        sleep 3
    fi
done

echo ""
echo "Kafka Connect and CDC setup completed!"
echo "=================================="
echo "Service URLs:"
echo "   - Kafka UI:        http://localhost:9090"
echo "   - Kafka Connect:   http://localhost:8083"
echo "   - Spring Boot:     http://localhost:8080"
echo ""
echo "CDC Monitoring:"
echo "   - Monitor CDC events: tail -f /tmp/spring-boot.log | grep 'Raw CDC message'"
echo "   - Check connector status: curl http://localhost:8083/connectors/order-postgres-connector/status"
echo "   - View Kafka topics: docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list"
echo ""