docker-compose up

# Create connectors
curl -s -X POST -H 'Content-Type: application/json' --data @connector-mqtt-source.json http://localhost:8083/connectors
curl -s -X POST -H 'Content-Type: application/json' --data @connector-mqtt-sink.json http://localhost:8083/connectors

# Update connectors
# Create json with just config file.
curl -s -X PUT -H 'Content-Type: application/json' --data @update.json http://localhost:8083/connectors/NAME/config


# Check source and sink are created
curl -sS localhost:8083/connector-plugins | jq -c '.[] | select( .class | contains("Mqtt") )'

# Send messages
docker-compose scale mqtt-client=1

# Check messages on kafka topic
kafka-console-consumer --bootstrap-server kafka:9092 --topic connect-custom --from-beginning

# Build consumer before scaling
docker build consumer -t test-kafka-emqx-cluster_kafka-consumer:latest