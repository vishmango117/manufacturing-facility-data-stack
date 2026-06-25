FROM confluentinc/cp-kafka-connect:7.6.1

# Debezium Postgres source + Confluent JDBC sink (Avro converters ship in base).
RUN confluent-hub install --no-prompt debezium/debezium-connector-postgresql:2.5.4 \
 && confluent-hub install --no-prompt confluentinc/kafka-connect-jdbc:10.7.6
