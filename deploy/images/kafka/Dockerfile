# The platform's Kafka image: the stock Strimzi image plus the Streamcell plugin jar (authorizer and topic
# policies). Not a fork: the jar uses Kafka's public extension points, and runs on brokers and controllers.
ARG BASE=quay.io/strimzi/kafka:1.3.0-kafka-4.3.1
FROM ${BASE}
COPY plugins/kafka/target/streamcell-kafka-plugins.jar /opt/kafka/libs/streamcell-kafka-plugins.jar
