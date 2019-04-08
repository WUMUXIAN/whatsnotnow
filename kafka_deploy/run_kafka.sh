#!/bin/bash

docker run -d \
    --net=zookeeper_default \
    --name=kafka \
    -p 29092:29092 \
    -v $(pwd)/data:/var/lib/kafka/data \
    --link zookeeper_zoo1_1:zookeeper \
    -e KAFKA_BROKER_ID=1 \
    -e KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181/kafka \
    -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:29092 \
    -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
    confluentinc/cp-kafka:5.1.2
