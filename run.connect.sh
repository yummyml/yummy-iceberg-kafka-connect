#!/bin/bash


docker run -it --name connect --rm --network=app_default -p 8083:8083 \
	-e GROUP_ID=1 \
	-e CONFIG_STORAGE_TOPIC=my-connect-configs \
	-e OFFSET_STORAGE_TOPIC=my-connect-offsets \
	-e BOOTSTRAP_SERVERS=kafka:9092 \
	-e CONNECT_TOPIC_CREATION_ENABLE=true \
	-v $(pwd)/.aws:/kafka/.aws/config \
	-v $(pwd)/kafka-connect-iceberg-sink/kafka-connect-iceberg-sink-0.1.3-shaded.jar:/kafka/connect/kafka-connect-iceberg-sink/kafka-connect-iceberg-sink-0.1.3-shaded.jar \
	debezium/connect:2.0
