#!/bin/bash


if [ ! -d "kafka-connect-iceberg-sink" ]
then
    mkdir kafka-connect-iceberg-sink
    cd kafka-connect-iceberg-sink
    curl -LO https://github.com/getindata/kafka-connect-iceberg-sink/releases/download/0.1.3/kafka-connect-iceberg-sink-0.1.3-shaded.jar
fi

docker run -it --name connect --rm --network=app_default -p 8083:8083 \
	-e GROUP_ID=1 \
	-e CONFIG_STORAGE_TOPIC=my-connect-configs \
	-e OFFSET_STORAGE_TOPIC=my-connect-offsets \
	-e BOOTSTRAP_SERVERS=kafka:9092 \
	-e CONNECT_TOPIC_CREATION_ENABLE=true \
	-v $(pwd)/kafka-connect-iceberg-sink/kafka-connect-iceberg-sink-0.1.3-shaded.jar:/kafka/connect/kafka-connect-iceberg-sink/kafka-connect-iceberg-sink-0.1.3-shaded.jar \
	debezium/connect:2.0
