#!/bin/bash

docker run -it --rm \
	--name zookeeper \
	--network app_default \
	-e ZOOKEEPER_CLIENT_PORT=2181 \
	-e ZOOKEEPER_TICK_TIME=2000 \
	confluentinc/cp-zookeeper:7.2.0

