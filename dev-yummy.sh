#!/bin/bash

docker run -it -p 8887:8888 --rm --shm-size=5.09gb --name yummy \
	--network app_default \
	-v $(pwd)/notebooks:/home/jovyan/notebooks \
	-v $(pwd)/../feast-yummy:/home/jovyan/feast-yummy \
	-v $(pwd)/../feast-schema:/home/jovyan/feast-schema \
	qooba/feast:yummy_spark /bin/bash
