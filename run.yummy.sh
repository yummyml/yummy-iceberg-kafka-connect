#!/bin/bash

docker run -it -p 8887:8888 --rm --shm-size=5.09gb --name yummy \
	--network app_default \
	-v $(pwd)/notebooks:/home/jovyan/notebooks \
	qooba/yummy:v0.0.2_spark /home/jovyan/notebooks/jupyter.sh
