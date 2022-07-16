#!/bin/bash

docker run --rm -it -p 9000:9000 \
	-p 9001:9001 --name minio \
	-v $(pwd)/minio-data:/data \
	--network app_default \
	minio/minio server /data --console-address ":9001"
