#!/bin/bash

docker run -it --name postgres --rm --network=app_default -e POSTGRES_PASSWORD=postgres \
  -p 5432:5432 postgres:12.11 -c wal_level=logical
