#!/bin/bash
docker logs -f redpanda_examples-redpanda-connect-1 | while read line; do
  if [[ "$line" == *"No rows returned from database. Shutting down."* ]]; then
    docker stop redpanda_examples-redpanda-connect-1
  fi
done