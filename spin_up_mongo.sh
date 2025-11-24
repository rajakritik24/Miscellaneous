#!/bin/bash

# Spin up a standard MongoDB instance
# NOTE: Standard local MongoDB does NOT support Atlas Vector Search ($vectorSearch).
# This will allow you to test connection, collection creation, and ingestion, 
# but create_vector_index and search operations will likely fail or behave differently.

docker run -d \
  --name mongo-vector-test \
  -p 27017:27017 \
  -e MONGO_INITDB_ROOT_USERNAME=admin \
  -e MONGO_INITDB_ROOT_PASSWORD=password \
  mongo:latest

echo "MongoDB started on localhost:27017"
echo "Connection String: mongodb://admin:password@localhost:27017"
