#!/bin/bash

dockerd -D &

cd /opt/arxiv/extractor
docker build ./ -t arxiv/fulltext-extractor:test

cd /opt/arxiv
docker build ./ -t arxiv/fulltext-api:test -f ./Dockerfile-api
docker build ./ -t arxiv/fulltext-worker:test -f ./Dockerfile-worker
#docker build ./ -t arxiv/fulltext-agent:test -f ./Dockerfile-agent

docker pull redis
docker pull atlassianlabs/localstack

docker network create test

docker run --network=test \
    --name=the-redis \
    -d redis

docker run -d -p 4567-4578:4567-4578 \
    -p 8080:8080 \
    --network=test \
    --name=localstack \
    -e "USE_SSL=true" \
    atlassianlabs/localstack

docker run -d --network=test \
   -e "REDIS_ENDPOINT=the-redis:6379" \
   -e "AWS_ACCESS_KEY_ID=foo" \
   -e "AWS_SECRET_ACCESS_KEY=bar" \
   -e "CLOUDWATCH_ENDPOINT=https://localstack:4582" \
   -e "DYNAMODB_ENDPOINT=https://localstack:4569" \
   -e "DYNAMODB_VERIFY=false" \
   -e "CLOUDWATCH_VERIFY=false" \
   -e "LOGLEVEL=10" \
   -v /var/run/docker.sock:/var/run/docker.sock \
   arxiv/fulltext-worker:test

docker run -d --network=test \
    -e "REDIS_ENDPOINT=the-redis:6379" \
    -e "AWS_ACCESS_KEY_ID=foo" \
    -e "AWS_SECRET_ACCESS_KEY=bar" \
    -e "CLOUDWATCH_ENDPOINT=https://localstack:4582" \
    -e "DYNAMODB_ENDPOINT=https://localstack:4569" \
    -e "DYNAMODB_VERIFY=false" \
    -e "CLOUDWATCH_VERIFY=false" \
    -e "LOGLEVEL=10" \
    arxiv/fulltext-api:test

curl \
    -H "Content-Type: application/json" \
    -X POST \
    -d '{"document_id":"1606.00128","url":"https://arxiv.org/pdf/1606.00128"}'
