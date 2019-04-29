#!/bin/bash

set -o pipefail
set -o errexit
set -o nounset

# Used to deploy

export LABEL=$1
export LOGLEVEL=40
export IMAGE_NAME=arxiv/plaintext-${LABEL}
if [ -z "${TRAVIS_TAG}" ]; then
    export SOURCE_REF=${TRAVIS_COMMIT}
else
    export SOURCE_REF=${TRAVIS_TAG}
fi

git fetch --unshallow || echo "Repository is already complete"
docker login -u "$DOCKERHUB_USERNAME" -p "$DOCKERHUB_PASSWORD"
docker build ./ -t ${IMAGE_NAME}:${SOURCE_REF} -f ./Dockerfile-${LABEL}
docker push ${IMAGE_NAME}:${SOURCE_REF}
