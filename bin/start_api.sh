#!/bin/bash

set -e

if [ -z ${AWS_SHARED_CREDENTIALS_FILE}]; then
    echo "No shared credentials file"
else
    while [ ! -f ${AWS_SHARED_CREDENTIALS_FILE} ]; do
        sleep 1
        echo "Waiting for AWS credentials at "${AWS_SHARED_CREDENTIALS_FILE}
    done
    echo "Found shared credentials file"
fi

uwsgi -H $(pipenv --venv) "$@"
