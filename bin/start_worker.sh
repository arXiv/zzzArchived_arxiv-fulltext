#!/bin/bash -e

set -e

if [ -z ${AWS_SHARED_CREDENTIALS_FILE} ]; then
    pipenv run celery worker $@
else
    while [ ! -f ${AWS_SHARED_CREDENTIALS_FILE} ]; do
        sleep 1
        echo "Waiting for AWS credentials at "${AWS_SHARED_CREDENTIALS_FILE}
    done


    pipenv run celery worker $@ &
    WORKER_PID=$!

    echo "Started celery as "${WORKER_PID}

    CREDS_HASH=$(md5sum ${AWS_SHARED_CREDENTIALS_FILE})

    while true; do
      if [ "${CREDS_HASH}" != "$(md5sum ${AWS_SHARED_CREDENTIALS_FILE})" ]; then
        echo "Hash has changed, stopping celery"
        kill ${WORKER_PID}
        pipenv run celery worker $@ &
        WORKER_PID=$!
        echo "Started celery as "${WORKER_PID}
        CREDS_HASH=$(md5sum ${AWS_SHARED_CREDENTIALS_FILE})
      fi
      sleep 1;
    done
fi
