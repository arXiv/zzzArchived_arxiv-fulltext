#!/bin/bash -e

set -e

if [ -z ${AWS_SHARED_CREDENTIALS_FILE} ]; then
    pipenv run python3.6 /opt/arxiv/start_agent.py $@
else
    while [ ! -f ${AWS_SHARED_CREDENTIALS_FILE} ]; do
        sleep 1
        echo "Waiting for AWS credentials at "${AWS_SHARED_CREDENTIALS_FILE}
    done

    sleep 30
    pipenv run python3.6 /opt/arxiv/start_agent.py $@ &
    WORKER_PID=$!

    echo "Started agent as "${WORKER_PID}

    CREDS_HASH=$(md5sum ${AWS_SHARED_CREDENTIALS_FILE})

    while true; do
      if [ "${CREDS_HASH}" != "$(md5sum ${AWS_SHARED_CREDENTIALS_FILE})" ]; then
        echo "Hash has changed, stopping agent"
        kill ${WORKER_PID}
        sleep 30
        pipenv run python3.6 /opt/arxiv/start_agent.py $@ &
        WORKER_PID=$!
        echo "Started agent as "${WORKER_PID}
        CREDS_HASH=$(md5sum ${AWS_SHARED_CREDENTIALS_FILE})
      fi
      sleep 1;
    done
fi
