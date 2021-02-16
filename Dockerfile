# arxiv/fulltext

ARG BASE_VERSION=0.17.0

FROM arxiv/base:${BASE_VERSION}

WORKDIR /opt/arxiv

EXPOSE 8000

ENV PATH="/opt/arxiv:${PATH}" \
    ARXIV_HOME="https://arxiv.org" \
    LOGLEVEL=10 \
    STORAGE_VOLUME=/data \
    KINESIS_STREAM="PDFIsAvailable" \
    KINESIS_SHARD_ID="0" \
    KINESIS_CHECKPOINT_VOLUME="/checkpoint" \
    KINESIS_START_TYPE="AT_TIMESTAMP"

VOLUME /data
VOLUME /checkpoint
VOLUME /pdfs

COPY Pipfile Pipfile.lock /opt/arxiv/
RUN pipenv install && rm -rf ~/.cache/pip

COPY wsgi.py uwsgi.ini /opt/arxiv/
COPY fulltext /opt/arxiv/fulltext/

ENTRYPOINT ["pipenv", "run"]
# CMD ["python", "-m", "fulltext.agent"]
# CMD ["uwsgi", "--ini", "/opt/arxiv/uwsgi.ini"]
# CMD ["celery", "worker", "-A", "fulltext.worker.celery_app", "--loglevel=INFO", "-E", "--concurrency=1"]