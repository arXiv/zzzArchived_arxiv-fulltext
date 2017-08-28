"""
Celery configuration module.

See `the celery docs
<http://docs.celeryproject.org/en/latest/userguide/configuration.html>`_.
"""

import os
from urllib import parse

AWS_ACCESS_KEY_ID = os.environ.get('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY', '')
# AWS_SECRET_KEY = parse.quote(AWS_SECRET_KEY, safe='')
# broker_url = "sqs://{}:{}@".format(AWS_ACCESS_KEY, AWS_SECRET_KEY)
FULLTEXT_REDIS_ENDPOINT = os.environ.get('FULLTEXT_REDIS_ENDPOINT')
broker_url = "redis://%s/0" % FULLTEXT_REDIS_ENDPOINT
result_backend = "redis://%s/0" % FULLTEXT_REDIS_ENDPOINT
broker_transport_options = {
    'region': os.environ.get('AWS_REGION', 'us-east-1'),
    'queue_name_prefix': 'fulltext-',
}
worker_prefetch_multiplier = 0
