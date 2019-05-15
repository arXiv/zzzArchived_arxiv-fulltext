"""
Celery configuration module.

See `the celery docs
<http://docs.celeryproject.org/en/latest/userguide/configuration.html>`_.
"""

import os
from urllib import parse

broker_url = "redis://%s/0" % os.environ.get('REDIS_ENDPOINT')
result_backend = "redis://%s/0" % os.environ.get('REDIS_ENDPOINT')
backend = results = result_backend
broker_transport_options = {
    # 'region': os.environ.get('AWS_REGION', 'us-east-1'),
    'queue_name_prefix': 'fulltext-',
}
worker_prefetch_multiplier = 1
task_acks_late = True
task_default_queue = 'fulltext-worker'
task_always_eager = bool(int(os.environ.get('CELERY_ALWAYS_EAGER', '0')))
