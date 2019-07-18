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
"""
Prevent the worker from taking more than one task at a time.

In general we want to treat our workers as ephemeral. Even though Celery itself
is pretty solid runtime, we may lose the underlying machine with little or no
warning. The less state held by the workers the better.
"""

task_acks_late = True
"""
Do not acknowledge a task until it has been completed.

As described for :const:`.worker_prefetch_multiplier`, we assume that workers
will disappear without warning. This ensures that a task will can be executed
again if the worker crashes during execution.
"""

task_default_queue = 'fulltext-worker'
"""
Name of the queue for plain text extraction tasks.

Using different queue names allows us to run many different queues on the same
underlying transport (e.g. Redis cluster).
"""

task_always_eager = bool(int(os.environ.get('CELERY_ALWAYS_EAGER', '0')))
"""
If True, tasks will be executed in the same process as the dispatcher.

This is only useful (and should only be used) for testing purposes.
"""
