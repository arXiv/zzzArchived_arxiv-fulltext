"""
Flask configuration.

Docstrings are from the `Flask configuration documentation
<http://flask.pocoo.org/docs/0.12/config/>`_.
"""
from typing import List, Tuple, Optional
from os import environ
import warnings
import re
import tempfile

NAMESPACE = environ.get('NAMESPACE')
"""Namespace in which this service is deployed; to qualify keys for secrets."""

LOGLEVEL = int(environ.get('LOGLEVEL', '20'))
"""
Logging verbosity.

See `https://docs.python.org/3/library/logging.html#levels`_.
"""

AUTH_UPDATED_SESSION_REF = True
"""Attache auth info at ``request.auth`` instead of ``request.session``."""

WAIT_FOR_SERVICES = bool(int(environ.get('WAIT_FOR_SERVICES', '0')))
"""Enable/disable waiting for upstream services to be available on startup."""

WAIT_ON_STARTUP = int(environ.get('WAIT_ON_STARTUP', '0'))
"""Number of seconds to wait before checking upstream services."""

# --- FLASK + WERKZEUG CONFIGURATION ---

DEBUG = bool(int(environ.get('DEBUG', '0')))
"""enable/disable debug mode"""

TESTING = bool(int(environ.get('TESTING', '0')))
"""enable/disable testing mode"""

PROPAGATE_EXCEPTIONS = \
    True if bool(int(environ.get('PROPAGATE_EXCEPTIONS', '0'))) else None
"""
explicitly enable or disable the propagation of exceptions. If not set or
explicitly set to None this is implicitly true if either TESTING or DEBUG is
true.
"""

PRESERVE_CONTEXT_ON_EXCEPTION: Optional[bool] = None
"""
By default if the application is in debug mode the request context is not
popped on exceptions to enable debuggers to introspect the data. This can be
disabled by this key. You can also use this setting to force-enable it for non
debug execution which might be useful to debug production applications (but
also very risky).
"""
if bool(int(environ.get('PRESERVE_CONTEXT_ON_EXCEPTION', '0'))):
    PRESERVE_CONTEXT_ON_EXCEPTION = True


SECRET_KEY = environ.get('SECRET_KEY', 'asdf1234')
"""
the secret key
"""

SESSION_COOKIE_NAME = environ.get('SESSION_COOKIE_NAME', 'fulltext')
"""
the name of the session cookie
"""

SESSION_COOKIE_DOMAIN = environ.get('SESSION_COOKIE_DOMAIN', None)
"""
the domain for the session cookie. If this is not set, the cookie will be valid
for all subdomains of SERVER_NAME.
"""

SESSION_COOKIE_PATH = environ.get('SESSION_COOKIE_PATH', None)
"""
the path for the session cookie. If this is not set the cookie will be valid
for all of APPLICATION_ROOT or if that is not set for '/'.
"""

SESSION_COOKIE_HTTPONLY \
    = bool(int(environ.get('SESSION_COOKIE_HTTPONLY', '1')))
"""
controls if the cookie should be set with the httponly flag. Defaults to True.
"""

SESSION_COOKIE_SECURE = bool(int(environ.get('SESSION_COOKIE_SECURE', '0')))
"""
controls if the cookie should be set with the secure flag. Defaults to False.
"""

PERMANENT_SESSION_LIFETIME = \
    int(environ.get('PERMANENT_SESSION_LIFETIME', '3600'))
"""
the lifetime of a permanent session as datetime.timedelta object. Starting with
Flask 0.8 this can also be an integer representing seconds.
"""

SESSION_REFRESH_EACH_REQUEST = \
    bool(int(environ.get('SESSION_REFRESH_EACH_REQUEST', '1')))
"""
this flag controls how permanent sessions are refreshed. If set to True (which
is the default) then the cookie is refreshed each request which automatically
bumps the lifetime. If set to False a set-cookie header is only sent if the
session is modified. Non permanent sessions are not affected by this.
"""

USE_X_SENDFILE = bool(int(environ.get('USE_X_SENDFILE', '0')))
"""
enable/disable x-sendfile
"""

LOGGER_NAME = environ.get('LOGGER_NAME', 'fulltext')
"""
the name of the logger
"""

LOGGER_HANDLER_POLICY = environ.get('LOGGER_HANDLER_POLICY', 'always')
"""
the policy of the default logging handler. The default is 'always' which means
that the default logging handler is always active. 'debug' will only activate
logging in debug mode, 'production' will only log in production and 'never'
disables it entirely.
"""

SERVER_NAME = environ.get('FULLTEXT_SERVER_NAME', None)
"""
the name and port number of the server. Required for subdomain support (e.g.
'myapp.dev:5000') Note that localhost does not support subdomains so setting
this to "localhost" does not help. Setting a ``SERVER_NAME`` also by default
enables URL generation without a request context but with an application
context.
"""

APPLICATION_ROOT = environ.get('APPLICATION_ROOT', '/')
"""
If the application does not occupy a whole domain or subdomain this can be set
to the path where the application is configured to live. This is for session
cookie as path value. If domains are used, this should be None.
"""

MAX_CONTENT_LENGTH = environ.get('MAX_CONTENT_LENGTH', None)
"""
If set to a value in bytes, Flask will reject incoming requests with a content
length greater than this by returning a 413 status code.
"""

SEND_FILE_MAX_AGE_DEFAULT = \
    int(environ.get('SEND_FILE_MAX_AGE_DEFAULT', 43200))
"""
Default cache control max age to use with send_static_file() (the default
static file handler) and send_file(), as datetime.timedelta or as seconds.
Override this value on a per-file basis using the get_send_file_max_age() hook
on Flask or Blueprint, respectively. Defaults to 43200 (12 hours).
"""

TRAP_HTTP_EXCEPTIONS = bool(int(environ.get('TRAP_HTTP_EXCEPTIONS', '0')))
"""
If this is set to True Flask will not execute the error handlers of HTTP
exceptions but instead treat the exception like any other and bubble it through
the exception stack. This is helpful for hairy debugging situations where you
have to find out where an HTTP exception is coming from.
"""

TRAP_BAD_REQUEST_ERRORS = \
    bool(int(environ.get('TRAP_BAD_REQUEST_ERRORS', '0')))
"""
Werkzeug's internal data structures that deal with request specific data will
raise special key errors that are also bad request exceptions. Likewise many
operations can implicitly fail with a BadRequest exception for consistency.
Since it’s nice for debugging to know why exactly it failed this flag can be
used to debug those situations. If this config is set to True you will get a
regular traceback instead.
"""

PREFERRED_URL_SCHEME = environ.get('PREFERRED_URL_SCHEME', 'http')
"""
The URL scheme that should be used for URL generation if no URL scheme is
available. This defaults to http.
"""

JSON_AS_ASCII = bool(int(environ.get('JSON_AS_ASCII', '0')))
"""
By default Flask serialize object to ascii-encoded JSON. If this is set to
False Flask will not encode to ASCII and output strings as-is and return
unicode strings. jsonify will automatically encode it in utf-8 then for
transport for instance.
"""

JSON_SORT_KEYS = bool(int(environ.get('JSON_SORT_KEYS', '1')))
"""
By default Flask will serialize JSON objects in a way that the keys are
ordered. This is done in order to ensure that independent of the hash seed of
the dictionary the return value will be consistent to not trash external HTTP
caches. You can override the default behavior by changing this variable. This
is not recommended but might give you a performance improvement on the cost of
cacheability.
"""

JSONIFY_PRETTYPRINT_REGULAR = \
    bool(int(environ.get('JSONIFY_PRETTYPRINT_REGULAR', '1')))
"""
If this is set to True (the default) jsonify responses will be pretty printed
if they are not requested by an XMLHttpRequest object (controlled by the
X-Requested-With header).
"""

JSONIFY_MIMETYPE = environ.get('JSONIFY_MIMETYPE', 'application/json')
"""
MIME type used for jsonify responses.
"""

TEMPLATES_AUTO_RELOAD = bool(int(environ.get('TEMPLATES_AUTO_RELOAD', '0')))
"""
Whether to check for modifications of the template source and reload it
automatically. By default the value is None which means that Flask checks
original file only in debug mode.
"""

EXPLAIN_TEMPLATE_LOADING = \
    bool(int(environ.get('EXPLAIN_TEMPLATE_LOADING', '0')))
"""
If this is enabled then every attempt to load a template will write an info
message to the logger explaining the attempts to locate the template. This can
be useful to figure out why templates cannot be found or wrong templates appear
to be loaded.
"""


# --- AWS CONFIGURATION ---

AWS_ACCESS_KEY_ID = environ.get('AWS_ACCESS_KEY_ID', 'nope')
"""
Access key for requests to AWS services.

If :const:`VAULT_ENABLED` is ``True``, this will be overwritten.
"""

AWS_SECRET_ACCESS_KEY = environ.get('AWS_SECRET_ACCESS_KEY', 'nope')
"""
Secret auth key for requests to AWS services.

If :const:`VAULT_ENABLED` is ``True``, this will be overwritten.
"""

AWS_REGION = environ.get('AWS_REGION', 'us-east-1')
"""Default region for calling AWS services."""


# --- EXTRACTOR CONFIGURATION ---

EXTRACTOR_IMAGE = environ.get('EXTRACTOR_IMAGE', 'arxiv/fulltext-extractor')
"""Name of the image used to extract plain text from PDFs."""

EXTRACTOR_VERSION = '0.3'
"""
The extractor version, used to sign extracted fulltext.

This should only be incremented when the extraction process itself changes,
not when the API of this web application changes.
"""

DOCKER_HOST = environ.get('DOCKER_HOST', 'tcp://localhost:2375')
"""
Docker host that will run the :const:`.EXTRACTOR_IMAGE`.

This can be a TCP address to a Docker API, e.g. ``tcp://localhost:2375``, or
use the local socket, e.g. ``unix:///var/run/docker.sock``.
"""

WORKDIR = environ.get('WORKDIR', '/pdfs')
"""Volume in the worker container where PDFs are stored."""

MOUNTDIR = environ.get('MOUNTDIR', '/pdfs')
"""Volume in the docker host to be mounted at /pdfs in extractor."""

STORAGE_VOLUME = environ.get('STORAGE_VOLUME', tempfile.mkdtemp())
"""Volume in API/worker containers where extraction results are stored."""


# --- KINESIS CONFIGURATION ---
KINESIS_ENDPOINT = environ.get('KINESIS_ENDPOINT')
"""Can be used to set an alternate endpoint, e.g. for testing."""

KINESIS_VERIFY = bool(int(environ.get('KINESIS_VERIFY', '1')))
"""Indicates whether SSL certificate verification should be enforced."""

KINESIS_STREAM = environ.get('KINESIS_STREAM', 'PDFIsAvailable')
"""Name of the stream to which the indexing agent subscribes."""

KINESIS_SHARD_ID = environ.get('KINESIS_SHARD_ID', '0')
"""Shard of :const:`.KINESIS_STREAM` that the agent should consume."""

KINESIS_CHECKPOINT_VOLUME = environ.get('KINESIS_CHECKPOINT_VOLUME', '/tmp')
"""Location on disk to store the consumer checkpoint."""

KINESIS_START_TYPE = environ.get('KINESIS_START_TYPE', 'AT_TIMESTAMP')
"""Start type to use if a checkpoint is not available at start-up."""

KINESIS_START_AT = environ.get('KINESIS_START_AT')
"""Timestamp (ISO-8601) at which to start if using ``AT_TIMESTAMP``."""

KINESIS_SLEEP = environ.get('KINESIS_SLEEP', '0.1')
"""
Amount of time to wait before moving on to the next record.

This can be used to address situations where we are exceeding throughput. But
also note https://arxiv-org.atlassian.net/browse/ARXIVNG-2041.
"""

if not KINESIS_VERIFY:
    warnings.warn('Certificate verification for Kinesis is disabled; this'
                  ' should not be disabled in production.')


# --- CELERY CONFIGURATION

REDIS_ENDPOINT = environ.get('REDIS_ENDPOINT', 'localhost:6379')
"""Hostname and port of the Redis used for task queueing and result storage."""

BROKER_URL = "redis://%s/0" % REDIS_ENDPOINT
RESULT_BACKEND = "redis://%s/0" % REDIS_ENDPOINT
QUEUE_NAME_PREFIX = 'fulltext-'

PREFETCH_MULTIPLIER = 1
"""
Prevent the worker from taking more than one task at a time.

In general we want to treat our workers as ephemeral. Even though Celery itself
is pretty solid runtime, we may lose the underlying machine with little or no
warning. The less state held by the workers the better.
"""

TASK_ACKS_LATE = True
"""
Do not acknowledge a task until it has been completed.

As described for :const:`.worker_prefetch_multiplier`, we assume that workers
will disappear without warning. This ensures that a task will can be executed
again if the worker crashes during execution.
"""

TASK_DEFAULT_QUEUE = 'fulltext-worker'
"""
Name of the queue for plain text extraction tasks.

Using different queue names allows us to run many different queues on the same
underlying transport (e.g. Redis cluster).
"""

# --- URL GENERATION ---

EXTERNAL_URL_SCHEME = environ.get('EXTERNAL_URL_SCHEME', 'https')
"""Scheme to use for external URLs."""

if EXTERNAL_URL_SCHEME != 'https':
    warnings.warn('External URLs will not use HTTPS proto')

BASE_SERVER = environ.get('BASE_SERVER', 'arxiv.org')
"""Base arXiv server."""

SERVER_NAME = environ.get('SERVER_NAME', None)
"""The name of this server."""

URLS: List[Tuple[str, str, str]] = []
"""
URLs for external services, for use with :func:`flask.url_for`.

For details, see :mod:`arxiv.base.urls`.
"""

VAULT_ENABLED = bool(int(environ.get('VAULT_ENABLED', '0')))
"""Enable/disable secret retrieval from Vault."""

if not VAULT_ENABLED:
    warnings.warn('Vault integration is disabled')

KUBE_TOKEN = environ.get('KUBE_TOKEN', 'fookubetoken')
"""Service account token for authenticating with Vault. May be a file path."""

VAULT_HOST = environ.get('VAULT_HOST', 'foovaulthost')
"""Vault hostname/address."""

VAULT_PORT = environ.get('VAULT_PORT', '1234')
"""Vault API port."""

VAULT_CERT = environ.get('VAULT_CERT')
"""Path to CA certificate for TLS verification when talking to Vault."""

VAULT_SCHEME = environ.get('VAULT_SCHEME', 'https')
"""Default is ``https``."""

VAULT_ROLE = environ.get('VAULT_ROLE', 'plaintext')
"""Vault role linked to this application's service account."""

if VAULT_ENABLED and VAULT_SCHEME != 'https':
    warnings.warn('Vault is not configured to use TLS; this is not safe for'
                  ' production!')

NS_AFFIX = '' if NAMESPACE == 'production' else f'-{NAMESPACE}'
VAULT_REQUESTS = [
    {'type': 'generic',
     'name': 'JWT_SECRET',
     'mount_point': f'secret{NS_AFFIX}/',
     'path': 'jwt',
     'key': 'jwt-secret',
     'minimum_ttl': 3600},
    {'type': 'aws',
     'name': 'AWS_S3_CREDENTIAL',
     'mount_point': f'aws{NS_AFFIX}/',
     'role': environ.get('VAULT_CREDENTIAL')}
]
"""Requests for Vault secrets."""

JWT_SECRET = environ.get('JWT_SECRET')

# --- UPSTREAM INTEGRATIONS ---

# Integration with the preview service.
PREVIEW_HOST = environ.get('PREVIEW_SERVICE_HOST', 'arxiv.org')
"""Hostname or addreess of the preview service."""

PREVIEW_PORT = environ.get('PREVIEW_SERVICE_PORT', '443')
"""Port for the preview service."""

PREVIEW_PROTO = environ.get(f'PREVIEW_PORT_{PREVIEW_PORT}_PROTO', 'https')
"""Protocol for the preview service."""

PREVIEW_PATH = environ.get('PREVIEW_PATH', '')
"""Path at which the preview service is deployed."""

PREVIEW_ENDPOINT = environ.get(
    'PREVIEW_ENDPOINT',
    '%s://%s:%s/%s' % (PREVIEW_PROTO, PREVIEW_HOST, PREVIEW_PORT,
                       PREVIEW_PATH)
)
"""
Full URL to the root preview service API endpoint.

If not explicitly provided, this is composed from :const:`PREVIEW_HOST`,
:const:`PREVIEW_PORT`, :const:`PREVIEW_PROTO`, and :const:`PREVIEW_PATH`.
"""

PREVIEW_VERIFY = bool(int(environ.get('PREVIEW_VERIFY', '1')))
"""Enable/disable SSL certificate verification for preview service."""

if PREVIEW_PROTO == 'https' and not PREVIEW_VERIFY:
    warnings.warn('Certificate verification for preview is disabled; this'
                  ' should not be disabled in production.')


# Integration with the canonical service.
#
# This is currently just the classic public arXiv site, specifically the PDF
# endpoint. With ARXIVNG-1495 this will be replaced with a new, dedicated API.
CANONICAL_HOST = environ.get('CANONICAL_SERVICE_HOST', 'arxiv.org')
"""Hostname or addreess of the canonical service."""

CANONICAL_PORT = environ.get('CANONICAL_SERVICE_PORT', '443')
"""Port for the canonical service."""

CANONICAL_PROTO = environ.get(f'CANONICAL_PROTO{CANONICAL_PORT}_PROTO',
                              'https')
"""Protocol for the canonical service."""

CANONICAL_PATH = environ.get('CANONICAL_PATH', '')
"""Path at which the canonical service is deployed."""

CANONICAL_ENDPOINT = environ.get(
    'CANONICAL_ENDPOINT',
    '%s://%s:%s/%s' % (CANONICAL_PROTO, CANONICAL_HOST, CANONICAL_PORT,
                       CANONICAL_PATH)
)
"""
Full URL to the root canonical service API endpoint.

If not explicitly provided, this is composed from :const:`CANONICAL_HOST`,
:const:`CANONICAL_PORT`, :const:`CANONICAL_PROTO`, and :const:`CANONICAL_PATH`.
"""

CANONICAL_VERIFY = bool(int(environ.get('CANONICAL_VERIFY', '1')))
"""Enable/disable SSL certificate verification for canonical service."""

if PREVIEW_PROTO == 'https' and not CANONICAL_VERIFY:
    warnings.warn('Certificate verification for canonical service is disabled;'
                  ' this should not be disabled in production.')
