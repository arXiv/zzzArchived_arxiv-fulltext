from flask import _app_ctx_stack as stack
import requests
import os
from datetime import datetime, timedelta
from fulltext import logging

logger = logging.getLogger(__name__)

DEF_ENDPT = "http://169.254.169.254/latest/meta-data/iam/security-credentials"


class CredentialsSession(object):
    """Responsible for maintaining current access credentials for this role."""

    fmt = "%Y-%m-%dT%H:%M:%SZ"

    def __init__(self, endpoint, role):
        """Set the instance metadata URL."""
        self.url = '%s/%s' % (endpoint, role)
        self.aws_access_key_id = None
        self.aws_secret_access_key = None
        self.get_credentials()

    def _parse_datestring(self, datestring):
        return datetime.strptime(datestring, self.fmt)

    def _refresh_credentials(self):
        """Retrieve fresh credentials for the service role."""
        response = requests.get(self.url)
        if not response.ok:
            raise IOError('Could not retrieve credentials')
        data = response.json()
        self.expires = self._parse_datestring(data['Expiration'])
        self.aws_access_key_id = data['AccessKeyId']
        self.aws_secret_access_key = data['SecretAccessKey']
        self.aws_session_token = data['Token']
        os.environ['AWS_ACCESS_KEY_ID'] = self.aws_access_key_id
        os.environ['AWS_SECRET_ACCESS_KEY'] = self.aws_secret_access_key
        os.environ['AWS_SESSION_TOKEN'] = self.aws_session_token
        os.environ['CREDENTIALS_EXPIRE'] = self.expires.strftime(self.fmt)

    @property
    def expired(self):
        """Indicate whether the current credentials are expired."""
        expires = os.environ.get('CREDENTIALS_EXPIRE')
        if expires is not None:
            self.expires = self._parse_datestring(expires)
        else:
            self.expires = datetime.now()
        # This is padded by 15 seconds, just to be safe.
        return self.expires - datetime.now() <= timedelta(seconds=15)

    def get_credentials(self):
        """Retrieve the current credentials for this role."""
        logger.debug('get credentials...')
        if self.expired or self.aws_access_key_id is None:
            logger.debug('expired, refreshing')
            self._refresh_credentials()
        logger.debug('new expiry: %s' % self.expires.strftime(self.fmt))
        return self.aws_access_key_id, self.aws_secret_access_key, \
            self.aws_session_token


class Credentials(object):
    """Credentials service integration."""

    def __init__(self, app=None):
        """Set and configure application, if provided."""
        self.app = app
        if app is not None:
            self.init_app(app)

    def init_app(self, app) -> None:
        """Configure an application instance."""
        app.config.setdefault('CREDENTIALS_ROLE', 'arxiv-fulltext')
        app.config.setdefault('CREDENTIALS_URL', DEF_ENDPT)

    def get_session(self) -> None:
        """Create a new :class:`.CredentialsSession`."""
        try:
            role = self.app.config['CREDENTIALS_ROLE']
            endpoint = self.app.config['CREDENTIALS_URL']
        except (RuntimeError, AttributeError) as e:   # No application context.
            role = os.environ.get('CREDENTIALS_ROLE', "arxiv-fulltext")
            endpoint = os.environ.get('CREDENTIALS_URL', DEF_ENDPT)
        return CredentialsSession(endpoint, role)

    @property
    def session(self):
        """Get/create :class:`.CredentialsSession` for this context."""
        ctx = stack.top
        if ctx is not None:
            if not hasattr(ctx, 'credentials'):
                ctx.retrieve = self.get_session()
            return ctx.retrieve
        return self.get_session()     # No application context.


credentials = Credentials()
