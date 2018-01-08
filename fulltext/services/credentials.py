"""Provides access to EC2 instance role credentials."""

import os
from datetime import datetime, timedelta
from typing import Tuple
import requests
import werkzeug

from fulltext import logging
from fulltext.context import get_application_config, get_application_global

logger = logging.getLogger(__name__)


class CredentialsSession(object):
    """Base class for credentials."""

    fmt = "%Y-%m-%dT%H:%M:%SZ"

    def __init__(self, endpoint, role, config):
        """Set the instance metadata URL."""
        self.url = '%s/%s' % (endpoint, role)
        logger.debug('New CredentialsSession %s with endpoint %s',
                     str(id(self)), self.url)
        self.config = config
        self.get_credentials()

    def _datetime(self, datestring: str) -> datetime:
        """
        Convenience method for parsing a datestring to a datetime.

        Parameters
        ----------
        datestring : str

        Returns
        -------
        :class:`.datetime`
        """
        return datetime.strptime(datestring, self.fmt)

    @property
    def access_key(self) -> str:
        """The current access key id."""
        return self.config.get('AWS_ACCESS_KEY_ID')

    @property
    def secret_key(self) -> str:
        """The current secret access key id."""
        return self.config.get('AWS_SECRET_ACCESS_KEY')

    @property
    def session_token(self) -> str:
        """The current session token."""
        return self.config.get('AWS_SESSION_TOKEN')

    def _get_expires(self) -> datetime:
        """The datetime at which the current credentials expire."""
        _expires = self.config.get('CREDENTIALS_EXPIRE')
        if isinstance(_expires, str):
            return self._datetime(_expires)
        return datetime.now()

    def _set_expires(self, expiry: datetime) -> None:
        """Set the current expiry."""
        if not isinstance(expiry, datetime):
            raise ValueError("Expiry must be a datetime object")
        exp = expiry.strftime(self.fmt)
        self.config['CREDENTIALS_EXPIRE'] = exp

    expires = property(_get_expires, _set_expires)

    @property
    def expired(self):
        """Indicate whether the current credentials are expired."""
        # This is padded by 15 seconds, just to be safe.
        return self.expires - datetime.now() <= timedelta(seconds=15)


class InstanceCredentialsSession(CredentialsSession):
    """Responsible for maintaining current access credentials for this role."""

    def _refresh_credentials(self) -> None:
        """Retrieve fresh credentials for the service role."""
        try:
            response = requests.get(self.url)
        except requests.exceptions.ConnectionError as e:
            logger.error(str(e))
            raise IOError('Could not retrieve credentials') from e

        if not response.ok:
            logger.error(str(response.content))
            raise IOError('Could not retrieve credentials')
        data = response.json()
        self.config['AWS_ACCESS_KEY_ID'] = data['AccessKeyId']
        self.config['AWS_SECRET_ACCESS_KEY'] = data['SecretAccessKey']
        self.config['AWS_SESSION_TOKEN'] = data['Token']
        self.config['CREDENTIALS_EXPIRE'] = data['Expiration']

    def get_credentials(self):
        """Retrieve the current credentials for this role."""
        logger.debug('InstanceCredentialsSession: get credentials...')
        if self.expired or self.access_key is None:
            logger.debug('expired, refreshing')
            self._refresh_credentials()
        logger.debug('new expiry: %s', self.expires.strftime(self.fmt))
        return self.access_key, self.secret_key, self.session_token


class PassthroughCredentialsSession(CredentialsSession):
    """Loads credentials directly from config."""

    def get_credentials(self):
        """Retrieve the current credentials."""
        logger.debug('PassthroughCredentialsSession: get credentials...')
        return self.access_key, self.secret_key, self.session_token


def init_app(app) -> None:
    """Configure an application instance."""
    config = get_application_config(app)
    config.setdefault('CREDENTIALS_ROLE', 'arxiv-fulltext')
    config.setdefault(
        'CREDENTIALS_URL',
        'http://169.254.169.254/latest/meta-data/iam/security-credentials'
    )


def get_session(app: object = None) -> CredentialsSession:
    """Create a new :class:`.CredentialsSession`."""
    config = get_application_config(app)
    if bool(config.get('INSTANCE_CREDENTIALS')):
        role = config.get('CREDENTIALS_ROLE', "arxiv-fulltext")
        endpoint = config.get(
            'CREDENTIALS_URL',
            'http://169.254.169.254/latest/meta-data/iam/security-credentials'
        )
        return InstanceCredentialsSession(endpoint, role, config)
    return PassthroughCredentialsSession('', '', config)


def current_session(app: werkzeug.local.LocalProxy=None) -> CredentialsSession:
    """Get/create :class:`.CredentialsSession` for this context."""
    g = get_application_global()
    if g:
        if 'credentials' not in g:
            g.credentials = get_session(app)
        g.credentials
        return g.credentials
    creds = get_session(app)
    creds.get_credentials()
    return creds


def get_credentials() -> Tuple[str, str, str]:
    """Get current credentials."""
    return current_session().get_credentials()
