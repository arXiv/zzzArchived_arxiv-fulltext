"""Unit tests for :mod:`fulltext.services.credentials`."""
import unittest
from unittest import mock
import os
from datetime import datetime, timedelta


class TestCredentialsService(unittest.TestCase):
    """Expected behavior of :class:`.InstanceCredentialsSession`."""

    @mock.patch('requests.get')
    def setUp(self, mock_get):
        """Set up the credentials session."""
        self.tearDown()
        mock_response = mock.MagicMock()
        mock_response.ok = True
        type(mock_response).json = mock.MagicMock(return_value={
          "Code": "Success",
          "LastUpdated": "2012-04-26T16:39:16Z",
          "Type": "AWS-HMAC",
          "AccessKeyId": "ASIAIOSFODNN7EXAMPLE",
          "SecretAccessKey": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
          "Token": "token",
          "Expiration": "2017-05-17T15:09:54Z"
        })
        mock_get.return_value = mock_response
        os.environ['INSTANCE_CREDENTIALS'] = 'true'
        from fulltext.services import credentials
        self.session = credentials.get_session()

    def tearDown(self):
        """Clear current credentials."""
        if hasattr(self, 'session'):
            if 'AWS_ACCESS_KEY_ID' in self.session.config:
                del self.session.config['AWS_ACCESS_KEY_ID']
            del self.session
        if 'AWS_ACCESS_KEY_ID' in os.environ:
            del os.environ['AWS_ACCESS_KEY_ID']
        if 'AWS_SECRET_ACCESS_KEY' in os.environ:
            del os.environ['AWS_SECRET_ACCESS_KEY']
        if 'AWS_SESSION_TOKEN' in os.environ:
            del os.environ['AWS_SESSION_TOKEN']
        if 'CREDENTIALS_EXPIRE' in os.environ:
            del os.environ['CREDENTIALS_EXPIRE']
        if 'INSTANCE_CREDENTIALS' in os.environ:
            del os.environ['INSTANCE_CREDENTIALS']

    def test_credentials_are_not_set(self):
        """Before setUp(), no credentials were set."""
        self.assertEqual(self.session.access_key,
                         "ASIAIOSFODNN7EXAMPLE",
                         "CredentialsSession should retrieve and set the"
                         " current access key id.")
        self.assertEqual(self.session.secret_key,
                         "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
                         "CredentialsSession should retrieve and set the"
                         " current secret access key.")

    @mock.patch('requests.get')
    def test_credentials_are_expired(self, mock_get):
        """New credentials are retrieved when current credentials expire."""
        new_expiry = datetime.now() + timedelta(seconds=300)
        os.environ['CREDENTIALS_EXPIRE'] = "2016-05-17T15:09:54Z"
        mock_response = mock.MagicMock()
        mock_response.ok = True
        type(mock_response).json = mock.MagicMock(return_value={
          "Code": "Success",
          "LastUpdated": "2012-04-26T16:39:16Z",
          "Type": "AWS-HMAC",
          "AccessKeyId": "ASIAIOSFODNN7NEW",
          "SecretAccessKey": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYNEWKEY",
          "Token": "token",
          "Expiration": new_expiry.strftime("%Y-%m-%dT%H:%M:%SZ")
        })
        mock_get.return_value = mock_response
        self.assertTrue(self.session.expired)
        self.session.get_credentials()

        self.assertEqual(mock_get.call_count, 1,
                         "CredentialsSession should call the instance metadata"
                         " endpoint.")
        self.assertEqual(self.session.access_key,
                         "ASIAIOSFODNN7NEW",
                         "CredentialsSession should retrieve and set the"
                         " current access key id.")
        self.assertEqual(self.session.secret_key,
                         "wJalrXUtnFEMI/K7MDENG/bPxRfiCYNEWKEY",
                         "CredentialsSession should retrieve and set the"
                         " current secret access key.")
