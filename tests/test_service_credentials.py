import unittest
from unittest import mock
import os
from datetime import datetime, timedelta


class TestCredentialsService(unittest.TestCase):
    @mock.patch('requests.get')
    def setUp(self, mock_get):
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
        from fulltext.services.credentials import credentials
        self.session = credentials.session

    def test_credentials_are_not_set(self):
        """New credentials are retrieved if unset."""
        self.assertEqual(self.session.aws_access_key_id,
                         "ASIAIOSFODNN7EXAMPLE",
                         "CredentialsSession should retrieve and set the"
                         " current access key id.")
        self.assertEqual(self.session.aws_secret_access_key,
                         "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
                         "CredentialsSession should retrieve and set the"
                         " current secret access key.")
        self.assertEqual(self.session.aws_access_key_id,
                         os.environ.get('AWS_ACCESS_KEY_ID'),
                         "CredentialsSession should set AWS_ACCESS_KEY_ID"
                         " environment variable.")
        self.assertEqual(self.session.aws_secret_access_key,
                         os.environ.get('AWS_SECRET_ACCESS_KEY'),
                         "CredentialsSession should set AWS_SECRET_ACCESS_KEY"
                         " environment variable.")

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
        self.assertEqual(self.session.aws_access_key_id,
                         "ASIAIOSFODNN7NEW",
                         "CredentialsSession should retrieve and set the"
                         " current access key id.")
        self.assertEqual(self.session.aws_secret_access_key,
                         "wJalrXUtnFEMI/K7MDENG/bPxRfiCYNEWKEY",
                         "CredentialsSession should retrieve and set the"
                         " current secret access key.")
        self.assertEqual(self.session.aws_access_key_id,
                         os.environ.get('AWS_ACCESS_KEY_ID'),
                         "CredentialsSession should set AWS_ACCESS_KEY_ID"
                         " environment variable.")
        self.assertEqual(self.session.aws_secret_access_key,
                         os.environ.get('AWS_SECRET_ACCESS_KEY'),
                         "CredentialsSession should set AWS_SECRET_ACCESS_KEY"
                         " environment variable.")
