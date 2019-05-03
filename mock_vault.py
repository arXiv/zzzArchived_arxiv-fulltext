"""Mock endpoint for Vault secrets."""

from flask import Flask, send_file, jsonify, request
from datetime import datetime

application = Flask(__name__)

TOK_ID = 0
KV_ID = 0
AWS_ID = 0

tokens = {}


@application.route('/v1/auth/kubernetes/login', methods=['POST'])
def log_in():
    global TOK_ID
    TOK_ID += 1
    tokens[TOK_ID] = datetime.now()
    return jsonify({'auth': {'client_token': f'{TOK_ID}'}})


@application.route('/v1/secret/data/<path>')
def get_kv_secret(path):
    global KV_ID
    KV_ID += 1
    return jsonify({
        "request_id": f"foo-request-{KV_ID}",
        "lease_id": "",
        "renewable": False,
        "lease_duration": 0,
        "data": {
            "data": {
                "jwt-secret": "foosecret"
            },
            "metadata": {
                "created_time": "2019-04-18T12:58:32.820693897Z",
                "deletion_time": "",
                "destroyed": False,
                "version": 1
            }
        },
        "wrap_info": None,
        "warnings": None,
        "auth": None
    })


@application.route('/v1/aws/creds/<role>')
def get_aws_secret(role):
    """Get an AWS credential."""
    global AWS_ID
    AWS_ID += 1
    return jsonify({
        "request_id": f"a-request-id-{AWS_ID}",
        "lease_id": f"aws/creds/{role}/a-lease-id-{AWS_ID}",
        "renewable": True,
        "lease_duration": 3600,
        "data": {
            "access_key": "ASDF1234",
            "secret_key": "xljadslklk3mlkmlkmxklmx09j3990j",
            "security_token": None
        },
        "wrap_info": None,
        "warnings": None,
        "auth": None
    })


@application.route('/v1/auth/token/lookup')
def look_up_a_token(self):
    """Look up an auth token."""
    tok = request.get_json()['token']
    return jsonify({
      "data": {
        "accessor": "8609694a-cdbc-db9b-d345-e782dbb562ed",
        "creation_time": int(round(datetime.timestamp(tokens[tok]), 0)),
        "creation_ttl": 2764800,
        "display_name": "fooname",
        "entity_id": "7d2e3179-f69b-450c-7179-ac8ee8bd8ca9",
        "expire_time": "2018-05-19T11:35:54.466476215-04:00",
        "explicit_max_ttl": 0,
        "id": "cf64a70f-3a12-3f6c-791d-6cef6d390eed",
        "identity_policies": [
          "dev-group-policy"
        ],
        "issue_time": tokens[tok].isoformat(),
        "meta": {
          "username": "tesla"
        },
        "num_uses": 0,
        "orphan": True,
        "path": "auth/kubernetes/login",
        "policies": [
          "default"
        ],
        "renewable": True,
        "ttl": 2764790
      }
    })
