#!/bin/bash

/usr/local/bin/virtualenv --python=/usr/bin/python3 /opt/fulltext/venv
source /opt/fulltext/venv/bin/activate
pip install -r /opt/fulltext/arxiv-fulltext/requirements.txt
