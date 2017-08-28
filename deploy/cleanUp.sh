#!/bin/bash

rm -rf /opt/fulltext/arxiv-fulltext || true
rm /etc/init.d/fulltext-worker || true
rm /etc/init.d/fulltext-agent || true
rm /opt/fulltext/bin/start_worker || true
rm /opt/fulltext/bin/start_kcl || true
rm /opt/fulltext/bin/agent.properties || true
rm -rf /opt/fulltext/venv || true
