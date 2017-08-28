#!/bin/bash
chown fulltext-worker /var/log/fulltext-worker.log
chmod 0666 /var/log/fulltext-worker.log
chmod +x /etc/init.d/fulltext-worker
su -c "$(aws ecr get-login --region=us-east-1)" fulltext-worker
service fulltext-worker stop || true
service fulltext-worker start
