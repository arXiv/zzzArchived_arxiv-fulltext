#!/bin/bash
touch /var/log/fulltext-agent.log
chown fulltext-agent /var/log/fulltext-agent.log
chmod 0666 /var/log/fulltext-agent.log
chmod +x /etc/init.d/fulltext-agent
service fulltext-agent stop || true
service fulltext-agent start
