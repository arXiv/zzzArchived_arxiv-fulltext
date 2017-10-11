"""Initialize the Celery application."""

from celery import Celery
from flask import Flask
import os

from fulltext.factory import create_worker_app

app = create_worker_app()
