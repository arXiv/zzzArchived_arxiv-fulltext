"""Metric reporting for extractions."""

import boto3
import os
from flask import _app_ctx_stack as stack


class MetricsSession(object):
    """Reports processing metrics to CloudWatch."""

    namespace = 'arXiv/FullText'

    def __init__(self, endpoint_url: str, aws_access_key: str,
                 aws_secret_key: str, region_name: str) -> None:
        self.cloudwatch = boto3.client('cloudwatch', region_name=region_name,
                                       endpoint_url=endpoint_url,
                                       aws_access_key_id=aws_access_key,
                                       aws_secret_access_key=aws_secret_key)

    def report(self, metric, value, units=None, dimensions=None):
        """Put data for a metric."""

        metric_data = {
            'MetricName': metric,
            'Value': value,
        }
        if units is not None:
            metric_data.update({'Unit': units})
        if dimensions is not None:
            metric_data.update({
                'Dimensions': [{'Name': key, 'Value': value}
                               for key, value in dimensions.items()]
            })
        self.cloudwatch.put_metric_data(Namespace=self.namespace,
                                        MetricData=[metric_data])



class Metrics(object):
    """Provides metric reporting service."""

    def __init__(self, app=None):
        self.app = app
        if app is not None:
            self.init_app(app)

    def init_app(self, app) -> None:
        app.config.setdefault('FULLTEXT_CLOUDWATCH_ENDPOINT', None)
        app.config.setdefault('AWS_ACCESS_KEY_ID', 'asdf1234')
        app.config.setdefault('AWS_SECRET_ACCESS_KEY', 'fdsa5678')
        app.config.setdefault('FULLTEXT_AWS_REGION', 'us-east-1')

    def get_session(self) -> None:
        try:
            endpoint_url = self.app.config['FULLTEXT_CLOUDWATCH_ENDPOINT']
            aws_access_key = self.app.config['AWS_ACCESS_KEY_ID']
            aws_secret_key = self.app.config['AWS_SECRET_ACCESS_KEY']
            region_name = self.app.config['FULLTEXT_AWS_REGION']
        except (RuntimeError, AttributeError) as e:    # No app context.
            endpoint_url = os.environ.get('FULLTEXT_CLOUDWATCH_ENDPOINT', None)
            aws_access_key = os.environ.get('AWS_ACCESS_KEY_ID', 'asdf')
            aws_secret_key = os.environ.get('AWS_SECRET_ACCESS_KEY', 'fdsa')
            region_name = os.environ.get('FULLTEXT_AWS_REGION', 'us-east-1')
        return MetricsSession(endpoint_url, aws_access_key, aws_secret_key,
                              region_name)

    @property
    def session(self):
        ctx = stack.top
        if ctx is not None:
            if not hasattr(ctx, 'metrics'):
                ctx.metrics = self.get_session()
            return ctx.metrics
        return self.get_session()     # No application context.


metrics = Metrics()
