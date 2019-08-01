"""Initialize the Celery application."""
import json
from typing import Any, Optional
from celery.signals import task_prerun, celeryd_init, worker_init

import docker
from docker.errors import ImageNotFound, APIError

from arxiv.base import logging
from arxiv.vault.manager import ConfigManager

from fulltext.factory import create_web_app
from fulltext.extract import create_worker_app

logger = logging.getLogger(__name__)
app = create_web_app(for_worker=True)
app.app_context().push()
celery_app = create_worker_app(app)


__secrets__: Optional[ConfigManager] = None
if app.config['VAULT_ENABLED']:
    __secrets__ = app.middlewares['VaultMiddleware'].secrets


@celeryd_init.connect   # Runs in the worker right when the daemon starts.
def get_secrets(*args: Any, **kwargs: Any) -> None:
    """Collect any required secrets from Vault."""
    if not app.config['VAULT_ENABLED']:
        print('Vault not enabled; skipping')
        return

    for key, value in __secrets__.yield_secrets():   # type: ignore
        app.config[key] = value


@celeryd_init.connect
def pull_image(*args: Any, **kwargs: Any) -> None:
    """Make the dind host pull the fulltext extractor image."""
    client = docker.DockerClient(app.config['DOCKER_HOST'])
    image_name = app.config['EXTRACTOR_IMAGE']
    image_tag = app.config['EXTRACTOR_VERSION']
    logger.info('Pulling %s', f'{image_name}:{image_tag}')
    for line in client.images.pull(f'{image_name}:{image_tag}', stream=True):
        print(json.dumps(line, indent=4))
        logger.debug(json.dumps(line, indent=4))
    logger.info('Finished pulling %s', f'{image_name}:{image_tag}')


@task_prerun.connect    # Runs in the worker before start a task.
def verify_secrets_up_to_date(*args: Any, **kwargs: Any) -> None:
    """Verify that any required secrets from Vault are up to date."""
    logger.debug('Veryifying that secrets are up to date')
    if not app.config['VAULT_ENABLED']:
        print('Vault not enabled; skipping')
        return

    for key, value in __secrets__.yield_secrets():   # type: ignore
        app.config[key] = value
