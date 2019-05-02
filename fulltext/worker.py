"""Initialize the Celery application."""
from typing import Any
from celery.signals import task_prerun, celeryd_init, worker_init

import docker
from docker.errors import ImageNotFound, APIError

from arxiv.vault.manager import ConfigManager
from fulltext.factory import create_worker_app, celery_app

app = create_worker_app()
app.app_context().push()

__secrets__ = None


@celeryd_init.connect   # Runs in the worker right when the daemon starts.
def get_secrets(*args: Any, **kwargs: Any) -> None:
    """Collect any required secrets from Vault."""
    if not app.config['VAULT_ENABLED']:
        print('Vault not enabled; skipping')
        return

    for key, value in get_secrets_manager().yield_secrets():
        app.config[key] = value


@celeryd_init.connect
def pull_image(*args: Any, **kwargs: Any) -> None:
    """Make the dind host pull the fulltext extractor image."""
    client = docker.DockerClient(app.config['DOCKER_HOST'])
    image_name = app.config['EXTRACTOR_IMAGE']
    image_tag = app.config['EXTRACTOR_VERSION']
    client.images.pull(f'{image_name}:{image_tag}')


@task_prerun.connect    # Runs in the worker before start a task.
def verify_secrets_up_to_date(*args: Any, **kwargs: Any) -> None:
    """Verify that any required secrets from Vault are up to date."""
    if not app.config['VAULT_ENABLED']:
        print('Vault not enabled; skipping')
        return

    for key, value in get_secrets_manager().yield_secrets():
        app.config[key] = value


def get_secrets_manager() -> ConfigManager:
    """Get or create a :class:`.ConfigManager` for this app."""
    global __secrets__
    if __secrets__ is None:
        __secrets__ = ConfigManager(app.config)
    return __secrets__
