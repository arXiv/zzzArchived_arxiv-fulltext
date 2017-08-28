import os
from fulltext.factory import create_web_app


def application(environ, start_response):
    for key, value in environ.items():
        os.environ[key] = str(value)
    app = create_web_app()
    return app(environ, start_response)
