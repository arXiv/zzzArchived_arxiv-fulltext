from fulltext.factory import create_web_app, celery_app
from fulltext.services import store

app = create_web_app()
with app.app_context():
    store.current_session().create_bucket()
