from fulltext.factory import create_web_app
from fulltext.services import store

app = create_web_app()
with app.app_context():
    store.Storage.current_session().create_bucket()
