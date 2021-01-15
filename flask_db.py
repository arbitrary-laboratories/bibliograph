from flask_sqlalchemy import SQLAlchemy

from alexandria.data_models import metadata

db = SQLAlchemy(metadata=metadata)
