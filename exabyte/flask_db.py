from flask_sqlalchemy import SQLAlchemy

from exabyte.alexandria.data_models import metadata

db = SQLAlchemy(metadata=metadata)
