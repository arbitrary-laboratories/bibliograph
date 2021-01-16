from exabyte import settings

from sqlalchemy import create_engine
from exabyte.models.main import Base

def init_db():
    engine = create_engine(settings.SQLALCHEMY_DATABASE_URI)
    Base.metadata.create_all(bind=engine)
