from sqlalchemy import Column, DateTime, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from uuid import uuid4

Base = declarative_base()

class Organization(Base):
    org_id = Column(UUID(as_uuid=True),
                    primary_key=True,
                    default=uuid4,
                    )
    name = Column(String)

class Table(Base):
    table_id = Column(UUID(as_uuid=True),
                      primary_key=True,
                      default=uuid4,
                      )
    org_id = Column(UUID(as_uuid=True),
                    default=uuid4,
                    )
    name = Column(String)
    description = Column(String)
    annotation = Column(String)
    warehouse = Column(String)
    changed_time = Column(DateTime)
    version = Column(Integer)
    is_latest = Column(Boolean)

class Column(Base):
    column_id = Column(UUID(as_uuid=True),
                       primary_key=True,
                       default=uuid4,
                       )
    table_id = Column(UUID(as_uuid=True),
                      default=uuid4,
                      )

    org_id = Column(UUID(as_uuid=True),
                    default=uuid4,
                    )
    data_type = Column(String)
    name = Column(String)
    description = Column(String)
    annotation = Column(String)
    changed_time = Column(DateTime)
    version = Column(Integer)
    is_latest = Column(Boolean)
