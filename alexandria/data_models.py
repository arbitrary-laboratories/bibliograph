from sqlalchemy import Column, DateTime, Integer, String, Boolean
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class Organization(Base):
    __tablename__ = "organizations"

    org_id = Column(String,
                    primary_key= True,
                   )
    name = Column(String)

class Table(Base):
    __tablename__ = "tables"

    table_id = Column(String,
                      primary_key=True,
                      )
    org_id = Column(String)
    name = Column(String)
    description = Column(String)
    pii_flag = Column(Boolean)
    warehouse = Column(String)
    warehouse_full_table_id = Column(String) #unique identifier
    changed_time = Column(DateTime)
    version = Column(Integer)
    is_latest = Column(Boolean)

class Column(Base):
    __tablename__ = "columns"

    column_id = Column(String,
                       primary_key=True,
                       )
    table_id = Column(String)
    org_id = Column(String)
    data_type = Column(String)
    name = Column(String)
    description = Column(String)
    pii_flag = Column(Boolean)
    warehouse_full_column_id = Column(String)
    changed_time = Column(DateTime)
    version = Column(Integer)
    is_latest = Column(Boolean)
