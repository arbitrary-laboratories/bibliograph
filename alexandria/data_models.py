import uuid

from datetime import datetime

from flask_sqlalchemy import SQLAlchemy

from sqlalchemy import Column, DateTime, Integer, String, Boolean, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

db = SQLAlchemy()

class Org(db.Model):
    __tablename__ = "org"

    id = Column(Integer, primary_key = True)
    uuid = Column(String, unique=True)
    name = Column(String)

    tables = relationship("TableInfo", back_populates="org")

    def __init__(self, name):
        self.name = name
        self.uuid = uuid.uuid4().__str__()


class TableInfo(db.Model):
    __tablename__ = "table_info"

    id = Column(Integer, primary_key = True)
    uuid = Column(String, unique=True)

    org_id = Column(Integer, ForeignKey("org.id"))
    org = relationship("Org", back_populates="tables")

    column_infos = relationship("ColumnInfo", back_populates="table_info")

    name = Column(String)
    description = Column(String)
    # TODO (tony) - is_pii
    pii_flag = Column(Boolean)
    warehouse = Column(String)
    warehouse_full_table_id = Column(String) #unique identifier
    changed_time = Column(DateTime)
    version = Column(Integer)
    is_latest = Column(Boolean)
    pii_column_count = Column(Integer, default=0)

    def __init__(self, name, org):
        self.name = name
        self.uuid = uuid.uuid4().__str__()
        self.org = org
        self.changed_time = datetime.datetime.now()


class ColumnInfo(db.Model):
    __tablename__ = "column_info"

    id = Column(Integer, primary_key = True)
    uuid = Column(String, unique=True)

    table_info_id = Column(Integer, ForeignKey("table_info.id"))
    table_info = relationship("TableInfo", back_populates="column_infos")

    org_id = Column(String, ForeignKey("org.id"))
    org = relationship("Org")

    data_type = Column(String)
    name = Column(String)
    description = Column(String)
    pii_flag = Column(Boolean)
    warehouse_full_column_id = Column(String)
    changed_time = Column(DateTime)
    version = Column(Integer)
    is_latest = Column(Boolean)

    def __init__(self, name, table_info, data_type):
        self.name = name
        self.uuid = uuid.uuid4().__str__()
        self.table_info = table_info
        self.org = table_info.org
        self.data_type = data_type
        self.changed_time = datetime.datetime.now()

    def to_dict(self):
        return dict(
            name = self.name,
            uuid = self.uuid,
            data_type = self.data_type,
            description = self.description,
            warehouse_full_column_id = self.warehouse_full_column_id,
            pii_flag = self.pii_flag,
            changed_time = self.changed_time
        )
