# TODO (@chilldude) split up models and inherit from same base

import uuid

from datetime import datetime

from flask_sqlalchemy import SQLAlchemy

from sqlalchemy import MetaData
from sqlalchemy import Column, DateTime, Integer, String, Boolean, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base


metadata = MetaData()
Base = declarative_base(metadata=metadata)

 #######################################################
 # ORM Models:
 # (-> is one directional, <-> is bidrectional)
 #
 # One -> Many
 #   - Org       <-> TableInfo
 #   - TableInfo <-> ColumnInfo
 #   - QueryInfo <-> QueryTableInfo
 #
 # One -> One
 #   - QueryTableInfo <-> TableInfo
 #
 # Many -> Many
 #   - TableInfo <-> Tag (via TableInfoTag)
 #   - ColumnInfo <-> Tag (via ColumnInfoTag)
 #
 #######################################################

class Org(Base):
    __tablename__ = "org"

    id = Column(Integer, primary_key = True, autoincrement=True)
    uuid = Column(String, unique=True)
    name = Column(String)

    tables = relationship("TableInfo", back_populates="org")

    def __init__(self, name):
        self.name = name
        self.uuid = uuid.uuid4().__str__()


class TableInfo(Base):
    __tablename__ = "table_info"

    id = Column(Integer, primary_key=True, autoincrement=True)
    uuid = Column(String, unique=True)

    org_id = Column(Integer, ForeignKey("org.id"))
    org = relationship("Org", back_populates="tables")

    column_infos = relationship("ColumnInfo", back_populates="table_info")
    query_table_info = relationship("QueryTableInfo", uselist=False, back_populates="table_info")

    tags = relationship("TableInfoTag",  back_populates="table_info")

    name = Column(String)
    description = Column(String)
    annotation = Column(String)

    pii_flag = Column(Boolean)

    warehouse = Column(String)
    warehouse_full_table_id = Column(String) #unique identifier
    changed_time = Column(DateTime)
    version = Column(Integer)
    is_latest = Column(Boolean)
    pii_column_count = Column(Integer, default=0)

    def __init__(self, org, name, warehouse_full_table_id, description=None,
                 annotation=None, pii_flag=False, warehouse=None, version=0,
                 is_latest=True, column_infos=[], query_table_info=None,
                 tags=[],
                 ):
        self.uuid = uuid.uuid4().__str__()
        self.org = org
        self.name = name
        self.description = description
        self.annotation = annotation
        self.pii_flag = pii_flag
        self.warehouse = warehouse
        self.warehouse_full_table_id = warehouse_full_table_id
        self.is_latest = True
        self.column_infos = column_infos
        self.query_table_info = None
        self.version = version
        self.changed_time = datetime.now()
        self.tags = tags


class ColumnInfo(Base):
    __tablename__ = "column_info"

    id = Column(Integer, primary_key = True, autoincrement=True)
    uuid = Column(String, unique=True)

    table_info_id = Column(Integer, ForeignKey("table_info.id"))
    table_info = relationship("TableInfo", back_populates="column_infos")

    org_id = Column(String, ForeignKey("org.id"))
    org = relationship("Org")

    tags = relationship("ColumnInfoTag",  back_populates="column_info")

    data_type = Column(String)
    name = Column(String)
    description = Column(String)
    annotation = Column(String)
    pii_flag = Column(Boolean)

    warehouse_full_column_id = Column(String)
    changed_time = Column(DateTime)
    version = Column(Integer)
    is_latest = Column(Boolean)

    def __init__(self, name, data_type, warehouse_full_column_id,
                 description=None, annotation=None, pii_flag=False, version=0,
                 is_latest=False, table_info=None,
                 tags=[]):
        self.name = name
        self.uuid = uuid.uuid4().__str__()
        self.table_info = table_info
        self.org = table_info.org
        self.data_type = data_type
        self.description = description
        self.annotation = annotation
        self.pii_flag = pii_flag
        self.warehouse_full_column_id = warehouse_full_column_id
        self.version = version
        self.is_latest = is_latest
        self.changed_time = datetime.now()
        self.tags = tags

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


class QueryInfo(Base):
    __tablename__ = "query_info"

    id = Column(Integer, primary_key=True, autoincrement=True)
    uuid = Column(String, unique=True)

    account = Column(String)

    creation_time = Column(DateTime)
    query_string = Column(String)
    query_table_infos = relationship("QueryTableInfo", back_populates="query_info")

    def __init__(self, query_string, account, creation_time):
        self.uuid = uuid.uuid4().__str__()
        self.query_string = query_string
        self.account = account
        self.creation_time = creation_time


class QueryTableInfo(Base):
    __tablename__ = "query_table_info"

    id = Column(Integer, primary_key=True, autoincrement=True)
    uuid = Column(String, unique=True)

    table_info_id = Column(Integer, ForeignKey("table_info.id"))
    table_info = relationship("TableInfo", back_populates="query_table_info")

    query_id = Column(Integer, ForeignKey("query_info.id"))
    query_info = relationship("QueryInfo", back_populates="query_table_infos")

    pii_flag = Column(Boolean)

    def __init__(self, table_info, query_info, pii_flag=False):
        self.uuid = uuid.uuid4().__str__()
        self.table_info = table_info
        self.query_info = query_info
        self.pii_flag = pii_flag


class Tag(Base):
    __tablename__ = "tag"

    id = Column(Integer, primary_key=True, autoincrement=True)
    uuid = Column(String, unique=True)
    name = Column(String)

    is_auto_classfied = Column(Boolean, default=False)

    table_infos = relationship("TableInfoTag", back_populates="tag")
    column_infos = relationship("ColumnInfoTag", back_populates="tag")

    def __init__(self, name, is_auto_classfied=False,
                 table_infos=[], column_infos=[]):
        self.uuid = uuid.uuid4().__str__()
        self.name = name
        self.is_auto_classfied = is_auto_classfied
        self.table_infos = table_infos
        self.column_infos = column_infos


class TableInfoTag(Base):
    __tablename__ = "table_info_tag"

    tag_id = Column(Integer, ForeignKey("tag.id"), primary_key=True)
    table_info_id = Column(Integer, ForeignKey("table_info.id"), primary_key=True)

    tag = relationship("Tag", back_populates="table_infos")
    table_info = relationship("TableInfo", back_populates="tags")


class ColumnInfoTag(Base):
    __tablename__ = "column_info_tag"

    tag_id = Column(Integer, ForeignKey("tag.id"), primary_key=True)
    column_info_id = Column(Integer, ForeignKey("column_info.id"), primary_key=True)

    tag = relationship("Tag", back_populates="column_infos")
    column_info = relationship("ColumnInfo", back_populates="tags")


db = SQLAlchemy(metadata=metadata)
