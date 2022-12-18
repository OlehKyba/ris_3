import logging
from contextlib import contextmanager

from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import sessionmaker, Session

from clickhouse_sqlalchemy import get_declarative_base
from clickhouse_sqlalchemy.orm.query import Query
from ris_3.settings import CLICKHOUSE_URI

log = logging.getLogger(__name__)
engine = create_engine(CLICKHOUSE_URI)
session_factory = sessionmaker(engine, query_cls=Query)
metadata = MetaData(bind=engine)

Base = get_declarative_base(metadata=metadata)


@contextmanager
def create_column_session() -> Session:
    conn = engine.connect()
    Base.metadata.create_all(conn)

    with session_factory() as session:
        try:
            yield session
        except Exception as e:
            log.exception(e)
            session.rollback()
        else:
            session.commit()

    Base.metadata.drop_all(conn)
    conn.close()
