import logging
from contextlib import contextmanager

from sqlalchemy import create_engine
from sqlalchemy.orm import registry, sessionmaker, Session
from sqlalchemy.orm.decl_api import DeclarativeMeta

from ris_3.settings import POSTGRES_URI


log = logging.getLogger(__name__)
mapper_registry = registry()
engine = create_engine(POSTGRES_URI)
session_factory = sessionmaker(engine)
session = session_factory()


class Base(metaclass=DeclarativeMeta):
    __abstract__ = True

    registry = mapper_registry
    metadata = mapper_registry.metadata

    __init__ = mapper_registry.constructor


@contextmanager
def create_sql_session() -> Session:
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
