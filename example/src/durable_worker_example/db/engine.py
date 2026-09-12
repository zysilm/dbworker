from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker


class Base(DeclarativeBase):
    pass


def create_engine_and_session_factory(database_url: str) -> tuple[Engine, sessionmaker[Session]]:
    engine = create_engine(database_url, connect_args={"check_same_thread": False})
    return engine, sessionmaker(bind=engine, expire_on_commit=False)
