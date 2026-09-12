from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, sessionmaker


class Base(DeclarativeBase):
    pass


def create_engine_and_sessions(database_url: str):
    engine = create_engine(database_url, connect_args={"check_same_thread": False})
    return engine, sessionmaker(bind=engine, expire_on_commit=False)
