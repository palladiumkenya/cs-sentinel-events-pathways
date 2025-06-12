from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from settings import Settings
import os

settings = Settings()
DATABASE_URL = f"mssql+pymssql://{settings.db_username}:{settings.db_password}@{settings.db_host}/{settings.db_name}"
print(f"Connecting to database at {settings.db_host} with user {settings.db_username}")
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
