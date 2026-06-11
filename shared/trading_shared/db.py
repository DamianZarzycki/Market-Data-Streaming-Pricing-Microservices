import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

DATABASE_URL = os.getenv("DATABASE_URL")

# Connecting to the database
engine = create_engine(DATABASE_URL)

SessionLocal = sessionmaker(
    # more control over changes that are pending to be written to the database, allowing for better transaction management and error handling.
    autocommit=False, 
    autoflush=False, 
    bind=engine)

def get_db():
    db = SessionLocal()
    try:
        # pauza
        yield db
    finally:
        db.close()