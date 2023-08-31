from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker
from Deta_Drive_Database_api import DetaDriveDatabase
from sqlalchemy.ext.declarative import declarative_base


deta_project_key = "c0wjeiymhax_rtbcWkEKaU7bqfks2AmUGtbdX4Zn7f2Y"
drive_name = "sql_app_drive"
db_name = "sql_app.db"
SQLALCHEMY_DATABASE_URL = 'sqlite:///:memory:'

db_deta = DetaDriveDatabase(deta_project_key, drive_name, db_name)

engine = create_engine(
    SQLALCHEMY_DATABASE_URL, connect_args={'check_same_thread': False}
)

@event.listens_for(engine, 'connect')
def connect(dbapi_connection, connection_record):
    db_deta.load_db_to_memory(dbapi_connection)

@event.listens_for(engine, 'commit')
def commit(dbapi_connection):
    db_deta.save_memory_to_drive(dbapi_connection.connection)

SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)
Base = declarative_base()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

