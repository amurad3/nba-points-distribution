from sqlalchemy import create_engine
from .config import DATABASE_URL

def get_engine():
    return create_engine(DATABASE_URL, pool_pre_ping=True)
