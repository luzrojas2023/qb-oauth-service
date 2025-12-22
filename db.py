import os
import psycopg2
from psycopg2.extras import RealDictCursor

def get_conn():
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL is not set in environment variables")
    # Supabase requires SSL
    return psycopg2.connect(db_url, cursor_factory=RealDictCursor, sslmode="require")
