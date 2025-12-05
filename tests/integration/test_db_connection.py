from src.db.engine import create_db_engine

def test_db_connection():
    engine = create_db_engine()
    conn = engine.connect()
    conn.close()

    assert engine is not None
