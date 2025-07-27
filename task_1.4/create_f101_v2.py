import psycopg2
from config import DB_CONFIG

create_schema_sql = """
CREATE SCHEMA IF NOT EXISTS DM;
"""

create_f101_v2 = """
--Витрина по форме 101

CREATE TABLE IF NOT EXISTS DM.DM_F101_ROUND_F_V2 (
    FROM_DATE DATE,
    TO_DATE DATE,
    CHAPTER CHAR(1),
    LEDGER_ACCOUNT CHAR(5),
    CHARACTERISTIC CHAR(1),
    BALANCE_IN_RUB NUMERIC(23,8),
    BALANCE_IN_VAL NUMERIC(23,8),
    BALANCE_IN_TOTAL NUMERIC(23,8),
    TURN_DEB_RUB NUMERIC(23,8),
    TURN_DEB_VAL NUMERIC(23,8),
    TURN_DEB_TOTAL NUMERIC(23,8),
    TURN_CRE_RUB NUMERIC(23,8),
    TURN_CRE_VAL NUMERIC(23,8),
    TURN_CRE_TOTAL NUMERIC(23,8),
    BALANCE_OUT_RUB NUMERIC(23,8),
    BALANCE_OUT_VAL NUMERIC(23,8),
    BALANCE_OUT_TOTAL NUMERIC(23,8)
);
""" 
def main():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        print("Creating schemas...")
        cur.execute(create_schema_sql)
        
        print("Creating tables...")
        cur.execute(create_f101_v2)
        
        conn.commit()
        print("Schemas and tables created successfully.")
        
    except Exception as e:
        print(f"Error: {e}")
        if 'conn' in locals():
            conn.rollback()
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    main()