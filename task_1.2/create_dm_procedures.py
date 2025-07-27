import psycopg2
from config import DB_CONFIG



create_schema_sql = """
CREATE SCHEMA IF NOT EXISTS DM;
"""


create_dm_tables = """
-- Витрина оборотов по лицевым счетам

CREATE TABLE IF NOT EXISTS DM.DM_ACCOUNT_TURNOVER_F (
        on_date DATE,
        account_rk INTEGER,
        credit_amount NUMERIC(23,8),
        credit_amount_rub NUMERIC(23,8),
        debet_amount NUMERIC(23,8),
        debet_amount_rub NUMERIC(23,8)
);

--Витрина остатков по лицевым счетам

CREATE TABLE IF NOT EXISTS DM.DM_ACCOUNT_BALANCE_F (
        on_date DATE,
        account_rk INTEGER,
        balance_out NUMERIC(23,8),
        balance_out_rub NUMERIC(23,8)
);
"""

def main():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        print("Creating schemas...")
        cur.execute(create_schema_sql)
        
        print("Creating tables...")
        cur.execute(create_dm_tables)
        
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