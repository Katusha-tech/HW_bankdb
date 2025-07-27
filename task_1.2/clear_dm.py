import psycopg2
from config import DB_CONFIG




def main():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        print("Удаление витрин и процедур...")
        
        # Удаляем таблицы витрин
        cur.execute("DROP TABLE IF EXISTS DM.DM_ACCOUNT_TURNOVER_F CASCADE;")
        cur.execute("DROP TABLE IF EXISTS DM.DM_ACCOUNT_BALANCE_F CASCADE;")
        
        # Удаляем процедуры
        cur.execute("DROP FUNCTION IF EXISTS ds.fill_account_turnover_f(DATE) CASCADE;")
        cur.execute("DROP FUNCTION IF EXISTS ds.fill_account_balance_f(DATE) CASCADE;")
        
        # Удаляем схему если пустая (опционально)
        # cur.execute("DROP SCHEMA IF EXISTS DM CASCADE;")
        
        conn.commit()
        print("Витрины и процедуры удалены!")
        print("Теперь можно заново создавать!")
        
    except Exception as e:
        print(f"Ошибка: {e}")
        if 'conn' in locals():
            conn.rollback()
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    main()
