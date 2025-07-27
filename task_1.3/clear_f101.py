import psycopg2
from config import DB_CONFIG

def main():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        print("Удаление витрин и процедур...")
        
        # Удаляем таблицы витрин
        cur.execute("DROP TABLE IF EXISTS DM.DM_F101_ROUND_F CASCADE;")
        
        # Удаляем процедуры
        cur.execute("DROP FUNCTION IF EXISTS dm.fill_f101_round_f CASCADE;")
        
        
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
