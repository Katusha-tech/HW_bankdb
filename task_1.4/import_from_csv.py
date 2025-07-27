import psycopg2
import csv
from datetime import datetime
from config import DB_CONFIG

def import_from_csv(filename):
    conn = None
    log_id = None
    rows_imported = 0
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        # Логируем начало процесса
        cur.execute("""
            INSERT INTO LOGS.etl_logs (table_name, status, start_time, message)
            VALUES (%s, %s, NOW(), %s)
            RETURNING id
        """, ('dm.dm_f101_round_f_v2', 'STARTED', f'Импорт из файла {filename}'))
        log_id = cur.fetchone()[0]
        conn.commit()
        
        print(f"Импорт данных в dm.dm_f101_round_f_v2 из {filename} - старт {datetime.now()}")

        # Очищаем таблицу перед импортом
        cur.execute("DELETE FROM dm.dm_f101_round_f_v2;")

        with open(filename, mode='r', encoding='utf-8') as f:
            reader = csv.reader(f)
            headers = next(reader)  # пропускаем заголовок
            
            insert_query = f"""
                INSERT INTO dm.dm_f101_round_f_v2 ({','.join(headers)})
                VALUES ({','.join(['%s'] * len(headers))})
            """
            
            data = list(reader)
            rows_imported = len(data)
            cur.executemany(insert_query, data)

        conn.commit()
        print(f"Импорт завершён успешно - {datetime.now()}. Импортировано строк: {rows_imported}")

        # Логируем успешное завершение
        cur.execute("""
            UPDATE LOGS.etl_logs
            SET status = %s,
                rows_loaded = %s,
                end_time = NOW(),
                message = %s
            WHERE id = %s
        """, ('SUCCESS', rows_imported, f'Импорт успешно завершён из файла {filename}', log_id))
        conn.commit()

    except Exception as e:
        if conn and log_id:
            cur.execute("""
                UPDATE LOGS.etl_logs
                SET status = %s,
                    end_time = NOW(),
                    message = %s
                WHERE id = %s
            """, ('FAILED', str(e), log_id))
            conn.commit()
        print(f"Ошибка импорта: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    import_from_csv('f101_export.csv')
