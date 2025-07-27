import psycopg2
import csv
from datetime import datetime
from config import DB_CONFIG

def export_to_csv(filename):
    conn = None
    log_id = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        # Логируем начало процесса экспорта
        cur.execute("""
            INSERT INTO LOGS.etl_logs (table_name, status, start_time, message)
            VALUES (%s, %s, NOW(), %s)
            RETURNING id
        """, ('dm.dm_f101_round_f', 'STARTED', f'Экспорт в файл {filename}'))
        log_id = cur.fetchone()[0]
        conn.commit()
        
        # Выбираем данные из таблицы
        cur.execute("SELECT * FROM dm.dm_f101_round_f;")
        rows = cur.fetchall()
        colnames = [desc[0] for desc in cur.description]
        
        # Записываем данные в CSV файл
        with open(filename, mode='w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(colnames)  # заголовок
            writer.writerows(rows)
        
        # Логируем успешное завершение
        cur.execute("""
            UPDATE LOGS.etl_logs
            SET status = %s,
                rows_loaded = %s,
                end_time = NOW(),
                message = %s
            WHERE id = %s
        """, ('SUCCESS', len(rows), f'Экспорт успешно завершён в файл {filename}', log_id))
        conn.commit()
        
        print(f"Экспорт завершён успешно. Записано строк: {len(rows)}")
        
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
        print(f"Ошибка экспорта: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    export_to_csv('f101_export.csv')

