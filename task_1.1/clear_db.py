"""
Очистка и пересоздание базы данных bankdb
"""
import psycopg2
from config import DB_CONFIG

def clear_database():
    try:
        # Подключаемся к postgres (системная БД)
        postgres_config = DB_CONFIG.copy()
        postgres_config['database'] = 'postgres'  # Меняем на системную БД
        
        conn = psycopg2.connect(**postgres_config)
        conn.autocommit = True
        cur = conn.cursor()
        
        print("Очистка базы данных...")
        
        # Закрываем все соединения с bankdb
        cur.execute("""
            SELECT pg_terminate_backend(pid)
            FROM pg_stat_activity
            WHERE datname = 'bankdb' AND pid <> pg_backend_pid();
        """)
        
        # Удаляем базу данных
        cur.execute('DROP DATABASE IF EXISTS bankdb;')
        print("База данных bankdb удалена")
        
        # Создаем заново
        cur.execute('CREATE DATABASE bankdb;')
        print("База данных bankdb создана заново")
        
        conn.close()
        print("База данных готова для демонстрации!")
        
    except Exception as e:
        print(f"Ошибка: {e}")

if __name__ == '__main__':
    clear_database()
