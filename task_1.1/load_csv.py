import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
from config import DB_CONFIG

def connect_db():
    """Функция создания и возвращения соединения"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        print(f"✅ Успешное подключение к базе данных {DB_CONFIG['database']} на {DB_CONFIG['host']}:{DB_CONFIG['port']}")
        return conn
    except psycopg2.Error as e:
        print(f"❌ Ошибка при подключении к базе данных: {e}")
        return None

def log_start(conn, table_name):
    """Функция для вставки записи о старте загрузки в таблицу логов"""
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS LOGS.etl_logs (
                    id SERIAL PRIMARY KEY,
                    table_name VARCHAR(100),
                    status VARCHAR(20),
                    start_time TIMESTAMP,
                    end_time TIMESTAMP,
                    rows_loaded INTEGER,
                    message TEXT
                );
            """)
            cur.execute("""
                INSERT INTO LOGS.etl_logs (table_name, status, start_time)
                VALUES (%s, %s, NOW())
                RETURNING id;
            """, (table_name, 'STARTED'))
            log_id = cur.fetchone()[0]
            conn.commit()
            print(f"📝 Лог {log_id}: загрузка для {table_name} начата.")
            return log_id
    except Exception as e:
        print(f"⚠️ Ошибка создания лога: {e}")
        return None

def log_end(conn, log_id, record_count, status='SUCCESS'):
    """Функция для вставки записи об окончании загрузки"""
    if log_id is None:
        return
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE LOGS.etl_logs
                SET status = %s,
                    rows_loaded = %s,
                    end_time = NOW(),
                    message = %s
                WHERE id = %s;
            """, (status, record_count, f"Загружено {record_count} записей", log_id))
            conn.commit()
            print(f"📝 Лог {log_id}: загрузка завершена ({status}), {record_count} записей.")
    except Exception as e:
        print(f"⚠️ Ошибка обновления лога: {e}")

def read_csv_with_encoding(file_path, date_columns=None, date_formats=None):
    """Функция для чтения CSV с автоопределением кодировки и обработкой дат"""
    encodings = ['utf-8', 'latin1', 'cp1251']
    
    for encoding in encodings:
        try:
            df = pd.read_csv(file_path, sep=';', encoding=encoding)
            df.columns = df.columns.str.lower()
            print(f"✅ Файл {file_path} прочитан ({encoding}): {len(df)} записей")
            
            # Обработка дат с правильными форматами
            if date_columns and date_formats:
                for col in date_columns:
                    if col in df.columns and col in date_formats:
                        print(f"📅 Обрабатываем даты в {col}")
                        df[col] = pd.to_datetime(df[col], format=date_formats[col], errors='coerce')
                        df[col] = df[col].where(pd.notna(df[col]), None)
            
            # Обработка NaN значений
            df = df.where(pd.notna(df), None)
            
            return df
            
        except Exception as e:
            continue
    
    print(f"❌ Ошибка: не удалось прочитать файл {file_path}")
    return None

def clean_currency_data(df):
    """Специальная обработка для таблицы валют"""
    if 'currency_code' in df.columns:
        df['currency_code'] = df['currency_code'].astype(str).str.replace('.0', '').str[:3]
        df['currency_code'] = df['currency_code'].replace('nan', None)
    
    if 'code_iso_char' in df.columns:
        df['code_iso_char'] = df['code_iso_char'].astype(str).str.replace('\x98', '').str[:3]
        df['code_iso_char'] = df['code_iso_char'].replace('nan', None)
    
    return df

def load_data(conn, df, table_name, key_columns=None):
    """Функция для загрузки данных в целевую таблицу"""
    if df is None or df.empty:
        print(f"⚠️ Нет данных для загрузки в {table_name}")
        return 0

    # Специальная обработка для MD_CURRENCY_D
    if 'MD_CURRENCY_D' in table_name:
        df = clean_currency_data(df)
    
    # Удаление дубликатов для MD_EXCHANGE_RATE_D
    if 'MD_EXCHANGE_RATE_D' in table_name and key_columns:
        before = len(df)
        df = df.drop_duplicates(subset=key_columns)
        if before != len(df):
            print(f"🔄 Удалено дубликатов: {before - len(df)}")

    columns = list(df.columns)
    values = [tuple(row) for row in df.to_numpy()]

    with conn.cursor() as cur:
        # Очищаем таблицу
        cur.execute(f"TRUNCATE TABLE {table_name};")
        
        # Загружаем данные
        insert_query = f"""
            INSERT INTO {table_name} ({', '.join(columns)})
            VALUES %s;
        """
        execute_values(cur, insert_query, values)

    conn.commit()
    print(f"✅ Данные в {table_name} загружены: {len(df)} записей")
    return len(df)

def load_all_tables():
    """Загрузка всех таблиц"""
    conn = connect_db()
    if conn is None:
        print("❌ Не удалось подключиться к базе данных. Завершаем.")
        return

    # Создаем схему LOGS если не существует
    with conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS LOGS;")
        conn.commit()

    # Конфигурация всех таблиц
    tables_config = [
        {
            'csv_file': 'csv_files/md_account_d.csv',
            'table_name': 'DS.MD_ACCOUNT_D',
            'date_columns': ['data_actual_date', 'data_actual_end_date'],
            'date_formats': {'data_actual_date': '%Y-%m-%d', 'data_actual_end_date': '%Y-%m-%d'},
            'key_columns': None
        },
        {
            'csv_file': 'csv_files/md_currency_d.csv',
            'table_name': 'DS.MD_CURRENCY_D',
            'date_columns': ['data_actual_date', 'data_actual_end_date'],
            'date_formats': {'data_actual_date': '%Y-%m-%d', 'data_actual_end_date': '%Y-%m-%d'},
            'key_columns': None
        },
        {
            'csv_file': 'csv_files/md_exchange_rate_d.csv',
            'table_name': 'DS.MD_EXCHANGE_RATE_D',
            'date_columns': ['data_actual_date', 'data_actual_end_date'],
            'date_formats': {'data_actual_date': '%Y-%m-%d', 'data_actual_end_date': '%Y-%m-%d'},
            'key_columns': ['data_actual_date', 'currency_rk']
        },
        {
            'csv_file': 'csv_files/md_ledger_account_s.csv',
            'table_name': 'DS.MD_LEDGER_ACCOUNT_S',
            'date_columns': ['start_date', 'end_date'],
            'date_formats': {'start_date': '%Y-%m-%d', 'end_date': '%Y-%m-%d'},
            'key_columns': None
        },
        {
            'csv_file': 'csv_files/ft_posting_f.csv',
            'table_name': 'DS.FT_POSTING_F',
            'date_columns': ['oper_date'],
            'date_formats': {'oper_date': '%d-%m-%Y'},
            'key_columns': None
        },
        {
            'csv_file': 'csv_files/ft_balance_f.csv',
            'table_name': 'DS.FT_BALANCE_F',
            'date_columns': ['on_date'],
            'date_formats': {'on_date': '%d.%m.%Y'},
            'key_columns': ['on_date', 'account_rk']
        }
    ]

    print("🏦 ЗАГРУЗКА БАНКОВСКОЙ БАЗЫ ДАННЫХ")
    print("=" * 50)
    
    total_records = 0
    
    for config in tables_config:
        print(f"\n📊 === Загрузка {config['table_name']} ===")
        
        log_id = log_start(conn, config['table_name'])
        
        df = read_csv_with_encoding(
            config['csv_file'], 
            config['date_columns'], 
            config['date_formats']
        )
        
        if df is None or df.empty:
            print(f"⚠️ Нет данных для загрузки {config['table_name']}")
            log_end(conn, log_id, 0, status='FAILED')
            continue
        
        record_count = load_data(conn, df, config['table_name'], config['key_columns'])
        total_records += record_count
        
        log_end(conn, log_id, record_count, status='SUCCESS')
    
    # Итоговая статистика
    print(f"\n🎯 ИТОГОВАЯ СТАТИСТИКА")
    print("=" * 40)
    
    with conn.cursor() as cur:
        tables = [config['table_name'] for config in tables_config]
        final_total = 0
        
        for table in tables:
            cur.execute(f'SELECT COUNT(*) FROM {table};')
            count = cur.fetchone()[0]
            final_total += count
            print(f"{table}: {count:,} записей")
    
    print(f"\n🏆 ВСЕГО ЗАГРУЖЕНО: {final_total:,} записей")
    print("✅ ЗАГРУЗКА ЗАВЕРШЕНА УСПЕШНО!")
    
    conn.close()

def main():
    """Основная функция - загружает одну таблицу FT_BALANCE_F (как было изначально)"""
    conn = connect_db()
    if conn is None:
        print("Не удалось подключиться к базе данных. Завершаем.")
        return

    table_name = 'DS.FT_BALANCE_F'
    log_id = log_start(conn, table_name)

    df = read_csv_with_encoding(
        'csv_files/ft_balance_f.csv', 
        ['on_date'], 
        {'on_date': '%d.%m.%Y'}
    )
    
    if df is None or df.empty:
        print("Нет данных для загрузки. Завершаем.")
        log_end(conn, log_id, 0, status='FAILED')
        conn.close()
        return

    record_count = load_data(conn, df, table_name, ['on_date', 'account_rk'])
    log_end(conn, log_id, record_count, status='SUCCESS')
    conn.close()

if __name__ == '__main__':
    load_all_tables()
