import psycopg2
from datetime import date, timedelta
from config import DB_CONFIG



def init_balance_2017():
    """Функция для заполнения начальных остатков на 31.12.2017 из DS.FT_BALANCE_F"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        print("Заполнение начальных остатков на 31.12.2017...")
        
        # Удаляем данные за 31.12.2017 если есть
        cur.execute("DELETE FROM DM.DM_ACCOUNT_BALANCE_F WHERE on_date = '2017-12-31'")
        
        # Заполняем начальные остатки
        init_sql = """
        INSERT INTO DM.DM_ACCOUNT_BALANCE_F (on_date, account_rk, balance_out, balance_out_rub)
        SELECT 
            '2017-12-31'::DATE as on_date,
            fb.account_rk,
            fb.balance_out,
            fb.balance_out * COALESCE(er.reduced_cource, 1) as balance_out_rub
        FROM DS.FT_BALANCE_F fb
        LEFT JOIN DS.MD_ACCOUNT_D acc ON fb.account_rk = acc.account_rk 
            AND '2017-12-31' BETWEEN acc.data_actual_date AND acc.data_actual_end_date
        LEFT JOIN DS.MD_EXCHANGE_RATE_D er ON acc.currency_rk = er.currency_rk 
            AND er.data_actual_date = '2017-12-31'
        WHERE fb.on_date = '2017-12-31';
        """
        
        cur.execute(init_sql)
        count = cur.rowcount
        conn.commit()
        
        print(f"Начальные остатки заполнены: {count} записей")
        return True
        
    except Exception as e:
        print(f"Ошибка при заполнении начальных остатков: {e}")
        if 'conn' in locals():
            conn.rollback()
        return False
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

def calculate_turnover_for_date(calc_date):
    """Функция для расчета оборотов за конкретную дату"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        # Вызываем процедуру расчета оборотов
        cur.execute("SELECT ds.fill_account_turnover_f(%s)", (calc_date,))
        conn.commit()
        
        # Проверяем результат
        cur.execute("SELECT COUNT(*) FROM DM.DM_ACCOUNT_TURNOVER_F WHERE on_date = %s", (calc_date,))
        count = cur.fetchone()[0]
        
        print(f"{calc_date}: обороты рассчитаны ({count} записей)")
        return True
        
    except Exception as e:
        print(f"Ошибка расчета оборотов за {calc_date}: {e}")
        return False
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

def calculate_balance_for_date(calc_date):
    """Функция для расчета остатков за конкретную дату"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        # Вызываем процедуру расчета остатков
        cur.execute("SELECT ds.fill_account_balance_f(%s)", (calc_date,))
        conn.commit()
        
        # Проверяем результат
        cur.execute("SELECT COUNT(*) FROM DM.DM_ACCOUNT_BALANCE_F WHERE on_date = %s", (calc_date,))
        count = cur.fetchone()[0]
        
        print(f"{calc_date}: остатки рассчитаны ({count} записей)")
        return True
        
    except Exception as e:
        print(f"Ошибка расчета остатков за {calc_date}: {e}")
        return False
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()



def main():
    print("РАСЧЕТ ВИТРИН ЗА ЯНВАРЬ 2018")
    print("="*50)
    
    # 1. Заполняем начальные остатки на 31.12.2017
    if not init_balance_2017():
        print("Не удалось заполнить начальные остатки. Завершение.")
        return
    
    # 2. Расчет за каждый день января 2018
    start_date = date(2018, 1, 1)
    end_date = date(2018, 1, 31)
    current_date = start_date
    
    success_count = 0
    total_days = (end_date - start_date).days + 1
    
    print(f"\nНачинаем расчет за {total_days} дней января 2018...")
    print("-" * 50)
    
    while current_date <= end_date:
        print(f"\nОбрабатываем {current_date}...")
        
        # Сначала обороты
        turnover_success = calculate_turnover_for_date(current_date)
        
        # Потом остатки
        balance_success = calculate_balance_for_date(current_date)
        
        if turnover_success and balance_success:
            success_count += 1
            print(f"{current_date} - успешно обработан")
        else:
            print(f"{current_date} - ошибка обработки")
        
        current_date += timedelta(days=1)
    
    print("\n" + "="*50)
    print(f"ИТОГИ РАСЧЕТА:")
    print(f"Успешно обработано: {success_count}/{total_days} дней")
    print("Расчет витрин завершен!")


if __name__ == "__main__":
    main()
