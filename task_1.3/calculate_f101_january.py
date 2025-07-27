import psycopg2
from config import DB_CONFIG
from datetime import datetime

def main():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        print("=" * 50)
        print("РАСЧЕТ ФОРМЫ 101 ЗА ЯНВАРЬ 2018")
        print("=" * 50)
        
        # Отчетная дата - первый день следующего месяца
        report_date = '2018-02-01'
        
        print(f"Расчет формы 101 за январь 2018...")
        print(f"Отчетная дата: {report_date}")
        print(f"Период: 2018-01-01 - 2018-01-31")
        print("-" * 50)
        
        # Вызываем процедуру расчета
        start_time = datetime.now()
        cur.execute("SELECT dm.fill_f101_round_f(%s);", (report_date,))
        end_time = datetime.now()
        
        conn.commit()
        
        # Получаем статистику
        cur.execute("""
            SELECT COUNT(*) as total_records,
                    COUNT(CASE WHEN BALANCE_IN_TOTAL != 0 THEN 1 END) as with_balance_in,
                    COUNT(CASE WHEN TURN_DEB_TOTAL != 0 OR TURN_CRE_TOTAL != 0 THEN 1 END) as with_turnover,
                    COUNT(CASE WHEN BALANCE_OUT_TOTAL != 0 THEN 1 END) as with_balance_out
            FROM DM.DM_F101_ROUND_F 
            WHERE FROM_DATE = '2018-01-01' AND TO_DATE = '2018-01-31';
        """)
        
        stats = cur.fetchone()
        
        # Получаем примеры данных
        cur.execute("""
            SELECT CHAPTER, LEDGER_ACCOUNT, CHARACTERISTIC,
                    BALANCE_IN_TOTAL, TURN_DEB_TOTAL, TURN_CRE_TOTAL, BALANCE_OUT_TOTAL
            FROM DM.DM_F101_ROUND_F 
            WHERE FROM_DATE = '2018-01-01' AND TO_DATE = '2018-01-31'
            ORDER BY LEDGER_ACCOUNT
            LIMIT 10;
        """)
        
        examples = cur.fetchall()
        
        print("Расчет завершен успешно!")
        print(f"Время выполнения: {end_time - start_time}")
        print("-" * 50)
        
        print("СТАТИСТИКА:")
        print(f"Всего записей: {stats[0]}")
        print(f"С входящими остатками: {stats[1]}")
        print(f"С оборотами: {stats[2]}")
        print(f"С исходящими остатками: {stats[3]}")
        print("-" * 50)
        
        print("ПРИМЕРЫ ДАННЫХ (первые 10 записей):")
        print("Глава | Счет  | Хар | Вх.остаток | Дб.оборот | Кр.оборот | Исх.остаток")
        print("-" * 80)
        
        for row in examples:
            chapter, account, char, bal_in, turn_deb, turn_cre, bal_out = row
            print(f"  {chapter}   | {account} |  {char}  | {bal_in:>10.2f} | {turn_deb:>9.2f} | {turn_cre:>9.2f} | {bal_out:>11.2f}")
        
        print("=" * 50)
        print("ФОРМА 101 ЗА ЯНВАРЬ 2018 ГОТОВА!")
        print("=" * 50)
        
    except Exception as e:
        print(f"Ошибка при расчете: {e}")
        if 'conn' in locals():
            conn.rollback()
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    main()
