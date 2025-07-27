import psycopg2  # Добавлен пробел
from config import DB_CONFIG

create_f101_procedure = """
CREATE OR REPLACE FUNCTION dm.fill_f101_round_f(i_OnDate DATE)
RETURNS VOID AS $$
DECLARE
    log_id INTEGER;
    v_from_date DATE;
    v_to_date DATE;
    v_prev_date DATE;
    v_record_count INTEGER;
BEGIN
    -- Логирование начала
    INSERT INTO LOGS.etl_logs (table_name, status, start_time)
    VALUES ('DM.DM_F101_ROUND_F', 'STARTED', NOW())
    RETURNING id INTO log_id;
    
    -- Вычисляем даты отчетного периода
    v_from_date := DATE_TRUNC('month', i_OnDate - INTERVAL '1 month');
    v_to_date := i_OnDate - INTERVAL '1 day';
    v_prev_date := v_from_date - INTERVAL '1 day';
    
    -- Очищаем данные за период
    DELETE FROM DM.DM_F101_ROUND_F 
    WHERE FROM_DATE = v_from_date AND TO_DATE = v_to_date;
    
    -- Заполняем витрину F101
    INSERT INTO DM.DM_F101_ROUND_F (
        FROM_DATE, TO_DATE, CHAPTER, LEDGER_ACCOUNT, CHARACTERISTIC,
        BALANCE_IN_RUB, BALANCE_IN_VAL, BALANCE_IN_TOTAL,
        TURN_DEB_RUB, TURN_DEB_VAL, TURN_DEB_TOTAL,
        TURN_CRE_RUB, TURN_CRE_VAL, TURN_CRE_TOTAL,
        BALANCE_OUT_RUB, BALANCE_OUT_VAL, BALANCE_OUT_TOTAL
    )
    SELECT 
        v_from_date as FROM_DATE,
        v_to_date as TO_DATE,
        las.chapter as CHAPTER,
        LEFT(acc.account_number, 5) as LEDGER_ACCOUNT,
        acc.char_type as CHARACTERISTIC,
        
        -- Входящие остатки
        COALESCE(SUM(CASE WHEN acc.currency_code IN ('810', '643') 
                        THEN bal_in.balance_out_rub ELSE 0 END), 0) as BALANCE_IN_RUB,
        COALESCE(SUM(CASE WHEN acc.currency_code NOT IN ('810', '643') 
                        THEN bal_in.balance_out_rub ELSE 0 END), 0) as BALANCE_IN_VAL,
        COALESCE(SUM(bal_in.balance_out_rub), 0) as BALANCE_IN_TOTAL,
        
        -- Дебетовые обороты
        COALESCE(SUM(CASE WHEN acc.currency_code IN ('810', '643') 
                        THEN turn.debet_amount_rub ELSE 0 END), 0) as TURN_DEB_RUB,
        COALESCE(SUM(CASE WHEN acc.currency_code NOT IN ('810', '643') 
                        THEN turn.debet_amount_rub ELSE 0 END), 0) as TURN_DEB_VAL,
        COALESCE(SUM(turn.debet_amount_rub), 0) as TURN_DEB_TOTAL,
        
        -- Кредитовые обороты
        COALESCE(SUM(CASE WHEN acc.currency_code IN ('810', '643') 
                        THEN turn.credit_amount_rub ELSE 0 END), 0) as TURN_CRE_RUB,
        COALESCE(SUM(CASE WHEN acc.currency_code NOT IN ('810', '643') 
                        THEN turn.credit_amount_rub ELSE 0 END), 0) as TURN_CRE_VAL,
        COALESCE(SUM(turn.credit_amount_rub), 0) as TURN_CRE_TOTAL,
        
        -- Исходящие остатки
        COALESCE(SUM(CASE WHEN acc.currency_code IN ('810', '643') 
                        THEN bal_out.balance_out_rub ELSE 0 END), 0) as BALANCE_OUT_RUB,
        COALESCE(SUM(CASE WHEN acc.currency_code NOT IN ('810', '643') 
                        THEN bal_out.balance_out_rub ELSE 0 END), 0) as BALANCE_OUT_VAL,
        COALESCE(SUM(bal_out.balance_out_rub), 0) as BALANCE_OUT_TOTAL
        
    FROM DS.MD_ACCOUNT_D acc
    JOIN DS.MD_LEDGER_ACCOUNT_S las ON LEFT(acc.account_number, 5)::text = las.ledger_account::text

    
    -- Входящие остатки (на день до начала периода)
    LEFT JOIN DM.DM_ACCOUNT_BALANCE_F bal_in 
        ON acc.account_rk = bal_in.account_rk 
        AND bal_in.on_date = v_prev_date
    
    -- Обороты за период
    LEFT JOIN DM.DM_ACCOUNT_TURNOVER_F turn 
        ON acc.account_rk = turn.account_rk 
        AND turn.on_date BETWEEN v_from_date AND v_to_date
    
    -- Исходящие остатки (на последний день периода)
    LEFT JOIN DM.DM_ACCOUNT_BALANCE_F bal_out 
        ON acc.account_rk = bal_out.account_rk 
        AND bal_out.on_date = v_to_date
    
    WHERE acc.data_actual_date <= v_to_date 
        AND acc.data_actual_end_date >= v_to_date
    
    GROUP BY las.chapter, LEFT(acc.account_number, 5), acc.char_type
    ORDER BY LEDGER_ACCOUNT;
    
    -- Получаем количество вставленных записей
    GET DIAGNOSTICS v_record_count = ROW_COUNT;
    
    -- Логирование окончания
    UPDATE LOGS.etl_logs
    SET status = 'SUCCESS',
        rows_loaded = v_record_count, 
        end_time = NOW(),
        message = 'Форма 101 рассчитана за период ' || v_from_date || ' - ' || v_to_date
    WHERE id = log_id;
    EXCEPTION WHEN OTHERS THEN
    -- Логируем ошибку
    UPDATE LOGS.etl_logs
        SET status = 'FAILED',
            end_time = NOW(),
            message = 'Ошибка: ' || SQLERRM
    WHERE id = log_id;
    RAISE;
    
END;
$$ LANGUAGE plpgsql;
"""

def main():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        print("Создание процедуры расчета F101...")
        cur.execute(create_f101_procedure)
        
        conn.commit()
        print("Процедура создана успешно!")
        
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
