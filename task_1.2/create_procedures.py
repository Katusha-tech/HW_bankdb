import psycopg2
from config import DB_CONFIG




# Процедура расчета оборотов

create_turnover_procedure = """
CREATE OR REPLACE FUNCTION ds.fill_account_turnover_f(i_OnDate DATE)
RETURNS VOID AS $$
DECLARE
    log_id INTEGER;
    record_count INTEGER := 0;
BEGIN
    -- Логирование начала
    INSERT INTO LOGS.etl_logs (table_name, status, start_time)
    VALUES ('DM.DM_ACCOUNT_TURNOVER_F', 'STARTED', NOW())
    RETURNING id INTO log_id;
    
    -- Удаляем данные за дату расчета для возможности перезапуска
    DELETE FROM DM.DM_ACCOUNT_TURNOVER_F WHERE on_date = i_OnDate;
    
    -- Расчет оборотов
    INSERT INTO DM.DM_ACCOUNT_TURNOVER_F (
        on_date, 
        account_rk, 
        credit_amount, 
        credit_amount_rub, 
        debet_amount, 
        debet_amount_rub
    )
    SELECT 
        i_OnDate as on_date,
        COALESCE(cr.account_rk, db.account_rk) as account_rk,
        COALESCE(cr.credit_amount, 0) as credit_amount,
        COALESCE(cr.credit_amount_rub, 0) as credit_amount_rub,
        COALESCE(db.debet_amount, 0) as debet_amount,
        COALESCE(db.debet_amount_rub, 0) as debet_amount_rub
    FROM 
        -- Обороты по кредиту
        (SELECT 
            p.credit_account_rk as account_rk,
            SUM(p.credit_amount) as credit_amount,
            SUM(p.credit_amount * COALESCE(er.reduced_cource, 1)) as credit_amount_rub
        FROM DS.FT_POSTING_F p
        LEFT JOIN DS.MD_ACCOUNT_D acc ON p.credit_account_rk = acc.account_rk 
            AND i_OnDate BETWEEN acc.data_actual_date AND acc.data_actual_end_date
        LEFT JOIN DS.MD_EXCHANGE_RATE_D er ON acc.currency_rk = er.currency_rk 
            AND er.data_actual_date = i_OnDate
        WHERE p.oper_date = i_OnDate
        GROUP BY p.credit_account_rk
        ) cr
    FULL OUTER JOIN 
        -- Обороты по дебету
        (SELECT 
            p.debet_account_rk as account_rk,
            SUM(p.debet_amount) as debet_amount,
            SUM(p.debet_amount * COALESCE(er.reduced_cource, 1)) as debet_amount_rub
        FROM DS.FT_POSTING_F p
        LEFT JOIN DS.MD_ACCOUNT_D acc ON p.debet_account_rk = acc.account_rk 
            AND i_OnDate BETWEEN acc.data_actual_date AND acc.data_actual_end_date
        LEFT JOIN DS.MD_EXCHANGE_RATE_D er ON acc.currency_rk = er.currency_rk 
            AND er.data_actual_date = i_OnDate
        WHERE p.oper_date = i_OnDate
        GROUP BY p.debet_account_rk
        ) db ON cr.account_rk = db.account_rk;
    
    GET DIAGNOSTICS record_count = ROW_COUNT;
    
    -- Логирование окончания
    UPDATE LOGS.etl_logs 
    SET status = 'SUCCESS', 
        rows_loaded = record_count, 
        end_time = NOW(),
        message = 'Обороты рассчитаны за ' || i_OnDate
    WHERE id = log_id;
    
    RAISE NOTICE 'Обороты за % рассчитаны: % записей', i_OnDate, record_count;
    
EXCEPTION
    WHEN OTHERS THEN
        UPDATE LOGS.etl_logs 
        SET status = 'FAILED', 
            end_time = NOW(),
            message = 'Ошибка: ' || SQLERRM
        WHERE id = log_id;
        RAISE;
END;
$$ LANGUAGE plpgsql;
"""

# Процедура расчета остатков по лицевым счетам

create_balance_procedure = """
CREATE OR REPLACE FUNCTION ds.fill_account_balance_f(i_OnDate DATE)
RETURNS VOID AS $$
DECLARE
    log_id INTEGER;
    record_count INTEGER := 0;
    prev_date DATE;
BEGIN
    -- Логирование начала
    INSERT INTO LOGS.etl_logs (table_name, status, start_time)
    VALUES ('DM.DM_ACCOUNT_BALANCE_F', 'STARTED', NOW())
    RETURNING id INTO log_id;
    
    -- Предыдущая дата
    prev_date := i_OnDate - INTERVAL '1 day';
    
    -- Удаляем данные за дату расчета
    DELETE FROM DM.DM_ACCOUNT_BALANCE_F WHERE on_date = i_OnDate;
    
    -- Расчет остатков
    INSERT INTO DM.DM_ACCOUNT_BALANCE_F (
        on_date, 
        account_rk, 
        balance_out, 
        balance_out_rub
    )
    SELECT 
        i_OnDate as on_date,
        acc.account_rk,
        CASE 
            WHEN acc.char_type = 'А' THEN 
                COALESCE(prev_bal.balance_out, 0) + 
                COALESCE(turn.debet_amount, 0) - 
                COALESCE(turn.credit_amount, 0)
            WHEN acc.char_type = 'П' THEN 
                COALESCE(prev_bal.balance_out, 0) - 
                COALESCE(turn.debet_amount, 0) + 
                COALESCE(turn.credit_amount, 0)
            ELSE COALESCE(prev_bal.balance_out, 0)
        END as balance_out,
        CASE 
            WHEN acc.char_type = 'А' THEN 
                COALESCE(prev_bal.balance_out_rub, 0) + 
                COALESCE(turn.debet_amount_rub, 0) - 
                COALESCE(turn.credit_amount_rub, 0)
            WHEN acc.char_type = 'П' THEN 
                COALESCE(prev_bal.balance_out_rub, 0) - 
                COALESCE(turn.debet_amount_rub, 0) + 
                COALESCE(turn.credit_amount_rub, 0)
            ELSE COALESCE(prev_bal.balance_out_rub, 0)
        END as balance_out_rub
    FROM DS.MD_ACCOUNT_D acc
    LEFT JOIN DM.DM_ACCOUNT_BALANCE_F prev_bal 
        ON acc.account_rk = prev_bal.account_rk 
        AND prev_bal.on_date = prev_date
    LEFT JOIN DM.DM_ACCOUNT_TURNOVER_F turn 
        ON acc.account_rk = turn.account_rk 
        AND turn.on_date = i_OnDate
    WHERE i_OnDate BETWEEN acc.data_actual_date AND acc.data_actual_end_date;
    
    GET DIAGNOSTICS record_count = ROW_COUNT;
    
    -- Логирование окончания
    UPDATE LOGS.etl_logs 
    SET status = 'SUCCESS', 
        rows_loaded = record_count, 
        end_time = NOW(),
        message = 'Остатки рассчитаны за ' || i_OnDate
    WHERE id = log_id;
    
    RAISE NOTICE 'Остатки за % рассчитаны: % записей', i_OnDate, record_count;
    
EXCEPTION
    WHEN OTHERS THEN
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
        
        print("Создание процедуры расчета оборотов...")
        cur.execute(create_turnover_procedure)
        
        print("Создание процедуры расчета остатков...")
        cur.execute(create_balance_procedure)
        
        conn.commit()
        print("Процедуры созданы успешно!")
        
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
