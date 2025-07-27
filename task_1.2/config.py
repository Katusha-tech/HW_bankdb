DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "bankdb",
    "user": "postgres",
    "password": "1111"
}

TABLE_KEYS = {
    'DS.FT_BALANCE_F': ['on_date', 'account_rk'],
    'DS.MD_ACCOUNT_D': ['data_actual_date', 'account_rk'],
    'DS.MD_CURRENCY_D': ['currency_rk', 'data_actual_date'],
    'DS.MD_EXCHANGE_RATE_D': ['data_actual_date', 'currency_rk'],
    'DS.MD_LEDGER_ACCOUNT_S': ['ledger_account', 'start_date']
}
