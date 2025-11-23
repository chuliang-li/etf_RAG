-- models/staging/etf_metrics_cycle_high_low.sql
{{ config(
    materialized='table',
    alias='d_price_HL_indicators',
    unique_key=['ts_code', 'trade_date']  
        ) }}

SELECT
    ts_code,
    trade_date,
    
    -- high_20: 过去 20 个交易日的最高价 (包括当日)
    MAX(high) OVER (
        PARTITION BY ts_code
        ORDER BY trade_date
        ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
    )::DECIMAL(18, 4) AS high_20,
    
    -- low_20: 过去 20 个交易日的最低价 (包括当日)
    MIN(low) OVER (
        PARTITION BY ts_code
        ORDER BY trade_date
        ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
    )::DECIMAL(18, 4) AS low_20
    
FROM
    {{ source('main', 'etf_daily') }}
ORDER BY
    ts_code, trade_date