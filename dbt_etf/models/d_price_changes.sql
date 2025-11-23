-- models/staging/d_price_change.sql
{{ config(
    materialized='table',
    alias='d_price_indicators',
    unique_key=['ts_code', 'trade_date']  
        ) }}


WITH source_data AS (
    -- 从原始日线表获取数据
    SELECT
        ts_code,
        trade_date,
        "close",
        vol
    FROM
       {{ source('main', 'etf_daily') }} 
),

with_lag AS (
    -- 计算前一天的收盘价 (prev_close)
    SELECT
        ts_code,
        trade_date,
        "close",
        vol,
        LAG("close", 1) OVER (PARTITION BY ts_code ORDER BY trade_date) AS prev_close
    FROM
        source_data
)

SELECT
    ts_code,
    trade_date,
    -- diff: 当日收盘价 - 前一日收盘价
    ("close" - prev_close)::DECIMAL(18, 4) AS diff,
    
    -- pct: (当日收盘价 - 前一日收盘价) / 前一日收盘价
    CASE
        WHEN prev_close IS NULL OR prev_close = 0 THEN NULL
        ELSE (("close" - prev_close) / prev_close)::DECIMAL(18, 6)
    END AS pct,
    
    -- vol_ma5: 5日成交量移动平均
    AVG(vol) OVER (
        PARTITION BY ts_code
        ORDER BY trade_date
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    )::DECIMAL(18, 4) AS vol_ma5

FROM
    with_lag
ORDER BY
    ts_code, trade_date