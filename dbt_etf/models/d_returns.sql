-- models/etf_returns_1_to_20d.sql
-- 计算 ETF 周期收益率 (1日到 20日 所有周期)
{{ config(
    materialized='table',
    alias='etf_daily_returns',
    unique_key=['ts_code', 'trade_date']  
        ) }}

WITH source_data AS (
    -- 从原始日线表获取所需的字段
    SELECT
        ts_code,
        trade_date,
        "close"
    FROM
        {{ source('main', 'etf_daily') }}
),

with_lag_prices AS (
    -- 使用 LAG 函数获取 1 到 20 个交易日前的收盘价
    SELECT
        ts_code,
        trade_date,
        "close",
        
        LAG("close", 1)  OVER (PARTITION BY ts_code ORDER BY trade_date) AS p_lag_1d,
        LAG("close", 2)  OVER (PARTITION BY ts_code ORDER BY trade_date) AS p_lag_2d,
        LAG("close", 3)  OVER (PARTITION BY ts_code ORDER BY trade_date) AS p_lag_3d,
        LAG("close", 4)  OVER (PARTITION BY ts_code ORDER BY trade_date) AS p_lag_4d,
        LAG("close", 5)  OVER (PARTITION BY ts_code ORDER BY trade_date) AS p_lag_5d,
        LAG("close", 6)  OVER (PARTITION BY ts_code ORDER BY trade_date) AS p_lag_6d,
        LAG("close", 7)  OVER (PARTITION BY ts_code ORDER BY trade_date) AS p_lag_7d,
        LAG("close", 8)  OVER (PARTITION BY ts_code ORDER BY trade_date) AS p_lag_8d,
        LAG("close", 9)  OVER (PARTITION BY ts_code ORDER BY trade_date) AS p_lag_9d,
        LAG("close", 10) OVER (PARTITION BY ts_code ORDER BY trade_date) AS p_lag_10d,
        LAG("close", 11) OVER (PARTITION BY ts_code ORDER BY trade_date) AS p_lag_11d,
        LAG("close", 12) OVER (PARTITION BY ts_code ORDER BY trade_date) AS p_lag_12d,
        LAG("close", 13) OVER (PARTITION BY ts_code ORDER BY trade_date) AS p_lag_13d,
        LAG("close", 14) OVER (PARTITION BY ts_code ORDER BY trade_date) AS p_lag_14d,
        LAG("close", 15) OVER (PARTITION BY ts_code ORDER BY trade_date) AS p_lag_15d,
        LAG("close", 16) OVER (PARTITION BY ts_code ORDER BY trade_date) AS p_lag_16d,
        LAG("close", 17) OVER (PARTITION BY ts_code ORDER BY trade_date) AS p_lag_17d,
        LAG("close", 18) OVER (PARTITION BY ts_code ORDER BY trade_date) AS p_lag_18d,
        LAG("close", 19) OVER (PARTITION BY ts_code ORDER BY trade_date) AS p_lag_19d,
        LAG("close", 20) OVER (PARTITION BY ts_code ORDER BY trade_date) AS p_lag_20d
    FROM
        source_data
)

SELECT
    ts_code,
    trade_date,
    
    -- 计算所有周期收益率 (格式: (P_t - P_{t-N}) / P_{t-N})
    (("close" - p_lag_1d) / p_lag_1d)::DECIMAL(18, 6) AS ret_1d,
    (("close" - p_lag_2d) / p_lag_2d)::DECIMAL(18, 6) AS ret_2d,
    (("close" - p_lag_3d) / p_lag_3d)::DECIMAL(18, 6) AS ret_3d,
    (("close" - p_lag_4d) / p_lag_4d)::DECIMAL(18, 6) AS ret_4d,
    (("close" - p_lag_5d) / p_lag_5d)::DECIMAL(18, 6) AS ret_5d,
    (("close" - p_lag_6d) / p_lag_6d)::DECIMAL(18, 6) AS ret_6d,
    (("close" - p_lag_7d) / p_lag_7d)::DECIMAL(18, 6) AS ret_7d,
    (("close" - p_lag_8d) / p_lag_8d)::DECIMAL(18, 6) AS ret_8d,
    (("close" - p_lag_9d) / p_lag_9d)::DECIMAL(18, 6) AS ret_9d,
    (("close" - p_lag_10d) / p_lag_10d)::DECIMAL(18, 6) AS ret_10d,
    (("close" - p_lag_11d) / p_lag_11d)::DECIMAL(18, 6) AS ret_11d,
    (("close" - p_lag_12d) / p_lag_12d)::DECIMAL(18, 6) AS ret_12d,
    (("close" - p_lag_13d) / p_lag_13d)::DECIMAL(18, 6) AS ret_13d,
    (("close" - p_lag_14d) / p_lag_14d)::DECIMAL(18, 6) AS ret_14d,
    (("close" - p_lag_15d) / p_lag_15d)::DECIMAL(18, 6) AS ret_15d,
    (("close" - p_lag_16d) / p_lag_16d)::DECIMAL(18, 6) AS ret_16d,
    (("close" - p_lag_17d) / p_lag_17d)::DECIMAL(18, 6) AS ret_17d,
    (("close" - p_lag_18d) / p_lag_18d)::DECIMAL(18, 6) AS ret_18d,
    (("close" - p_lag_19d) / p_lag_19d)::DECIMAL(18, 6) AS ret_19d,
    (("close" - p_lag_20d) / p_lag_20d)::DECIMAL(18, 6) AS ret_20d

FROM
    with_lag_prices
WHERE
    -- 确保至少有 20 个交易日的数据点才能计算完整的指标集
    p_lag_20d IS NOT NULL
ORDER BY
    ts_code, trade_date