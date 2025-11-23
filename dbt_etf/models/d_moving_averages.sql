-- models/d_moving_averages.sql 计算移动平均线

{{ config(
    materialized='table',
    alias='d_ma_indicators',
    unique_key=['ts_code', 'trade_date']  
        ) }}

SELECT
    ts_code,
    trade_date,
    -- 使用AVG()窗口函数计算5日移动平均
    AVG(close) OVER (
        PARTITION BY ts_code
        ORDER BY trade_date
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    ) AS ma5,
    -- 10日移动平均
    AVG(close) OVER (
        PARTITION BY ts_code
        ORDER BY trade_date
        ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
    ) AS ma10,
    -- 20日移动平均
    AVG(close) OVER (
        PARTITION BY ts_code
        ORDER BY trade_date
        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    ) AS ma20,
    -- 40日移动平均
    AVG(close) OVER (
        PARTITION BY ts_code
        ORDER BY trade_date
        ROWS BETWEEN 39 PRECEDING AND CURRENT ROW
    ) AS ma40
FROM
    {{ source('main', 'etf_daily') }} -- 从你的源表 etf_daily 获取数据
ORDER BY
    ts_code, trade_date