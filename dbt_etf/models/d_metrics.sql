-- models/d_metrics.sql
-- 最终的指标表，将所有指标计算结果合并

{{ config(
    materialized='table',
    alias='etf_daily_metrics',
    unique_key=['ts_code', 'trade_date']  
        ) }}

SELECT
    t1.ts_code,
    t1.trade_date,
    
    -- 来自 etf_metrics_price_changes.sql
    t2.diff,
    t2.pct,
    t2.vol_ma5,
    
    -- 来自 etf_metrics_ma.sql (假设你的ma脚本的别名是 t1)
    t1.ma5,
    t1.ma10,
    t1.ma20,
    
    -- 来自 etf_metrics_cycle_high_low.sql
    t3.high_20,
    t3.low_20
    
FROM
    {{ ref('d_moving_averages') }} t1 -- 你的移动平均脚本
LEFT JOIN
    {{ ref('d_price_changes') }} t2
    ON t1.ts_code = t2.ts_code AND t1.trade_date = t2.trade_date
LEFT JOIN
    {{ ref('d_price_high_low') }} t3
    ON t1.ts_code = t3.ts_code AND t1.trade_date = t3.trade_date

ORDER BY
    t1.ts_code, t1.trade_date