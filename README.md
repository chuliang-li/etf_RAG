# etf 数据探索 AI 助手

## 技术和架构说明（仅在Windows下测试通过）
- etf数据文件以 csv 格式存在 `input_csv` 目录，执行 `python.exe .\utils\load_csv_2_duck.py` 加载到 `db\etfRAG.duck` 数据库里的 `etf_daily` 表
- 然后 `cd dbt_etf` 执行 `dbt run` 可以生成 `etf_daily_metrics` 技术指标表和 `etf_daily_returns` 周期收益率计算表
- 大模型使用的是本地的**Ollama**的 `qwen3:4b` 模型，请执行搭建
- 使用 `python.exe .\frontend\streamlit_app.py` 运行程序
  
## 未来考虑
- 增加可以使用云上llm的选项（比如阿里云的dashscope平台）
- 目前仅包含有限的技术指标计算，将来考虑增加更多技术指标
- 从简单的 etf 自然语言查询 变成 etf 投研助手
