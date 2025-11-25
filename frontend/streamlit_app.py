# frontend/streamlit_app.py

import streamlit as st
import pandas as pd
from pathlib import Path
from db.duckdb_client import DuckDBClient
from llm_client.llm_qwen import QwenOllamaClient


st.set_page_config(page_title="ETF 数据探索系统", layout="wide")
st.title("📈 ETF 数据探索系统 (LLM + DuckDB)")


# 初始化组件
db_client = DuckDBClient()
db_conn = db_client.con

# 默认加载 SQL prompt
project_root = Path(__file__).resolve().parents[1]
sql_prompt_path = project_root / "prompts/sql_prompt.txt"
result_prompt_path = project_root / "prompts/result_prompt.txt"

llm = QwenOllamaClient(
    model_name="qwen3:4b",
    prompt_file=sql_prompt_path,
)


# ---------------------------------------------------
# 输入问题
# ---------------------------------------------------
question = st.text_input("请输入您的问题（例如：今天涨得最多的ETF？）：")

if question:

    # ---------------------------------------------------
    # 1. 用 SQL prompt 生成 SQL
    # ---------------------------------------------------
    with st.spinner("🤖 LLM 正在生成 SQL..."):
        sql_query = llm.run_prompt(user_question=question)

    st.subheader("🧠 LLM 生成的 SQL")
    st.code(sql_query, language="sql")

    # ---------------------------------------------------
    # 2. 执行 SQL
    # ---------------------------------------------------
    try:
        df = db_conn.execute(sql_query).fetchdf()
    except Exception as e:
        st.error(f"SQL 执行错误: {e}")
        df = None

    if df is not None and not df.empty:

        # ---------------------------------------------------
        # 新增部分：展示 SQL 执行结果
        # ---------------------------------------------------
        st.subheader("📊 SQL 执行结果")
        st.dataframe(df) # 使用 st.dataframe() 来展示结果表格
        # ---------------------------------------------------
        # ---------------------------------------------------
        # 3. 切换到 result_prompt.txt 再次让 LLM 分析结果
        # ---------------------------------------------------
        llm.load_prompt(result_prompt_path)

        df_json = df.to_json(orient="records", force_ascii=False)

        with st.spinner("🧠 LLM 正在分析查询结果..."):
            analysis = llm.run_prompt(
                user_question=question,
                sql_query=sql_query,
                query_result_json=df_json
            )

        # ---------------------------------------------------
        # 4. 展示自然语言总结（没有表格）
        # ---------------------------------------------------
        st.subheader("📌 分析结果（自然语言）")
        st.write(analysis)
    else:
        st.subheader("📌 抱歉，没有数据符合你的问题")
