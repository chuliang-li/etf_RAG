import duckdb
import pandas as pd
from datetime import date
import os

class DuckDBClient:
    def __init__(self, db_path='etfRAG.duck'):
        # 确保数据库路径是正确的，即使程序是从其他目录运行的
        script_dir = os.path.dirname(os.path.abspath(__file__))
        full_db_path = os.path.join(script_dir, db_path)
        print(f"Connecting to database at: {full_db_path}") # 调试信息
        self.con = duckdb.connect(full_db_path)

    def query(self, sql: str):
        try:
            # 打印查询语句，便于调试
            print(f"Executing SQL: {sql}")
            return self.con.execute(sql).fetchdf()
        except Exception as e:
            print(f"Query Error: {e}") # 打印错误信息
            return pd.DataFrame([{"error": str(e)}])

# --- 以下是新增的调试块 ---
if __name__ == "__main__":
    # 1. 实例化客户端
    client = DuckDBClient()

    
    # 注意：在实际调试前，您可能需要确保 etfRAG.duck 文件存在，或者至少允许程序创建它
    query_sql = """
    select max(trade_date) from etf_daily;
    """
    result_df=client.query(query_sql)
    print("\n--- Test Query Result ---")
    print(result_df)
    
    client.con.close()
    