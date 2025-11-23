import duckdb
import pandas as pd
import os
from pathlib import Path


root = Path(__file__).resolve().parents[1]
myDuckDB = root / "db/etfRAG.duck"
data_dir = root / "input_csv/"

def load_etf_data_to_duckdb(data_dir='data', db_path=myDuckDB):
    """
    加载指定目录下所有 ETF 的 CSV 日线数据到 DuckDB 数据库。
    已处理 gb2312 编码、文件末尾非数据行以及列顺序不匹配问题。

    Args:
        data_dir (str): 存放 CSV 文件的目录路径。
        db_path (str): DuckDB 数据库文件的保存路径。
    """
    # 1. 连接到 DuckDB 数据库
    try:
        con = duckdb.connect(db_path)
        print(f"成功连接到 DuckDB 数据库: {db_path}")
    except duckdb.Error as e:
        print(f"连接 DuckDB 失败: {e}")
        return

    # 2. 创建或连接用于存储 ETF 日线数据的表
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS etf_daily (
        ts_code VARCHAR,
        trade_date DATE,
        exchange_code VARCHAR,
        open DECIMAL(18, 4),
        high DECIMAL(18, 4),
        low DECIMAL(18, 4),
        close DECIMAL(18, 4),
        vol BIGINT,
        amount DECIMAL(20, 4)
    );
    """
    con.execute(create_table_sql)
    print("成功创建或连接到表 'etf_daily'")

    # 核心修改点：在加载数据前清空表格
    try:
        con.execute("TRUNCATE TABLE etf_daily;")
        print("成功清空 'etf_daily' 表，准备重新加载数据。")
    except Exception as e:
        print(f"清空表格失败: {e}")
        con.close()
        return

    # 3. 遍历并加载数据
    file_list = [f for f in os.listdir(data_dir) if f.endswith('.csv')]
    if not file_list:
        print(f"在目录 '{data_dir}' 中没有找到任何 CSV 文件。")
        con.close()
        return

    for filename in file_list:
        try:
            filepath = os.path.join(data_dir, filename)

            parts = filename.split('#')
            if len(parts) != 2:
                print(f"跳过文件 '{filename}'，文件名格式不正确。")
                continue

            exchange_code = parts[0].upper()
            etf_code = parts[1].split('.')[0]
            ts_code = f"{etf_code}.{exchange_code}"

            # 使用 Pandas 读取 CSV 文件，并指定列名、编码和跳过末尾行
            df = pd.read_csv(filepath,
                             header=None,
                             names=['trade_date', 'open', 'high', 'low', 'close', 'vol', 'amount'],
                             dtype={'trade_date': str, 'open': float, 'high': float, 'low': float,
                                    'close': float, 'vol': 'int64', 'amount': float},
                             encoding='gb2312',
                             skipfooter=1,
                             engine='python')

            # 数据类型转换
            df['trade_date'] = pd.to_datetime(df['trade_date'], format='%Y/%m/%d').dt.date

            cutoff_date = pd.to_datetime("2021-01-01").date()
            df = df[df['trade_date'] >= cutoff_date]

            # 添加 ts_code 和 exchange_code 列
            df['ts_code'] = ts_code
            df['exchange_code'] = exchange_code

            # 关键修改点：调整列顺序以匹配数据库表结构
            df = df[['ts_code', 'trade_date', 'exchange_code', 'open', 'high', 'low', 'close', 'vol', 'amount']]


            # 将 DataFrame 数据写入 DuckDB
            # con.append('etf_daily', df)
            con.execute("INSERT OR REPLACE INTO etf_daily SELECT * FROM df")
            print(f"成功加载文件 '{filename}' 的 {len(df)} 行数据到数据库。")

        except Exception as e:
            print(f"加载文件 '{filename}' 失败: {e}")
            continue

    # 4. 关闭数据库连接
    con.close()
    print("所有文件加载完成，数据库连接已关闭。")

# --- 主程序入口 ---
if __name__ == '__main__':
    load_etf_data_to_duckdb(data_dir=data_dir,db_path=myDuckDB)

    # 可选: 验证数据是否已成功加载
    try:
        con_check = duckdb.connect(myDuckDB)
        result = con_check.execute("SELECT * FROM etf_daily order by trade_date desc LIMIT 7").fetchdf()
        print("\n查询示例 (数据库中前5行数据):")
        print(result)
        con_check.close()
    except Exception as e:
        print(f"查询数据库失败: {e}")