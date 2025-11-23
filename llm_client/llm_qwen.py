# llm_client/llm_qwen.py

import json
from pathlib import Path
from langchain_ollama import ChatOllama


class QwenOllamaClient:
    """
    使用 ChatOllama 调用本地 Qwen 模型，并支持动态加载 prompt 文件。
    自动加载 ETF 对照表 etf_map.json。
    """

    def __init__(
        self,
        model_name: str = "qwen3:4b",
        temperature: float = 0.01,
        prompt_file: str = None,
        etf_map_file: str = None
    ):
        self.model_name = model_name
        self.temperature = temperature

        # 默认 prompt 文件
        if prompt_file is None:
            root = Path(__file__).resolve().parents[1]
            prompt_file = root / "prompts/sql_prompt.txt"

        self.prompt_file_path = Path(prompt_file)
        self.base_prompt = self.prompt_file_path.read_text(encoding="utf-8")

        # 自动加载 ETF 对照表
        if etf_map_file is None:
            root = Path(__file__).resolve().parents[1]
            etf_map_file = root / "prompts/etf_map.json"

        self.etf_map_file = Path(etf_map_file)
        self.etf_map = self._load_etf_map()

        # 初始化模型
        self.llm = ChatOllama(
            model=self.model_name,
            temperature=self.temperature,
        )

    # -----------------------------
    # 加载 ETF 对照表
    # -----------------------------
    def _load_etf_map(self):
        if not self.etf_map_file.exists():
            print(f"⚠️ ETF 对照表不存在: {self.etf_map_file}，将使用空映射。")
            return {}
        return json.load(open(self.etf_map_file, "r", encoding="utf-8"))

    # -----------------------------
    # 动态切换 prompt 文件
    # -----------------------------
    def load_prompt(self, filepath: str):
        pf = Path(filepath)
        if not pf.exists():
            raise FileNotFoundError(f"Prompt not found: {pf}")
        self.base_prompt = pf.read_text(encoding="utf-8")
        self.prompt_file_path = pf

    # -----------------------------
    # 核心方法：填充 Prompt → 调用 LLM
    # -----------------------------
    def run_prompt(self, **kwargs) -> str:

        # 注入 ETF 对照表（自动）
        kwargs["etf_name_map"] = json.dumps(self.etf_map, ensure_ascii=False)

        text = self.base_prompt
        for k, v in kwargs.items():
            text = text.replace("{" + k + "}", str(v))

        result = self.llm.invoke(text)
        return result.content


if __name__ == "__main__":
    root = Path(__file__).resolve().parents[1]
    sql_prompt = root / "prompts/sql_prompt.txt"
    result_prompt = root / "prompts/result_prompt.txt"

    client = QwenOllamaClient(prompt_file=sql_prompt)

    print(client.run_prompt(user_question="今天涨得最多的ETF？"))
