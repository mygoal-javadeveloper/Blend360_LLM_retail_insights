"""
Agents:
- build_sql_agent (Fuzzy SQL Generator using direct llm.create)
- LanguageToSQLAgent (Mistral via langchain-ollama ChatOllama)
- SQLExecutionAgent (DuckDB)
- ValidationAgent (Safety)
"""

import json
from typing import Dict, List

from langchain_ollama import ChatOllama
from langchain_core.messages import SystemMessage, HumanMessage

from utils import (
    get_duckdb_connection,
    list_tables,
    get_table_schema_summary
)


LLM = ChatOllama(model="mistral", temperature=0.0)


SYSTEM_PROMPT = """
You are the Retail Insights SQL Agent.

Your job:
- Convert the user question into correct SQL for DuckDB.
- Work even if data is dirty, inconsistent, misspelled, or mixed-case.
- Always use fuzzy logic for status, shipping levels, categories, etc.

===========================
FUZZY VALUE RULES
===========================

1. STATUS NORMALIZATION  
Use LOWER(status) LIKE patterns.

SHIPPED:
  "shipped", "shipping", "shipped - delivered", "ship", "shippd", "shiped"

CANCELLED:
  "cancelled", "canceled", "cncl", "cnx", "cancel", "cancelled by buyer"

RETURNED:
  "returned", "return", "returned to seller", "refund"

PENDING:
  "pending", "pending - waiting for pick up", "waiting pickup"

2. SHIPPING SERVICE LEVELS  
Use LOWER(ship_service_level) with LIKE fuzzy matches:

Standard → ["standard", "std", "stnd"]  
Expedited → ["expedited", "exp", "express"]

===========================
MANDATORY SQL RULES
===========================

ALWAYS:
- Use LOWER(column) in comparisons.
- Use LIKE wildcard matches.
- Never use "=" for status or shipping level.
- When user asks for shipped:
    LOWER(status) LIKE '%ship%'
- When user asks for cancelled:
    LOWER(status) LIKE '%cancel%' OR LOWER(status) LIKE '%cn%'

===========================
OUTPUT FORMAT
===========================

Return only JSON:
{
  "sql": "<SQL QUERY>",
  "tables_used": ["table1"]
}
"""


def build_sql_agent(llm):
    """
    llm: an LLM with .create() interface (Ollama, GPT, LM Studio).
    Returns generate_sql() function.
    """
    def generate_sql(question: str, tables: List[str]) -> Dict:
        user_prompt = f"""
User question:
{question}

Available tables:
{tables}

Return ONLY valid JSON.
"""

        response = llm.create(
            model="mistral",
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user_prompt}
            ],
            temperature=0.1
        )

        try:
            raw = response["choices"][0]["message"]["content"]
            return json.loads(raw)
        except Exception:
            return {"sql": "", "tables_used": []}

    return generate_sql



SQL_SYSTEM_PROMPT = (
    "You are a SQL expert for DuckDB. Given schema summaries and a question, "
    "return ONE valid SQL SELECT statement. No markdown."
)

class LanguageToSQLAgent:
    def __init__(self):
        self.llm = LLM

    def _schema_text(self, table_name=None):
        if table_name:
            return f"Table `{table_name}`: {get_table_schema_summary(table_name)}"
        return "\n".join(
            f"Table `{t}`: {get_table_schema_summary(t)}"
            for t in list_tables()
        )

    def _clean_response(self, text: str) -> str:
        s = (text or "").strip()
        if "```" in s:
            parts = s.split("```")
            s = parts[-2] if len(parts) >= 3 else s.replace("```", "")
        s = s.lstrip()
        if s.lower().startswith("sql"):
            s = s[3:].strip()
        if not s.endswith(";"):
            s += ";"
        return s

    def question_to_sql(self, question: str, table_name=None):
        schema = self._schema_text(table_name)
        resp = self.llm.invoke([
            SystemMessage(content=SQL_SYSTEM_PROMPT),
            HumanMessage(
                content=f"Schema:\n{schema}\n\nQuestion: {question}\nReturn only SQL."
            )
        ])
        return self._clean_response(resp.content or "")




class SQLExecutionAgent:
    def __init__(self, db_file: str = "data/retail_insights.duckdb"):
        self.db_file = db_file

    def run_sql(self, sql: str):
        con = get_duckdb_connection(self.db_file)
        try:
            df = con.execute(sql).df()
            con.close()
            return {"ok": True, "df": df}
        except Exception as e:
            con.close()
            return {"ok": False, "error": str(e)}



class ValidationAgent:
    forbidden_tokens = ["drop ", "delete ", "update ", "insert ", "alter ", "create "]

    def _clean(self, sql: str) -> str:
        s = (sql or "").lower()
        s = s.replace("```", "").strip()
        if s.startswith("sql"):
            s = s[3:].strip()
        return s

    def validate(self, sql: str):
        s = self._clean(sql)
        for t in self.forbidden_tokens:
            if t in s:
                return False, f"Forbidden operation detected: {t.strip()}"
        if not s.startswith("select"):
            return False, "Only SELECT queries allowed."
        return True, "SQL OK"
