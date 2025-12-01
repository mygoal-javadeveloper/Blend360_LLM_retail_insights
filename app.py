import streamlit as st
from utils import load_all_csvs_into_duckdb, list_tables, get_table_schema_summary, create_unified_sales_view, create_master_sales, DB_FILE
from agents import LanguageToSQLAgent, SQLExecutionAgent, ValidationAgent
import duckdb
from langchain_ollama import ChatOllama
from langchain_core.messages import SystemMessage, HumanMessage
import pandas as pd

st.set_page_config(page_title="Retail Insights Assistant", layout="wide")
st.title("Retail Insights Assistant — Local Mistral (Ollama)")

st.sidebar.header("Data / Controls")
if st.sidebar.button("Clean & (Re)Load all CSVs into DuckDB"):
    with st.spinner("Cleaning CSVs and loading into DuckDB..."):
        loaded = load_all_csvs_into_duckdb(folder="data")
        unified_candidates = create_unified_sales_view()
    st.sidebar.success(f"Loaded: {loaded} | unified candidates: {unified_candidates}")

if st.sidebar.button("Create master_sales (merge compatible sales tables)"):
    with st.spinner("Creating master_sales table..."):
        created = create_master_sales()
    st.sidebar.success(f"Master created from: {created}")

tables = list_tables()
st.sidebar.markdown("**Detected tables**")
if tables:
    for t in tables:
        st.sidebar.write("- " + t)
else:
    st.sidebar.info("No tables detected. Click Clean & (Re)Load to ingest CSVs from /data")

mode = st.sidebar.radio("Mode", ["Summarization", "Conversational Q&A"])

if mode == "Summarization":
    st.header("Summarization Mode")
    if not tables:
        st.warning("No data loaded. Use the sidebar controls to load CSVs.")
    else:
        table = st.selectbox("Choose a table to summarize", tables)
        con = duckdb.connect(DB_FILE)
        sample = con.execute(f"SELECT * FROM {table} LIMIT 10").df()
        st.subheader("Sample rows")
        st.dataframe(sample)
        if st.button("Generate Summary"):

            llm = ChatOllama(model="mistral", temperature=0.2)

            con = duckdb.connect(DB_FILE)
            df_all = con.execute(f"SELECT * FROM {table}").df()
            con.close()

            num_rows = len(df_all)
            dtypes = df_all.dtypes.astype(str).to_dict()


            numeric_cols = [c for c in df_all.select_dtypes(include='number').columns]
            categorical_cols = [c for c in df_all.select_dtypes(include='object').columns]


            important_numeric = []
            if numeric_cols:
                variances = df_all[numeric_cols].var().sort_values(ascending=False)
                important_numeric = list(variances.head(3).index)


            important_categorical = []
            if categorical_cols:
                uniques = df_all[categorical_cols].nunique().sort_values(ascending=False)
                important_categorical = list(uniques.head(3).index)


            small_stats = {}
            for col in important_numeric:
                small_stats[col] = {
                    "mean": float(df_all[col].mean()),
                    "min": float(df_all[col].min()),
                    "max": float(df_all[col].max()),
                    "std": float(df_all[col].std()),
                }

            top_values = {}
            for col in important_categorical:
                top_values[col] = df_all[col].value_counts().head(3).to_dict()


            llm_input = f"""
            You are an expert senior data analyst. Provide a deep dataset summary using ONLY the following key information.

            Dataset: {table}
            Total Rows: {num_rows}

            Column Types:
            {dtypes}

            MOST IMPORTANT NUMERIC COLUMNS (Top 3 by variance):
            {important_numeric}

            Stats for important numeric columns:
            {small_stats}

            MOST IMPORTANT CATEGORICAL COLUMNS (Top 3 by uniqueness):
            {important_categorical}

            Top categorical values:
            {top_values}

            Write:
            - What this dataset is about
            - Key patterns
            - Column-level insights
            - Data quality issues
            - Trends & business interpretation
            - Any anomalies or surprising patterns

            Write in readable paragraphs, not bullet points.
            """

            with st.spinner("Generating deep LLM summary…"):
                response = llm.invoke(llm_input)

            st.subheader("Deep Summary")
            st.write(response.content)

        con.close()

else:
    st.header("Conversational Q&A")
    question = st.text_input("Your question")
    restrict = st.selectbox("Optional: restrict to a table (or choose All)", ["All"] + tables)
    if st.button("Ask") and question.strip():
        lang_agent = LanguageToSQLAgent()
        exec_agent = SQLExecutionAgent()
        val_agent = ValidationAgent()

        with st.spinner("Generating SQL..."):
            if restrict == "All":
                sql = lang_agent.question_to_sql(question, table_name=None)
            else:
                sql = lang_agent.question_to_sql(question, table_name=restrict)

        st.subheader("Generated SQL")
        st.code(sql)

        ok, msg = val_agent.validate(sql)
        if not ok:
            st.error("Validation failed: " + msg)
        else:
            res = exec_agent.run_sql(sql)
            if not res["ok"]:
                st.error("SQL execution error: " + res["error"])
            else:
                df = res["df"]
                st.subheader("Result")
                st.dataframe(df)
                st.markdown("Result (first 10 rows):")
                st.json(df.head(10).to_dict(orient="records"))
