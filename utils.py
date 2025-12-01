"""
Utilities for Retail Insights Assistant:
- auto-load & preprocess CSVs
- safe CSV reading
- filename cleaning
- column normalization
- value standardization (status, currency, date, qty)
- ingestion into DuckDB
- relationship discovery for JOIN suggestions
- optional unified_sales view
- optional master_sales table
"""

import os
import re
import csv
import duckdb
import pandas as pd
from typing import List, Dict

DB_FILE = "data/retail_insights.duckdb"


def safe_read_csv(path: str) -> pd.DataFrame | None:
    """
    Tries multiple read strategies to avoid CSV load failures.
    """
    try:
        return pd.read_csv(path, low_memory=False)
    except Exception as e1:
        print(f"[WARN] fast read failed {path}: {e1}")

    try:
        return pd.read_csv(path, engine="python", on_bad_lines="skip", sep=None, low_memory=False)
    except Exception as e2:
        print(f"[WARN] python engine failed {path}: {e2}")

    try:
        return pd.read_csv(path, engine="python", on_bad_lines="skip",
                           quoting=csv.QUOTE_NONE, sep=None, low_memory=False)
    except Exception as e3:
        print(f"[ERROR] cannot read CSV {path}: {e3}")
        return None


def clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = (
        df.columns.astype(str)
            .str.strip()
            .str.lower()
            .str.replace('[^a-z0-9]+', '_', regex=True)
            .str.replace('_+', '_', regex=True)
            .str.strip('_')
    )
    return df


def normalize_values(df: pd.DataFrame) -> pd.DataFrame:

    # ----- Normalize status -----
    if "status" in df.columns:
        df["status"] = df["status"].astype(str).str.lower().str.strip()

        df["status"] = df["status"].replace({
            r".*shipped.*": "shipped",
            r".*delivered.*": "shipped",
            r".*cancel.*": "cancelled",
            r".*return.*": "returned",
        }, regex=True)

    # ----- Normalize amount -----
    amt_cols = ["amount", "gross_amt", "price", "rate", "sales", "total"]
    for col in amt_cols:
        if col in df.columns:
            df[col] = (
                df[col]
                .astype(str)
                .str.replace(r"[^0-9.\-]", "", regex=True)
            )
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    # ----- Normalize qty -----
    qty_cols = ["qty", "pcs", "quantity", "units", "stock"]
    for col in qty_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

    # ----- Normalize date columns -----
    date_candidates = [c for c in df.columns if "date" in c or c in ["months", "month"]]
    for dc in date_candidates:
        try:
            df[dc] = pd.to_datetime(df[dc], errors="coerce")
        except:
            pass

    return df



def clean_filename(fname: str) -> str:
    new = fname.lower().strip().replace(" ", "_")
    new = re.sub(r"[^a-z0-9._]", "", new)
    return new



def ensure_data_folder(folder="data"):
    os.makedirs(folder, exist_ok=True)



def clean_and_normalize_csv_files(folder="data") -> List[str]:
    """
    - Renames files safely
    - loads CSV with safe reader
    - cleans columns
    - fixes statuses, amounts, qty, dates
    - writes cleaned CSV back to disk
    """
    ensure_data_folder(folder)
    processed = []

    for filename in os.listdir(folder):
        if not filename.lower().endswith(".csv"):
            continue

        old_path = os.path.join(folder, filename)
        new_name = clean_filename(filename)
        new_path = os.path.join(folder, new_name)

        # rename file
        if old_path != new_path:
            try:
                os.rename(old_path, new_path)
                print(f"[RENAME] {filename} → {new_name}")
            except Exception as e:
                print(f"[WARN] rename failed: {e}")
                continue

        # read file
        df = safe_read_csv(new_path)
        if df is None:
            print(f"[SKIP] unreadable: {new_name}")
            continue

        # clean & normalize
        df = clean_columns(df)
        df = normalize_values(df)

        # save cleaned
        try:
            df.to_csv(new_path, index=False)
            processed.append(new_name)
            print(f"[CLEANED] {new_name}")
        except Exception as e:
            print(f"[ERROR] cannot save CSV: {e}")

    return processed



def get_duckdb_connection(db_file=DB_FILE):
    os.makedirs(os.path.dirname(db_file) or ".", exist_ok=True)
    return duckdb.connect(database=db_file)



def load_all_csvs_into_duckdb(folder="data", db_file=DB_FILE, verbose=True) -> List[str]:
    processed = clean_and_normalize_csv_files(folder)
    con = get_duckdb_connection(db_file)
    tables = []

    for fname in processed:
        path = os.path.join(folder, fname)
        table = os.path.splitext(fname)[0]

        try:
            con.execute(f"CREATE OR REPLACE TABLE {table} AS SELECT * FROM read_csv_auto('{path}')")
            tables.append(table)
            if verbose:
                print(f"[LOADED] {fname} → table `{table}`")
        except Exception as e:
            print(f"[ERROR] load failed: {e}")

    con.close()
    return tables



def list_tables(db_file=DB_FILE) -> List[str]:
    con = get_duckdb_connection(db_file)
    try:
        df = con.execute("SHOW TABLES").df()
        tables = df["name"].tolist()
    except:
        tables = []
    con.close()
    return tables



def get_table_schema_summary(table, db_file=DB_FILE) -> str:
    con = get_duckdb_connection(db_file)
    try:
        df = con.execute(f"PRAGMA table_info('{table}')").df()
    except:
        con.close()
        return ""
    con.close()

    rows = [f"{r['name']} ({r['type']})" for _, r in df.iterrows()]
    return ", ".join(rows)



def build_relationship_graph(db_file=DB_FILE) -> Dict[str, Dict[str, List[tuple]]]:
    con = get_duckdb_connection(db_file)
    tables = list_tables(db_file)

    schemas = {
        t: con.execute(f"PRAGMA table_info('{t}')").df()
        for t in tables
    }

    graph = {}

    for t1 in tables:
        graph[t1] = {}
        cols1 = schemas[t1]["name"].tolist()

        for t2 in tables:
            if t1 == t2:
                continue

            cols2 = schemas[t2]["name"].tolist()
            common = set(cols1).intersection(cols2)

            for c in common:
                graph[t1].setdefault(c, []).append((t2, c))

    con.close()
    return graph



def create_unified_sales_view(db_file=DB_FILE):
    con = get_duckdb_connection(db_file)
    tables = list_tables(db_file)

    candidate = []
    for t in tables:
        schema = con.execute(f"PRAGMA table_info('{t}')").df()
        cols = [c.lower() for c in schema["name"].tolist()]
        if ("sku" in cols or "sku_code" in cols or "style_id" in cols) and \
           ("date" in cols or "order_date" in cols or "months" in cols):
            candidate.append(t)

    if not candidate:
        con.close()
        return []

    selects = []
    for t in candidate:
        schema = con.execute(f"PRAGMA table_info('{t}')").df()
        cols = [c.lower() for c in schema["name"].tolist()]

        date_col = next((c for c in cols if c in ["date", "order_date", "months"]), None)
        sku_col = next((c for c in cols if c in ["sku", "sku_code", "style_id"]), None)
        qty_col = next((c for c in cols if c in ["qty", "pcs", "stock"]), None)
        amt_col = next((c for c in cols if c in ["amount", "gross_amt", "rate"]), None)

        proj = [
            f"{date_col} AS date" if date_col else "NULL::timestamp AS date",
            f"{sku_col} AS sku" if sku_col else "NULL AS sku",
            f"{qty_col} AS qty" if qty_col else "0 AS qty",
            f"{amt_col} AS amount" if amt_col else "0 AS amount",
            f"'{t}' AS source"
        ]

        selects.append("SELECT " + ", ".join(proj) + f" FROM {t}")

    sql = "CREATE OR REPLACE VIEW unified_sales AS\n" + "\nUNION ALL\n".join(selects)
    con.execute(sql)
    con.close()

    print("[VIEW] unified_sales created")
    return candidate



def create_master_sales(db_file=DB_FILE, out_table="master_sales"):
    con = get_duckdb_connection(db_file)
    tables = list_tables(db_file)

    candidate = []
    for t in tables:
        cols = [c.lower() for c in con.execute(f"PRAGMA table_info('{t}')").df()["name"].tolist()]
        if ("sku" in cols or "sku_code" in cols or "style_id" in cols) and \
           ("amount" in cols or "qty" in cols or "gross_amt" in cols):
            candidate.append(t)

    if not candidate:
        con.close()
        return []

    selects = []

    for t in candidate:
        cols = [c.lower() for c in con.execute(f"PRAGMA table_info('{t}')").df()["name"].tolist()]

        order_id = next((c for c in cols if "order" in c and "id" in c), None)
        date_col = next((c for c in cols if c in ["date", "order_date", "months"]), None)
        sku_col = next((c for c in cols if c in ["sku", "sku_code", "style_id"]), None)
        qty_col = next((c for c in cols if c in ["qty", "pcs", "stock"]), None)
        amt_col = next((c for c in cols if c in ["amount", "gross_amt", "rate"]), None)

        proj = [
            f"{order_id} AS order_id" if order_id else "NULL AS order_id",
            f"{date_col} AS date" if date_col else "NULL::timestamp AS date",
            f"{sku_col} AS sku" if sku_col else "NULL AS sku",
            f"{qty_col} AS qty" if qty_col else "0 AS qty",
            f"{amt_col} AS amount" if amt_col else "0 AS amount",
            f"'{t}' AS source"
        ]

        selects.append("SELECT " + ", ".join(proj) + f" FROM {t}")

    sql = f"CREATE OR REPLACE TABLE {out_table} AS\n" + "\nUNION ALL\n".join(selects)
    con.execute(sql)
    con.close()

    print(f"[MASTER] {out_table} created")
    return candidate
