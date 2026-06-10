import os

import pandas as pd
import streamlit as st
from databricks import sql
from databricks.sdk.core import Config

assert os.getenv("DATABRICKS_WAREHOUSE_ID"), "DATABRICKS_WAREHOUSE_ID must be set in app.yaml or environment."
assert os.getenv("DAG_TABLE_NAME"), "DAG_TABLE_NAME must be set in app.yaml or environment."

DAG_TABLE_NAME = os.getenv("DAG_TABLE_NAME")
WAREHOUSE_ID = os.getenv("DATABRICKS_WAREHOUSE_ID")

NODE_COLUMNS = [
    "id", "type", "name", "description", "creator_email", "run_as_email",
    "is_failed", "last_failed_time", "first_failed_time", "failure_count",
    "failure_detail", "last_activity_time", "created_time", "status",
]
EDGE_COLUMNS = ["id", "type", "source_id", "target_id", "connecting_tables", "edge_table_count"]


def _user_token():
    """On-behalf-of-user token forwarded by Databricks Apps, if present."""
    try:
        return st.context.headers.get("X-Forwarded-Access-Token")
    except Exception:
        return None


def current_user_key() -> str:
    """Identity used to partition the data cache per user (OBO isolation)."""
    try:
        return (st.context.headers.get("X-Forwarded-Email")
                or st.context.headers.get("X-Forwarded-User")
                or "local")
    except Exception:
        return "local"


def _connect():
    cfg = Config()
    http_path = f"/sql/1.0/warehouses/{WAREHOUSE_ID}"
    token = _user_token()
    if token:
        return sql.connect(server_hostname=cfg.host, http_path=http_path, access_token=token)
    # Fallback: app service principal or local default auth (development)
    return sql.connect(server_hostname=cfg.host, http_path=http_path,
                       credentials_provider=lambda: cfg.authenticate)


def query_databricks(query: str) -> pd.DataFrame:
    try:
        with _connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(query)
                return cursor.fetchall_arrow().to_pandas()
    except Exception as e:
        raise Exception(
            f"Databricks query failed: {e}. Check warehouse ID, table existence and permissions."
        )


@st.cache_data(ttl=300)
def load_graph(user_key: str):
    """Load the DAG table once per user and split into (nodes_df, edges_df).

    user_key keeps the cache per-user: queries run with the requesting user's
    on-behalf-of token, so results must never be shared across users.
    """
    df = query_databricks(f"""
        SELECT result_type, id, type, name, description, creator_email, run_as_email,
               is_failed, last_failed_time, first_failed_time, failure_count,
               failure_detail, last_activity_time, created_time, status,
               source_id, target_id, connecting_tables, edge_table_count
        FROM {DAG_TABLE_NAME}
    """)
    nodes_df = df[df["result_type"] == "NODES"][NODE_COLUMNS].copy()
    edges_df = df[df["result_type"] == "EDGES"][EDGE_COLUMNS].copy()

    nodes_df["id"] = nodes_df["id"].astype(str)
    nodes_df["is_failed"] = nodes_df["is_failed"].astype("boolean").fillna(False).astype(bool)
    for col in ("last_failed_time", "first_failed_time", "last_activity_time", "created_time"):
        nodes_df[col] = pd.to_datetime(nodes_df[col], utc=True, errors="coerce")
    nodes_df["failure_count"] = pd.to_numeric(nodes_df["failure_count"], errors="coerce").fillna(0).astype(int)

    edges_df["source_id"] = edges_df["source_id"].astype(str)
    edges_df["target_id"] = edges_df["target_id"].astype(str)
    return nodes_df, edges_df
