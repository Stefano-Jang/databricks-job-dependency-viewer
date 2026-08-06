import os

import pandas as pd
import streamlit as st
from databricks import sql
from databricks.sdk.core import Config

assert os.getenv("DATABRICKS_WAREHOUSE_ID"), "DATABRICKS_WAREHOUSE_ID must be set in app.yaml or environment."
assert os.getenv("DAG_TABLE_NAME"), "DAG_TABLE_NAME must be set in app.yaml or environment."

DAG_TABLE_NAME = os.getenv("DAG_TABLE_NAME")
WAREHOUSE_ID = os.getenv("DATABRICKS_WAREHOUSE_ID")
# The app reads only the graph table: the job already folds the shared edge layer
# into it (NODES / EDGES / SHARED_TABLES rows), so the edge table is not queried
# from here.
IMPACT_MAX_DEPTH = int(os.getenv("IMPACT_MAX_DEPTH", "5"))
FAILURE_LOOKBACK_DAYS = int(os.getenv("FAILURE_LOOKBACK_DAYS", "7"))

NODE_COLUMNS = [
    "id", "type", "subtype", "name", "description", "creator_email", "run_as_email",
    "is_failed", "last_failed_time", "first_failed_time", "failure_count",
    "failure_detail", "last_activity_time", "created_time", "status",
    "in_degree", "out_degree", "downstream_reach", "criticality_rank",
]
EDGE_COLUMNS = ["id", "type", "source_id", "target_id", "connecting_tables", "edge_table_count", "edge_kinds"]
SHARED_TABLE_COLUMNS = ["id", "name", "writer_count"]
# The Insights tab needs identity plus ranking metrics, not the full node row.
OVERVIEW_COLUMNS = [
    "id", "type", "subtype", "name", "description", "creator_email", "run_as_email",
    "is_failed", "last_failed_time", "failure_count", "downstream_reach", "criticality_rank",
]


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


def _escape_sql_string(s: str) -> str:
    """Escape a string for safe use in SQL string literals."""
    return "'" + s.replace("'", "''") + "'"


NODE_TIME_COLUMNS = ("last_failed_time", "first_failed_time", "last_activity_time", "created_time")
NODE_INT_COLUMNS = ("failure_count", "in_degree", "out_degree", "downstream_reach", "criticality_rank")


def _coerce_nodes(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize node dtypes in place.

    Every loader that returns node rows goes through here, so the coercion rules
    live in one place instead of drifting between copies. Columns absent from a
    given projection are skipped rather than forced.
    """
    if df.empty:
        return df
    df["id"] = df["id"].astype(str)
    if "is_failed" in df.columns:
        df["is_failed"] = df["is_failed"].astype("boolean").fillna(False).astype(bool)
    for col in NODE_TIME_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], utc=True, errors="coerce")
    for col in NODE_INT_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
    return df


def _coerce_edges(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize edge dtypes in place; empty frames keep the expected columns."""
    if df.empty:
        return pd.DataFrame(columns=EDGE_COLUMNS)
    df["source_id"] = df["source_id"].astype(str)
    df["target_id"] = df["target_id"].astype(str)
    df["edge_table_count"] = pd.to_numeric(df["edge_table_count"], errors="coerce").fillna(0).astype(int)
    return df


@st.cache_data(ttl=300)
def load_incidents(user_key: str, window_hours: float):
    """Failed nodes only, server-side filtered on last_failed_time within window.

    user_key keeps the cache per-user and window_hours keys the result on time window.
    """
    since_expr = f"TIMESTAMPADD(HOUR, -{float(window_hours)}, CURRENT_TIMESTAMP())"
    query = f"""
        SELECT id, type, subtype, name, description, creator_email, run_as_email,
               is_failed, last_failed_time, first_failed_time, failure_count,
               failure_detail, last_activity_time, created_time, status,
               in_degree, out_degree, downstream_reach, criticality_rank
        FROM {DAG_TABLE_NAME}
        WHERE result_type = 'NODES'
          AND is_failed = true
          AND last_failed_time >= {since_expr}
    """
    df = query_databricks(query)
    # A workspace with no failures is the good case, not an error. Return a frame
    # that still carries the expected columns so callers can index into it.
    if df.empty:
        return pd.DataFrame(columns=NODE_COLUMNS)
    return _coerce_nodes(df)


@st.cache_data(ttl=300)
def load_subgraph(user_key: str, root_id: str, depth: int):
    """Incident subgraph: root + downstream up to depth hops + direct upstream context.

    Expands hops in SQL (fixed-depth joins) so browser never receives full edge set.
    """
    root_id_safe = _escape_sql_string(root_id)
    hops = max(1, min(int(depth), IMPACT_MAX_DEPTH))
    # One CTE per hop, generated to the requested depth. Fixed-depth joins keep
    # this bounded and avoid the recursion row limit that a path-tracking
    # recursive CTE hits on a large workspace.
    hop_ctes, hop_unions = [], ["SELECT id FROM h1"]
    hop_ctes.append(
        "h1 AS (SELECT DISTINCT target_id AS id FROM edges "
        "WHERE source_id IN (SELECT id FROM root))"
    )
    for n in range(2, hops + 1):
        hop_ctes.append(
            f"h{n} AS (SELECT DISTINCT e.target_id AS id FROM h{n - 1} p "
            f"JOIN edges e ON e.source_id = p.id)"
        )
        hop_unions.append(f"SELECT id FROM h{n}")
    downstream_sql = " UNION ".join(hop_unions)

    query = f"""
        WITH root AS (
            SELECT id FROM {DAG_TABLE_NAME}
            WHERE result_type = 'NODES' AND id = {root_id_safe}
        ),
        edges AS (
            SELECT source_id, target_id FROM {DAG_TABLE_NAME}
            WHERE result_type = 'EDGES'
        ),
        {', '.join(hop_ctes)},
        downstream AS ({downstream_sql}),
        -- direct upstream only: enough to show what fed the failure
        upstream AS (
            SELECT DISTINCT source_id AS id FROM edges
            WHERE target_id IN (SELECT id FROM root)
        ),
        all_ids AS (
            SELECT id FROM root
            UNION SELECT id FROM downstream
            UNION SELECT id FROM upstream
        )
        SELECT id, type, subtype, name, description, creator_email, run_as_email,
               is_failed, last_failed_time, first_failed_time, failure_count,
               failure_detail, last_activity_time, created_time, status,
               in_degree, out_degree, downstream_reach, criticality_rank
        FROM {DAG_TABLE_NAME}
        WHERE result_type = 'NODES' AND id IN (SELECT id FROM all_ids)
    """
    nodes_df = query_databricks(query)
    if nodes_df.empty:
        return pd.DataFrame(columns=NODE_COLUMNS), pd.DataFrame(columns=EDGE_COLUMNS)

    nodes_df = _coerce_nodes(nodes_df)

    # Edges are fetched for the resolved node set, so the subgraph is closed:
    # every edge returned has both endpoints present.
    node_ids_quoted = ",".join(_escape_sql_string(nid) for nid in nodes_df["id"])
    edges_df = query_databricks(f"""
        SELECT id, type, source_id, target_id, connecting_tables, edge_table_count, edge_kinds
        FROM {DAG_TABLE_NAME}
        WHERE result_type = 'EDGES'
          AND source_id IN ({node_ids_quoted})
          AND target_id IN ({node_ids_quoted})
    """)
    return nodes_df, _coerce_edges(edges_df)


@st.cache_data(ttl=300)
def load_overview(user_key: str, limit: int = 50):
    """Top-N nodes by criticality_rank for the Insights tab."""
    query = f"""
        SELECT id, type, subtype, name, description, creator_email, run_as_email,
               is_failed, last_failed_time, failure_count, downstream_reach, criticality_rank
        FROM {DAG_TABLE_NAME}
        WHERE result_type = 'NODES'
        ORDER BY criticality_rank ASC
        LIMIT {int(limit)}
    """
    df = query_databricks(query)
    # Projection is deliberately narrower than NODE_COLUMNS: the Insights table
    # only needs identity plus the ranking metrics.
    if df.empty:
        return pd.DataFrame(columns=OVERVIEW_COLUMNS)
    return _coerce_nodes(df)


@st.cache_data(ttl=300)
def load_shared_tables(user_key: str):
    """Shared (hub) tables and their writer counts."""
    df = query_databricks(f"""
        SELECT id, name, writer_count
        FROM {DAG_TABLE_NAME}
        WHERE result_type = 'SHARED_TABLES'
        ORDER BY writer_count DESC
    """)
    if df.empty:
        return pd.DataFrame(columns=SHARED_TABLE_COLUMNS)
    df["writer_count"] = pd.to_numeric(df["writer_count"], errors="coerce").fillna(0).astype(int)
    return df


@st.cache_data(ttl=300)
def load_stats(user_key: str):
    """Workspace-level aggregates, computed server-side.

    Returns a dict of scalars plus a per-type breakdown DataFrame. This is what
    lets the header metrics and the Insights tab render without the app ever
    loading the graph.
    """
    totals = query_databricks(f"""
        SELECT
          SUM(CASE WHEN result_type = 'NODES' THEN 1 ELSE 0 END)          AS total_nodes,
          SUM(CASE WHEN result_type = 'EDGES' THEN 1 ELSE 0 END)          AS total_edges,
          SUM(CASE WHEN result_type = 'SHARED_TABLES' THEN 1 ELSE 0 END)  AS shared_tables,
          SUM(CASE WHEN result_type = 'NODES' AND is_failed THEN 1 ELSE 0 END) AS failed_nodes,
          MAX(CASE WHEN result_type = 'NODES' THEN last_activity_time END)     AS freshness
        FROM {DAG_TABLE_NAME}
    """)
    by_type = query_databricks(f"""
        SELECT type,
               COALESCE(subtype, '-')                        AS subtype,
               COUNT(*)                                      AS entities,
               SUM(CASE WHEN is_failed THEN 1 ELSE 0 END)    AS failed
        FROM {DAG_TABLE_NAME}
        WHERE result_type = 'NODES'
        GROUP BY type, COALESCE(subtype, '-')
        ORDER BY entities DESC
    """)

    out = {"by_type": by_type}
    # An aggregate query over an empty table returns one row of NULLs, not zero
    # rows, so `totals.empty` is False here and every scalar is NaN. That happens
    # on a fresh deploy before the job has run. Coerce NaN to 0 explicitly --
    # `int(nan or 0)` raises, because NaN is truthy.
    row = totals.iloc[0] if not totals.empty else {}
    for key in ("total_nodes", "total_edges", "shared_tables", "failed_nodes"):
        value = pd.to_numeric(row.get(key), errors="coerce")
        out[key] = 0 if pd.isna(value) else int(value)
    out["freshness"] = pd.to_datetime(row.get("freshness"), utc=True, errors="coerce")
    return out


@st.cache_data(ttl=300)
def load_failed_edges(user_key: str, failed_ids: tuple):
    """Edges between failed entities only — the input to cascade classification.

    Cascade detection only needs to know whether one failure sits downstream of
    another, so this stays small no matter how large the workspace is.
    """
    # Drop nulls rather than stringifying them: str(None) would become the
    # literal 'None' and silently match nothing.
    ids = [str(i) for i in failed_ids if i is not None and str(i).strip()]
    if not ids:
        return pd.DataFrame(columns=EDGE_COLUMNS)
    quoted = ",".join(_escape_sql_string(i) for i in ids)
    df = query_databricks(f"""
        SELECT id, type, source_id, target_id, connecting_tables, edge_table_count, edge_kinds
        FROM {DAG_TABLE_NAME}
        WHERE result_type = 'EDGES'
          AND source_id IN ({quoted})
          AND target_id IN ({quoted})
    """)
    return _coerce_edges(df)


@st.cache_data(ttl=300)
def load_graph(user_key: str):
    """Load full graph (nodes, edges, shared tables) for fallback/legacy use.

    This query does NOT filter, so it should only be used when absolutely necessary.
    Prefer scoped queries (load_incidents, load_subgraph, etc.) instead.

    user_key keeps the cache per-user.
    """
    query = f"""
        SELECT result_type, id, type, subtype, name, description, creator_email, run_as_email,
               is_failed, last_failed_time, first_failed_time, failure_count,
               failure_detail, last_activity_time, created_time, status,
               in_degree, out_degree, downstream_reach, criticality_rank,
               source_id, target_id, connecting_tables, edge_table_count, edge_kinds, writer_count
        FROM {DAG_TABLE_NAME}
    """
    df = query_databricks(query)
    if df.empty:
        return pd.DataFrame(columns=NODE_COLUMNS), pd.DataFrame(columns=EDGE_COLUMNS)

    nodes_df = _coerce_nodes(df[df["result_type"] == "NODES"][NODE_COLUMNS].copy())
    edges_df = _coerce_edges(df[df["result_type"] == "EDGES"][EDGE_COLUMNS].copy())
    return nodes_df, edges_df
