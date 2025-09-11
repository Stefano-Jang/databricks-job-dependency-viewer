import streamlit as st
from st_link_analysis import st_link_analysis, NodeStyle, EdgeStyle
import pandas as pd
from conn import load_dag_data

st.set_page_config(page_title="JIIG", layout="wide")

# ---------- Utilities ----------
def _to_str_set(series: pd.Series) -> set:
    if series is None:
        return set()
    return set(series.dropna().astype(str).tolist())

def undirected_unique_edges(edges_df: pd.DataFrame) -> pd.DataFrame:
    if edges_df is None or edges_df.empty:
        return pd.DataFrame(columns=["u", "v"])
    e = edges_df[["source_id", "target_id"]].dropna().astype(str)
    e = e[e["source_id"] != e["target_id"]]
    if e.empty:
        return pd.DataFrame(columns=["u", "v"])
    mask = e["source_id"] <= e["target_id"]
    undirected = pd.DataFrame({
        "u": e["source_id"].where(mask, e["target_id"]),
        "v": e["target_id"].where(mask, e["source_id"])
    })
    return undirected.drop_duplicates(ignore_index=True)

def most_connected_node(nodes_df: pd.DataFrame, edges_df: pd.DataFrame):
    ue = undirected_unique_edges(edges_df)
    if ue.empty:
        return None, 0, "N/A"
    degree_counts = pd.concat([ue["u"], ue["v"]], ignore_index=True).value_counts()
    top_node_id = str(degree_counts.idxmax())
    top_node_degree = int(degree_counts.max())
    id_to_name = dict(zip(nodes_df["id"].astype(str), nodes_df["name"].astype(str)))
    return top_node_id, top_node_degree, id_to_name.get(top_node_id, top_node_id)

def apply_filter_by_mode(df: pd.DataFrame, mode: str) -> pd.DataFrame:
    if df is None or len(df) == 0:
        return df
    nodes_df = df[df["result_type"] == "NODES"].copy()
    edges_df = df[df["result_type"] == "EDGES"].copy()
    if nodes_df.empty:
        return df

    has_is_failed = "is_failed" in nodes_df.columns
    failed_ids = _to_str_set(nodes_df[nodes_df["is_failed"] == True]["id"]) if has_is_failed else set()
    connected_ids = _to_str_set(edges_df["source_id"]) | _to_str_set(edges_df["target_id"])

    if mode == "failed_subgraph":
        relevant_edges = edges_df[
            edges_df["source_id"].astype(str).isin(failed_ids) |
            edges_df["target_id"].astype(str).isin(failed_ids)
        ].copy()
        node_ids = failed_ids | _to_str_set(relevant_edges["source_id"]) | _to_str_set(relevant_edges["target_id"])
        relevant_nodes = nodes_df[nodes_df["id"].astype(str).isin(node_ids)].copy()
        return pd.concat([relevant_nodes, relevant_edges], ignore_index=True)

    if mode == "connected_only":
        node_ids = connected_ids | failed_ids
        relevant_nodes = nodes_df[nodes_df["id"].astype(str).isin(node_ids)].copy()
        relevant_edges = edges_df[
            edges_df["source_id"].astype(str).isin(node_ids) &
            edges_df["target_id"].astype(str).isin(node_ids)
        ].copy()
        return pd.concat([relevant_nodes, relevant_edges], ignore_index=True)

    return df  # 'all'

def deduplicate_nodes(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or len(df) == 0:
        return df
    nodes_df = df[df["result_type"] == "NODES"].copy()
    edges_df = df[df["result_type"] == "EDGES"].copy()
    if nodes_df.empty:
        return df

    out = []
    for _, g in nodes_df.groupby("id"):
        if len(g) == 1:
            row = g.iloc[0].copy()
            row["is_failed"] = bool(row.get("is_failed", False))
            row["status"] = row.get("status", "FAILED" if row["is_failed"] else "HEALTHY")
            out.append(row)
            continue
        if "is_failed" in g.columns and g["is_failed"].any():
            failed = g[g["is_failed"] == True]
            row = failed.sort_values("last_failed_time", ascending=False, na_position="last").iloc[0].copy() \
                  if "last_failed_time" in failed.columns else failed.iloc[0].copy()
        else:
            row = g.iloc[0].copy()
            row["is_failed"] = False
            row["status"] = "HEALTHY"
        out.append(row)

    nodes_out = pd.DataFrame(out)
    return pd.concat([nodes_out, edges_df], ignore_index=True)

def transform_to_st_link_analysis_format(df: pd.DataFrame):
    nodes_df = df[df["result_type"] == "NODES"].copy()
    edges_df = df[df["result_type"] == "EDGES"].copy()
    failure_map = dict(zip(nodes_df["id"].astype(str), nodes_df.get("is_failed", False).fillna(False)))

    highlight_id = str(st.session_state.get("highlight_node_id", "")) if "highlight_node_id" in st.session_state else ""

    nodes = []
    for _, node in nodes_df.iterrows():
        is_failed = bool(node.get("is_failed", False))
        entity_type = str(node["type"]).upper()
        base_style = f"{entity_type}_{'FAILED' if is_failed else 'HEALTHY'}"
        style_label = base_style if str(node["id"]) != highlight_id else f"{base_style}_HIGHLIGHT"
        nodes.append({"data": {
            "id": str(node["id"]),
            "label": style_label,
            "name": str(node["name"]),
            "entity_type": entity_type,
            "status": str(node.get("status", "")),
            "failure_count": int(node.get("failure_count", 0)) if pd.notna(node.get("failure_count")) else 0,
            "last_modified_time": str(node.get("created_time") or ""),
            "last_failed_time": str(node.get("last_failed_time") or ""),
            "job_id": str(node.get("job_id") or ""),
            "pipeline_id": str(node.get("pipeline_id") or ""),
            "creator": str(node.get("creator_email") or ""),
            "run_as": str(node.get("run_as_email") or "")
        }})

    edges = []
    for _, edge in edges_df.iterrows():
        s = str(edge["source_id"]); t = str(edge["target_id"])
        edge_label = "DEPENDENCY_FAILED" if bool(failure_map.get(s, False)) else "DEPENDENCY_HEALTHY"
        edges.append({"data": {
            "id": str(edge["id"]),
            "label": edge_label,
            "source": s,
            "target": t,
            "connecting_table": str(edge.get("connecting_table", "")),
            "source_failed": bool(failure_map.get(s, False)),
            "target_failed": bool(failure_map.get(t, False))
        }})
    return {"nodes": nodes, "edges": edges}

def create_node_styles():
    return [
        NodeStyle("JOB_HEALTHY", "#28A745", "name", "group"),
        NodeStyle("JOB_FAILED", "#DC3545", "name", "group"),
        NodeStyle("PIPELINE_HEALTHY", "#28A745", "name", "person"),
        NodeStyle("PIPELINE_FAILED", "#DC3545", "name", "person"),
        NodeStyle("JOB_HEALTHY_HIGHLIGHT", "#28A745", "name", "campaign"),
        NodeStyle("JOB_FAILED_HIGHLIGHT", "#DC3545", "name", "campaign"),
        NodeStyle("PIPELINE_HEALTHY_HIGHLIGHT", "#28A745", "name", "campaign"),
        NodeStyle("PIPELINE_FAILED_HIGHLIGHT", "#DC3545", "name", "campaign"),
    ]

def create_edge_styles():
    return [
        EdgeStyle("DEPENDENCY_HEALTHY", caption="connecting_table", directed=True),
        EdgeStyle("DEPENDENCY_FAILED", caption="connecting_table", directed=True),
    ]

def compute_metrics(nodes_df: pd.DataFrame, edges_df: pd.DataFrame):
    e_dir = len(edges_df)
    connected_ids = _to_str_set(edges_df["source_id"]) | _to_str_set(edges_df["target_id"])
    n_conn = len(connected_ids)
    n_all = len(nodes_df)
    avg_all = (e_dir / n_all) if n_all > 0 else 0.0

    ue = undirected_unique_edges(edges_df)
    e_undir = len(ue)
    avg_conn = (2 * e_undir / n_conn) if n_conn > 0 else 0.0

    failed = len(nodes_df[nodes_df["is_failed"] == True]) if "is_failed" in nodes_df.columns else 0
    healthy = n_all - failed
    return {
        "edges_directed": e_dir,
        "edges_undirected": e_undir,
        "num_nodes_connected": n_conn,
        "num_nodes_all": n_all,
        "avg_degree_all": avg_all,
        "avg_degree_connected": avg_conn,
        "failed_entities": failed,
        "healthy_entities": healthy,
    }

# ---------- App ----------
def main():
    st.title("🔗 Job & Pipeline Dependency Graph(within 24 hours)")

    if "applied_filter_mode" not in st.session_state:
        st.session_state["applied_filter_mode"] = "failed_subgraph"
    if "applied_layout" not in st.session_state:
        st.session_state["applied_layout"] = "concentric"

    st.sidebar.header("🔧 Filters")
    mode_label_to_key = {
        "(1) Failed+affected": "failed_subgraph",
        "(2) (1) + edge node": "connected_only",
        "(3) All nodes": "all",
    }
    selected_label = st.sidebar.radio(
        "Select Filter Options",
        list(mode_label_to_key.keys()),
        index=["failed_subgraph","connected_only","all"].index(st.session_state["applied_filter_mode"]),
        key="filter_mode_label"
    )
    pending_filter_mode = mode_label_to_key[selected_label]

    st.sidebar.header("Layout")
    layouts = ["concentric", "cose", "breadthfirst", "circle", "grid"]
    pending_layout = st.sidebar.selectbox(
        "Graph layout (applied on Redraw)",
        layouts,
        index=layouts.index(st.session_state.get("applied_layout", "concentric")),
        key="layout_selectbox"
    )

    if st.sidebar.button("🔄 Redraw Graph", use_container_width=True):
        st.session_state["applied_filter_mode"] = pending_filter_mode
        st.session_state["applied_layout"] = pending_layout
        st.cache_data.clear()
        st.rerun()

    st.sidebar.header("Legends")
    st.sidebar.markdown("ICON: 👤 Jobs, 👥 Pipelines")
    st.sidebar.markdown("COLOR: 🔴 Unhealthy, 🟢 Healthy")

    # focus query param
    focus_val = None
    try:
        focus_val = st.query_params.get("focus", None)
    except Exception:
        try:
            focus_val = st.experimental_get_query_params().get("focus", None)
        except Exception:
            focus_val = None
    if focus_val:
        st.session_state["highlight_node_id"] = focus_val[0] if isinstance(focus_val, list) else str(focus_val)

    applied_filter_mode = st.session_state["applied_filter_mode"]
    applied_layout = st.session_state.get("applied_layout", "concentric")

    with st.spinner("Loading DAG data..."):
        try:
            df_raw = load_dag_data()
            if df_raw is None or len(df_raw) == 0:
                st.warning("No data available")
                return

            df_filtered = apply_filter_by_mode(df_raw, applied_filter_mode)
            df = deduplicate_nodes(df_filtered)

            nodes_df = df[df["result_type"] == "NODES"].copy()
            edges_df = df[df["result_type"] == "EDGES"].copy()

            # Metrics / Most connected
            m = compute_metrics(nodes_df, edges_df)
            top_node_id, top_node_degree, top_node_name = most_connected_node(nodes_df, edges_df)

            c1, c2, c3, c4, c5 = st.columns(5)
            with c1: st.metric("🔴 Failed", m["failed_entities"])
            with c2: st.metric("🟢 Healthy", m["healthy_entities"])
            with c3: st.metric("E (undirected)", m["edges_undirected"])
            with c4: st.metric("Avg Degree (all)", f"{m['avg_degree_all']:.2f}")
            with c5:
                st.markdown("Most Connected Node")
                if top_node_id:
                    if st.button(f"📍 {top_node_name} ({top_node_degree})"):
                        st.session_state["highlight_node_id"] = top_node_id
                else:
                    st.markdown("N/A")

            elements = transform_to_st_link_analysis_format(df)

            node_styles = create_node_styles()
            edge_styles = create_edge_styles()
            layout_config = {"name": applied_layout, "animate": True, "fit": True, "padding": 80}

            st_link_analysis(
                elements,
                layout=layout_config,
                node_styles=node_styles,
                edge_styles=edge_styles,
                key="dag_graph",
                height=800
            )

        except Exception as e:
            st.error(f"Error loading data: {str(e)}")

if __name__ == "__main__":
    main()
