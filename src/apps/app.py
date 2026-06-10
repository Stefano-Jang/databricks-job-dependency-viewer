import os

import pandas as pd
import streamlit as st
from st_link_analysis import st_link_analysis, NodeStyle, EdgeStyle, Event

from conn import current_user_key, load_graph
from graph_utils import (
    build_adjacency,
    classify_incidents,
    critical_tables,
    downstream_hops,
    owners_of,
    top_nodes_by_degree,
    upstream_neighbors,
)

st.set_page_config(page_title="JIIG", layout="wide", page_icon="🔗")

ROLE_COLORS = {
    "ROOT": "#DC3545",      # selected failed entity
    "FAILED": "#B02A37",    # other failed entities
    "AFFECTED": "#FD7E14",  # downstream of a failure
    "CONTEXT": "#ADB5BD",   # upstream context (not impacted)
    "HEALTHY": "#28A745",
}
# icons must exist in st-link-analysis' bundled icon set
TYPE_ICONS = {"JOB": "analytics", "PIPELINE": "factory"}
LAYOUTS = ["breadthfirst", "concentric", "cose", "circle", "grid"]
# Must match the failure_lookback_days bundle variable (set via app env)
LOOKBACK_DAYS = max(1, int(os.getenv("FAILURE_LOOKBACK_DAYS", "7")))


def create_node_styles():
    styles = []
    for etype, icon in TYPE_ICONS.items():
        for role, color in ROLE_COLORS.items():
            styles.append(NodeStyle(f"{etype}_{role}", color, "name", icon))
    return styles


def create_edge_styles():
    return [
        EdgeStyle("DEPENDENCY", caption="caption_text", directed=True),
        EdgeStyle("TRIGGER", caption="caption_text", directed=True),
    ]


def recompute_failed_window(nodes_df: pd.DataFrame, window_hours: float) -> pd.DataFrame:
    """A node counts as failed only if its last failure is inside the window."""
    out = nodes_df.copy()
    now_utc = pd.Timestamp.now(tz="UTC")
    since = now_utc - pd.Timedelta(hours=float(window_hours))
    lft = out["last_failed_time"]
    out["is_failed"] = lft.notna() & (lft >= since) & (lft <= now_utc)
    out["status"] = out["is_failed"].map({True: "FAILED", False: "HEALTHY"})
    return out


def _s(value) -> str:
    """NaN/None-safe string."""
    return "" if value is None or pd.isna(value) else str(value)


def edge_caption(row) -> str:
    if str(row.type) == "trigger":
        return "triggers"
    tables = _s(row.connecting_tables)
    first = tables.split(", ")[0] if tables else ""
    count = int(row.edge_table_count) if pd.notna(row.edge_table_count) else 0
    return f"{first} (+{count - 1})" if count > 1 else first


def build_elements(nodes_df, edges_df, roles: dict):
    """Build st-link-analysis elements; roles maps node_id -> ROLE."""
    nodes = []
    for row in nodes_df.itertuples(index=False):
        nid = str(row.id)
        etype = str(row.type).upper()
        role = roles.get(nid, "HEALTHY")
        nodes.append({"data": {
            "id": nid,
            "label": f"{etype}_{role}",
            "name": _s(row.name),
            "entity_type": etype,
            "role": role,
            "status": _s(row.status),
            "failure_count": int(row.failure_count),
            "failure_detail": _s(row.failure_detail),
            "last_failed_time": _s(row.last_failed_time),
            "last_activity_time": _s(row.last_activity_time),
            "creator": _s(row.creator_email),
            "run_as": _s(row.run_as_email),
        }})
    edges = []
    for row in edges_df.itertuples(index=False):
        edges.append({"data": {
            "id": str(row.id),
            "label": str(row.type).upper(),
            "source": str(row.source_id),
            "target": str(row.target_id),
            "caption_text": edge_caption(row),
            "connecting_tables": _s(row.connecting_tables),
        }})
    return {"nodes": nodes, "edges": edges}


def render_graph(nodes_df, edges_df, roles, layout, key, height=650):
    elements = build_elements(nodes_df, edges_df, roles)
    ret = st_link_analysis(
        elements,
        layout={"name": layout, "animate": True, "fit": True, "padding": 60},
        node_styles=create_node_styles(),
        edge_styles=create_edge_styles(),
        events=[Event("clicked_node", "click tap", "node")],
        key=key,
        height=height,
    )
    if isinstance(ret, dict) and ret.get("action") == "clicked_node":
        tid = (ret.get("data") or {}).get("target_id")
        if isinstance(tid, (str, int)):
            return str(tid)
    return None


def node_detail_panel(nodes_df: pd.DataFrame, node_id: str):
    sel = nodes_df[nodes_df["id"] == str(node_id)].head(1)
    if sel.empty:
        return
    row = sel.iloc[0]
    ntype = str(row["type"]).lower()
    host = os.getenv("DATABRICKS_HOST", "").strip().rstrip("/")
    if host and not host.startswith("http"):
        host = f"https://{host}"
    url = f"{host}/{'jobs' if ntype == 'job' else 'pipelines'}/{row['id']}" if host else ""
    with st.container(border=True):
        c1, c2 = st.columns([4, 1])
        with c1:
            st.markdown(f"**{_s(row['name'])}**  ·  {ntype}  ·  `{row['id']}`")
            st.caption(
                f"owner: {_s(row['run_as_email']) or _s(row['creator_email']) or '-'}  |  "
                f"failures: {row['failure_count']}  |  "
                f"last failed: {_s(row['last_failed_time']) or '-'}  |  "
                f"detail: {_s(row['failure_detail']) or '-'}"
            )
        with c2:
            if url:
                st.link_button("Open in Databricks", url, use_container_width=True)


def notification_text(root_row, affected_df: pd.DataFrame, owners: list, depth: int) -> str:
    lines = [
        f"[JIIG] Incident: {root_row['name']} ({root_row['type']}) failed",
        f"Last failed (UTC): {root_row['last_failed_time']}",
        f"Failure detail: {_s(root_row['failure_detail']) or '-'}",
        f"Downstream impact within {depth} hops: "
        f"{len(affected_df)} entities, {len(owners)} owners",
        "",
        "Affected entities:",
    ]
    for row in affected_df.itertuples(index=False):
        owner = _s(row.run_as_email) or _s(row.creator_email) or "-"
        lines.append(f"  - [hop {row.hop}] {row.name} ({row.type}) — {owner}")
    lines += ["", "Owners to notify: " + ", ".join(owners)]
    return "\n".join(lines)


def incident_view(nodes_df, edges_df, forward, reverse, layout, depth, window_hours):
    failed_nodes = nodes_df[nodes_df["is_failed"]].copy()
    failed_ids = failed_nodes["id"].tolist()

    failed_times = dict(zip(failed_nodes["id"], failed_nodes["last_failed_time"]))
    incidents = classify_incidents(failed_ids, forward, depth, failed_times)
    all_affected = set().union(*(set(i["affected"]) for i in incidents.values())) if incidents else set()
    all_owners = owners_of(nodes_df, all_affected)

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("🔴 Failed entities", len(failed_ids))
    c2.metric("🟠 Affected (transitive)", len(all_affected - set(failed_ids)))
    c3.metric("👥 Owners to notify", len(all_owners))
    c4.metric("🔗 Edges", len(edges_df))
    freshness = nodes_df["last_activity_time"].max()
    c5.metric("🕒 Data up to (UTC)", "-" if pd.isna(freshness) else freshness.strftime("%m-%d %H:%M"))

    if not failed_ids:
        st.success("No Job/Pipeline failures within the selected window. 🎉")
        return

    # ---------- incident summary table ----------
    rows = []
    for fid in failed_ids:
        info = incidents[fid]
        node = nodes_df[nodes_df["id"] == fid].iloc[0]
        owners = owners_of(nodes_df, set(info["affected"]))
        rows.append({
            "name": node["name"],
            "type": node["type"],
            "classification": "CASCADE" if info["is_cascade"] else "ROOT CAUSE",
            "last_failed (UTC)": node["last_failed_time"],
            f"failures ({LOOKBACK_DAYS}d)": node["failure_count"],
            "blast_radius": len(info["affected"]),
            "owners_affected": len(owners),
            "failure_detail": node["failure_detail"],
            "id": fid,
        })
    inc_df = (
        pd.DataFrame(rows)
        .sort_values(["blast_radius", "last_failed (UTC)"], ascending=[False, False])
        .reset_index(drop=True)
    )

    st.markdown("#### 🚨 Incidents (select a row to analyze impact)")
    st.caption(
        "ROOT CAUSE: failure with no failed upstream — start here. "
        "CASCADE: sits downstream of another failure, likely a consequence."
    )
    selection = st.dataframe(
        inc_df,
        hide_index=True,
        use_container_width=True,
        on_select="rerun",
        selection_mode="single-row",
        key=f"incident_table_{window_hours}_{depth}",
        column_config={"id": None},
    )

    sel_rows = []
    if selection is not None:
        try:
            sel_rows = list(selection.selection.rows)
        except (AttributeError, TypeError):
            sel_rows = list(selection.get("selection", {}).get("rows", []))
    sel_rows = [r for r in sel_rows if r < len(inc_df)]
    if not sel_rows:
        st.info("Select an incident above to see its impact subgraph and insights.")
        return

    root_id = str(inc_df.iloc[sel_rows[0]]["id"])
    root_row = nodes_df[nodes_df["id"] == root_id].iloc[0]
    info = incidents[root_id]
    affected = info["affected"]

    # ---------- incident detail ----------
    st.markdown(f"### 🎯 Impact of `{root_row['name']}`")

    sub_ids = {root_id} | set(affected)
    context_ids = upstream_neighbors(reverse, {root_id})
    show_ids = sub_ids | context_ids

    sub_nodes = nodes_df[nodes_df["id"].isin(show_ids)].copy()
    sub_edges = edges_df[
        edges_df["source_id"].isin(show_ids) & edges_df["target_id"].isin(show_ids)
    ].copy()
    # keep the picture focused: drop context-to-context edges
    sub_edges = sub_edges[
        sub_edges["source_id"].isin(sub_ids) | sub_edges["target_id"].isin(sub_ids)
    ]

    roles = {nid: "AFFECTED" for nid in affected}
    for nid in sub_nodes[sub_nodes["is_failed"]]["id"]:
        roles[nid] = "FAILED"
    for nid in context_ids:
        roles.setdefault(nid, "CONTEXT")
    roles[root_id] = "ROOT"

    graph_col, insight_col = st.columns([3, 2])

    with graph_col:
        clicked = render_graph(sub_nodes, sub_edges, roles, layout, key=f"impact_{root_id}")
        node_detail_panel(nodes_df, clicked or root_id)

    with insight_col:
        affected_df = (
            nodes_df[nodes_df["id"].isin(affected)]
            .assign(hop=lambda d: d["id"].map(affected))
            .sort_values(["hop", "name"])
        )
        owners = owners_of(nodes_df, set(affected))

        st.markdown("**Affected entities by hop**")
        if affected_df.empty:
            st.write("No downstream consumers found in the lineage window.")
        else:
            st.dataframe(
                affected_df[["hop", "name", "type", "run_as_email", "is_failed"]],
                hide_index=True, use_container_width=True, height=240,
            )

        tables = critical_tables(forward, root_id, affected)
        if tables:
            st.markdown("**Critical tables (feeding most consumers)**")
            st.dataframe(
                pd.DataFrame(tables, columns=["table", "consumers"]),
                hide_index=True, use_container_width=True, height=180,
            )

        if owners:
            st.markdown("**Owners to notify**")
            st.code(", ".join(owners), language=None)

        with st.expander("📋 Notification draft", expanded=False):
            st.code(notification_text(root_row, affected_df, owners, depth), language=None)


def full_graph_view(nodes_df, edges_df, forward, layout, depth):
    st.caption(
        "Overview of every active entity in the window. The view is capped for "
        "responsiveness — failed nodes are always kept, the rest ranked by degree."
    )
    cap = st.slider("Max nodes to render", 50, 1000, 300, 50)

    failed_ids = nodes_df[nodes_df["is_failed"]]["id"].tolist()
    affected = set()
    for fid in failed_ids:
        affected |= set(downstream_hops(forward, fid, depth))

    keep = top_nodes_by_degree(nodes_df, edges_df, cap)
    keep |= set(failed_ids)
    view_nodes = nodes_df[nodes_df["id"].isin(keep)]
    view_edges = edges_df[
        edges_df["source_id"].isin(keep) & edges_df["target_id"].isin(keep)
    ]
    if len(nodes_df) > len(view_nodes):
        st.warning(
            f"Showing {len(view_nodes)} of {len(nodes_df)} nodes "
            f"({len(view_edges)} edges). Raise the cap to see more."
        )

    roles = {nid: "AFFECTED" for nid in affected}
    for nid in failed_ids:
        roles[nid] = "FAILED"

    clicked = render_graph(view_nodes, view_edges, roles, layout, key="full_graph", height=700)
    if clicked:
        node_detail_panel(nodes_df, clicked)


def main():
    st.title("🔗 JIIG — Job Incidents Identification Graph")

    st.sidebar.header("🔧 Analysis settings")
    max_hours = LOOKBACK_DAYS * 24
    window_hours = st.sidebar.slider(
        "Failure window (hours)", 1, max_hours, min(24, max_hours),
        help="Only failures within this window are treated as incidents.",
    )
    depth = st.sidebar.slider(
        "Impact depth (hops)", 1, 5, 3,
        help="How many dependency hops to follow downstream of a failure.",
    )
    layout = st.sidebar.selectbox("Graph layout", LAYOUTS, index=0)
    if st.sidebar.button("🔄 Refresh data", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    st.sidebar.header("Legend")
    st.sidebar.markdown(
        "- 🔴 **ROOT**: selected failure\n"
        "- 🟥 **FAILED**: other failures\n"
        "- 🟠 **AFFECTED**: downstream impact\n"
        "- ⚪ **CONTEXT**: direct upstream\n"
        "- 🟢 **HEALTHY**\n\n"
        "Icons: 📊 Lakeflow Job · 🏭 Pipeline (SDP)\n\n"
        "Edges: table dependency / job→pipeline trigger"
    )

    try:
        with st.spinner("Loading dependency graph..."):
            nodes_df, edges_df = load_graph(current_user_key())
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return

    if nodes_df.empty:
        st.warning("No data available — run the JIIG job first.")
        return

    nodes_df = recompute_failed_window(nodes_df, window_hours)
    forward, reverse = build_adjacency(edges_df)

    tab_incidents, tab_full = st.tabs(["🚨 Incidents", "🌐 Full graph"])
    with tab_incidents:
        incident_view(nodes_df, edges_df, forward, reverse, layout, depth, window_hours)
    with tab_full:
        full_graph_view(nodes_df, edges_df, forward, layout, depth)


if __name__ == "__main__":
    main()
