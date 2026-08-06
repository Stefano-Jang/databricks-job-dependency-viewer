import os

import pandas as pd
import streamlit as st
from st_link_analysis import st_link_analysis, NodeStyle, EdgeStyle, Event

from conn import (
    current_user_key, load_failed_edges, load_graph, load_incidents,
    load_overview, load_shared_tables, load_stats, load_subgraph,
)
from graph_utils import (
    build_adjacency,
    classify_incidents,
    critical_tables,
    downstream_hops,
    owners_of,
    owners_by_kind,
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
# JOB/PIPELINE = producers; dashboard/genie/query/alert = read-only leaf consumers
TYPE_ICONS = {
    "JOB": "analytics",
    "PIPELINE": "factory",
    "DASHBOARD": "dashboard",
    "GENIE": "psychology",
    "QUERY": "code",
    "ALERT": "notification_important",
}
# dagre/fcose/cola ship inside st-link-analysis but were never exposed before 2.0.
# dagre is first (and the default) because a dependency DAG should read as layers,
# not as a hairball.
LAYOUTS = ["dagre", "breadthfirst", "concentric", "cose", "circle", "grid", "fcose", "cola"]
# The SQL merges parallel edge kinds into one row, so an edge label can be a
# combination ("dependency+trigger"). Every value needs a style or the edge
# renders unstyled.
EDGE_KINDS = ["DEPENDENCY", "TRIGGER", "TABLE_TRIGGER"]
EDGE_LABELS = [
    "DEPENDENCY", "TRIGGER", "TABLE_TRIGGER",
    "DEPENDENCY+TRIGGER", "DEPENDENCY+TABLE_TRIGGER", "TRIGGER+TABLE_TRIGGER",
    "DEPENDENCY+TABLE_TRIGGER+TRIGGER", "DEPENDENCY+TRIGGER+TABLE_TRIGGER",
]
# Must match the failure_lookback_days bundle variable (set via app env)
LOOKBACK_DAYS = max(1, int(os.getenv("FAILURE_LOOKBACK_DAYS", "7")))


def create_node_styles():
    styles = []
    for etype, icon in TYPE_ICONS.items():
        for role, color in ROLE_COLORS.items():
            styles.append(NodeStyle(f"{etype}_{role}", color, "name", icon))
    return styles


def create_edge_styles():
    return [EdgeStyle(label, caption="caption_text", directed=True) for label in EDGE_LABELS]


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
    # dagre supports rankDir for top-down layout (dependency DAG best practice)
    layout_cfg = {"name": layout, "animate": True, "fit": True, "padding": 60}
    if layout == "dagre":
        layout_cfg["rankDir"] = "TB"  # Top-to-bottom: failures at top, impacts below
    ret = st_link_analysis(
        elements,
        layout=layout_cfg,
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


# What a stale upstream table actually means to each kind of consumer. A
# dashboard owner does not need to hear about a job run; they need to know the
# numbers on screen may be wrong.
IMPACT_WORDING = {
    "job": "input tables may be stale — re-run after the upstream fix",
    "pipeline": "input tables may be stale — refresh after the upstream fix",
    "dashboard": "may be showing stale data right now",
    "genie": "may answer questions from stale data",
    "query": "returns stale results until the upstream fix lands",
    "alert": "may fire or stay silent on stale data",
}


def notification_text(root_row, affected_df: pd.DataFrame, owners: list,
                      by_kind: dict, depth: int) -> str:
    lines = [
        f"[JIIG] Incident: {_s(root_row['name'])} ({_s(root_row['type'])}) failed",
        f"Last failed (UTC): {_s(root_row['last_failed_time'])}",
        f"Failure detail: {_s(root_row['failure_detail']) or '-'}",
        f"Downstream impact within {depth} hops: "
        f"{len(affected_df)} entities, {len(owners)} owners",
    ]
    for kind in sorted(by_kind):
        lines += ["", f"{kind.upper()} — {IMPACT_WORDING.get(kind, 'may be affected')}"]
        sub = affected_df[affected_df["type"] == kind] if not affected_df.empty else affected_df
        for row in sub.itertuples(index=False):
            owner = _s(row.run_as_email) or _s(row.creator_email) or "-"
            lines.append(f"  - [hop {row.hop}] {_s(row.name)} — {owner}")
        lines.append(f"  notify: {', '.join(by_kind[kind])}")
    return "\n".join(lines)


def incident_view(user_key, incidents_df, stats, layout, depth, window_hours):
    """Incident triage. Reads only failed entities, then one subgraph on demand.

    Nothing here loads the whole graph: the incident list is server-side
    filtered, and the impact subgraph for the selected incident is fetched by
    a scoped query. That is what keeps the app usable on a workspace with
    thousands of entities.
    """
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("🔴 Failed entities", len(incidents_df))
    c2.metric("🧩 Entities tracked", stats.get("total_nodes", "-"))
    c3.metric("🔗 Dependency edges", stats.get("total_edges", "-"))
    freshness = stats.get("freshness")
    c4.metric("🕒 Data up to (UTC)", "-" if freshness is None or pd.isna(freshness)
              else pd.Timestamp(freshness).strftime("%m-%d %H:%M"))

    if incidents_df.empty:
        st.success("No Job/Pipeline failures within the selected window. 🎉")
        return

    # Cascade classification needs the edges among failed entities only, which
    # is a small graph even when the workspace is large.
    failed_ids = incidents_df["id"].astype(str).tolist()
    failed_times = dict(zip(incidents_df["id"].astype(str), incidents_df["last_failed_time"]))
    failed_edges = load_failed_edges(user_key, failed_ids)
    forward_failed, _ = build_adjacency(failed_edges)
    incidents = classify_incidents(failed_ids, forward_failed, depth, failed_times)

    inc_df = incidents_df.assign(
        classification=[
            "CASCADE" if incidents[str(i)]["is_cascade"] else "ROOT CAUSE"
            for i in incidents_df["id"].astype(str)
        ],
    )
    show = pd.DataFrame({
        "name": inc_df["name"],
        "type": inc_df["type"],
        "classification": inc_df["classification"],
        "last_failed (UTC)": inc_df["last_failed_time"],
        f"failures (last {LOOKBACK_DAYS}d)": inc_df["failure_count"],
        "blast radius (all hops)": inc_df["downstream_reach"],
        "failure_detail": inc_df["failure_detail"],
        "id": inc_df["id"],
    }).sort_values(["blast radius (all hops)", "last_failed (UTC)"],
                   ascending=[False, False]).reset_index(drop=True)

    st.markdown("#### 🚨 Incidents (select a row to analyze impact)")
    st.caption(
        "ROOT CAUSE: no failed upstream — start here. "
        "CASCADE: sits downstream of another failure, likely a consequence. "
        f"Failure counts cover the full {LOOKBACK_DAYS}-day job window; the list itself "
        "is filtered to the failure window selected in the sidebar."
    )
    selection = st.dataframe(
        show, hide_index=True, use_container_width=True,
        on_select="rerun", selection_mode="single-row",
        key=f"incident_table_{window_hours}_{depth}",
        column_config={"id": None},
    )

    sel_rows = []
    if selection is not None:
        try:
            sel_rows = list(selection.selection.rows)
        except (AttributeError, TypeError):
            sel_rows = list(selection.get("selection", {}).get("rows", []))
    sel_rows = [r for r in sel_rows if r < len(show)]
    if not sel_rows:
        st.info("Select an incident above to see its impact subgraph and insights.")
        return

    root_id = str(show.iloc[sel_rows[0]]["id"])

    # ---------- incident detail: one scoped query ----------
    with st.spinner("Loading impact subgraph..."):
        sub_nodes, sub_edges = load_subgraph(user_key, root_id, depth)
    if sub_nodes.empty:
        st.warning("Could not load the subgraph for this incident.")
        return

    root_match = sub_nodes[sub_nodes["id"] == root_id]
    if root_match.empty:
        st.warning("Selected incident is missing from the subgraph.")
        return
    root_row = root_match.iloc[0]
    st.markdown(f"### 🎯 Impact of `{_s(root_row['name'])}`")

    forward, reverse = build_adjacency(sub_edges)
    affected = downstream_hops(forward, root_id, depth)
    context_ids = upstream_neighbors(reverse, {root_id})

    roles = {nid: "AFFECTED" for nid in affected}
    for nid in sub_nodes[sub_nodes["is_failed"] == True]["id"]:
        roles[nid] = "FAILED"
    for nid in context_ids:
        roles.setdefault(nid, "CONTEXT")
    roles[root_id] = "ROOT"

    graph_col, insight_col = st.columns([3, 2])

    with graph_col:
        clicked = render_graph(sub_nodes, sub_edges, roles, layout, key=f"impact_{root_id}")
        node_detail_panel(sub_nodes, clicked or root_id)

    with insight_col:
        affected_df = (
            sub_nodes[sub_nodes["id"].isin(affected)]
            .assign(hop=lambda d: d["id"].map(affected))
            .sort_values(["hop", "name"])
        )
        owners = owners_of(sub_nodes, set(affected))
        by_kind = owners_by_kind(sub_nodes, set(affected))

        st.markdown("**Affected entities by hop**")
        if affected_df.empty:
            st.write("No downstream consumers found in the lineage window.")
        else:
            cols = [c for c in ["hop", "name", "type", "subtype", "run_as_email", "is_failed"]
                    if c in affected_df.columns]
            st.dataframe(affected_df[cols], hide_index=True,
                         use_container_width=True, height=240)

        tables = critical_tables(forward, root_id, affected)
        if tables:
            st.markdown("**Critical tables (feeding most consumers)**")
            st.dataframe(
                pd.DataFrame(tables, columns=["table", "consumers"]),
                hide_index=True, use_container_width=True, height=180,
            )

        if by_kind:
            st.markdown("**Owners to notify**")
            for kind, emails in sorted(by_kind.items()):
                st.caption(f"{kind} ({len(emails)})")
                st.code(", ".join(emails), language=None)

        with st.expander("📋 Notification draft", expanded=False):
            st.code(notification_text(root_row, affected_df, owners, by_kind, depth), language=None)


def insights_view(user_key: str, stats: dict):
    """Workspace insight: what matters here, and where the risk sits.

    Everything on this tab comes from server-side aggregates or a top-N query,
    never from loading the graph.
    """
    cols = st.columns(4)
    cols[0].metric("🧩 Entities tracked", stats.get("total_nodes", "-"))
    cols[1].metric("🔗 Dependency edges", stats.get("total_edges", "-"))
    cols[2].metric("🔴 Failed (job window)", stats.get("failed_nodes", "-"))
    cols[3].metric("📌 Shared tables", stats.get("shared_tables", "-"))

    st.markdown("#### 🎯 Most critical entities")
    st.caption(
        "Ranked by blast radius — how many entities sit downstream. These are the "
        "jobs and pipelines whose failure hurts most, whether or not they are failing today."
    )
    try:
        top_critical = load_overview(user_key, 20)
    except Exception as e:
        st.error(f"Could not load overview: {e}")
        top_critical = pd.DataFrame()
    if top_critical.empty:
        st.info("No entities found.")
    else:
        display_cols = ["criticality_rank", "name", "type", "subtype",
                        "downstream_reach", "out_degree", "in_degree", "is_failed"]
        st.dataframe(
            top_critical[[c for c in display_cols if c in top_critical.columns]],
            hide_index=True, use_container_width=True, height=320,
        )

    left, right = st.columns(2)

    with left:
        st.markdown("#### 📊 Entities by type")
        by_type = stats.get("by_type")
        if by_type is not None and not by_type.empty:
            st.dataframe(by_type, hide_index=True, use_container_width=True, height=260)
        else:
            st.write("No breakdown available.")

    with right:
        st.markdown("#### 📌 Shared tables (excluded from edges)")
        st.caption(
            "Written by more producers than `max_table_fanout` allows, so they are not "
            "treated as dependencies. A table with hundreds of writers is usually a "
            "platform sink or an overloaded contract — worth knowing either way."
        )
        try:
            shared_df = load_shared_tables(user_key)
        except Exception:
            shared_df = pd.DataFrame()
        if not shared_df.empty:
            cols_present = [c for c in ["name", "writer_count"] if c in shared_df.columns]
            st.dataframe(shared_df.head(25)[cols_present], hide_index=True,
                         use_container_width=True, height=260)
        else:
            st.write("No shared tables above the fan-out cap.")


def full_graph_view(user_key, layout, depth, window_hours):
    """Workspace overview. Loaded on demand only.

    This is the one view that needs the whole graph, and on a large workspace
    that is thousands of nodes and edges. It stays behind an explicit button so
    opening JIIG to triage an incident never pays for it.
    """
    st.caption(
        "Overview of the highest-criticality entities in the workspace. Capped for "
        "responsiveness: failed nodes are always kept, the rest by criticality rank."
    )
    cap = st.slider("Max nodes to render", 50, 1000, 300, 50)
    if not st.session_state.get("full_graph_loaded"):
        if not st.button("Load workspace graph", type="primary"):
            st.info("The full graph is loaded on demand — it is the only view that reads the whole table.")
            return
        st.session_state["full_graph_loaded"] = True

    with st.spinner("Loading workspace graph..."):
        nodes_df, edges_df = load_graph(user_key)
    if nodes_df.empty:
        st.warning("No data available — run the JIIG job first.")
        return

    nodes_df = recompute_failed_window(nodes_df, window_hours)
    forward, _ = build_adjacency(edges_df)

    failed_ids = nodes_df[nodes_df["is_failed"]]["id"].tolist()
    affected = set()
    for fid in failed_ids:
        affected |= set(downstream_hops(forward, fid, depth))

    # criticality_rank is precomputed in SQL; rank 1 = most critical.
    keep = set(failed_ids)
    if len(keep) < cap:
        keep |= set(nodes_df.nsmallest(cap - len(keep), "criticality_rank")["id"].tolist())

    view_nodes = nodes_df[nodes_df["id"].isin(keep)]
    view_edges = edges_df[
        edges_df["source_id"].isin(keep) & edges_df["target_id"].isin(keep)
    ]
    if len(nodes_df) > len(view_nodes):
        st.warning(
            f"Showing {len(view_nodes)} of {len(nodes_df)} nodes "
            f"({len(view_edges)} of {len(edges_df)} edges). Raise the cap to see more."
        )

    roles = {nid: "AFFECTED" for nid in affected}
    for nid in failed_ids:
        roles[nid] = "FAILED"

    clicked = render_graph(view_nodes, view_edges, roles, layout, key="full_graph", height=700)
    if clicked:
        node_detail_panel(nodes_df, clicked)


def main():
    st.title("🔗 JIIG — Job Incidents Identification Graph")

    user_key = current_user_key()
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
        "Icons: 📊 Job · 🏭 Pipeline · 📊 Dashboard · 🧠 Genie · 💻 Query · 🔔 Alert\n\n"
        "Edges: table dependency / job→pipeline trigger"
    )

    # Only the failed entities and a few aggregates are loaded up front. The
    # impact subgraph is fetched per incident, and the workspace graph only when
    # the user asks for it.
    try:
        with st.spinner("Loading incidents..."):
            incidents_df = load_incidents(user_key, window_hours)
            stats = load_stats(user_key)
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return

    if not stats.get("total_nodes"):
        st.warning("No data available — run the JIIG job first.")
        return

    tab_incidents, tab_insights, tab_full = st.tabs(["🚨 Incidents", "💡 Insights", "🌐 Workspace graph"])
    with tab_incidents:
        incident_view(user_key, incidents_df, stats, layout, depth, window_hours)
    with tab_insights:
        insights_view(user_key, stats)
    with tab_full:
        full_graph_view(user_key, layout, depth, window_hours)


if __name__ == "__main__":
    main()
