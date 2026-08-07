import html
import os
from itertools import combinations

import pandas as pd
import streamlit as st

try:
    from st_link_analysis import EdgeStyle, Event, NodeStyle, st_link_analysis

    LINK_ANALYSIS_AVAILABLE = True
except ImportError:
    LINK_ANALYSIS_AVAILABLE = False

from conn import (
    current_user_key,
    load_graph,
    load_incident_signals,
    load_incidents,
    load_leaders,
    load_overview,
    load_shared_tables,
    load_stats,
    load_subgraph,
)
from graph_utils import (
    build_adjacency,
    classify_incidents,
    critical_tables,
    downstream_hops,
    downstream_paths,
    owners_by_kind,
    owners_of,
    upstream_hops,
    upstream_neighbors,
)


st.set_page_config(
    page_title="JIIG — Incident Intelligence",
    page_icon=":material/account_tree:",
    layout="wide",
    initial_sidebar_state="expanded",
)

ROLE_COLORS = {
    "ROOT": "#D92D20",
    "FAILED": "#7A271A",
    "AFFECTED": "#EAAA08",
    "CONTEXT": "#98A2B3",
    "HEALTHY": "#2E6F95",
}
TYPE_ICONS = {
    "JOB": "analytics",
    "PIPELINE": "factory",
    "DASHBOARD": "dashboard",
    "GENIE": "psychology",
    "QUERY": "code",
    "ALERT": "notification_important",
}
LAYOUTS = ["dagre", "breadthfirst", "concentric", "cose", "fcose", "cola"]
EDGE_KINDS = ["DEPENDENCY", "TABLE_TRIGGER", "TRIGGER"]
EDGE_LABELS = [
    "+".join(combo)
    for size in range(1, len(EDGE_KINDS) + 1)
    for combo in combinations(sorted(EDGE_KINDS), size)
]
TERMINAL_TYPES = {"dashboard", "genie", "query", "alert"}
LOOKBACK_DAYS = max(1, int(os.getenv("FAILURE_LOOKBACK_DAYS", "7")))


def inject_styles():
    st.markdown(
        """
        <style>
        :root {
          --jiig-ink: #101828;
          --jiig-muted: #667085;
          --jiig-border: #DDE3EA;
          --jiig-surface: #FFFFFF;
          --jiig-canvas: #F4F6F8;
          --jiig-navy: #102A43;
          --jiig-blue: #246BFD;
          --jiig-red: #D92D20;
          --jiig-amber: #EAAA08;
          --jiig-green: #067647;
        }
        html, body, [class*="css"] {
          font-family: "Aptos", "Segoe UI Variable", "Helvetica Neue", sans-serif;
          color: var(--jiig-ink);
        }
        .stApp { background: var(--jiig-canvas); }
        .block-container { max-width: 1480px; padding-top: 1.4rem; padding-bottom: 4rem; }
        header[data-testid="stHeader"] { background: transparent; }
        [data-testid="stSidebar"] { background: #102A43; border-right: 0; }
        [data-testid="stSidebar"] * { color: #F2F4F7; }
        [data-testid="stSidebar"] [data-baseweb="select"] * { color: #101828; }
        [data-testid="stSidebar"] .stButton button,
        [data-testid="stSidebar"] .stButton button * { color: #102A43 !important; }
        [data-testid="stSidebar"] .stSlider [data-testid="stTickBarMin"],
        [data-testid="stSidebar"] .stSlider [data-testid="stTickBarMax"] { color: #CBD5E1; }
        [data-testid="stSidebar"] hr { border-color: rgba(255,255,255,.14); }
        div[data-testid="stVerticalBlockBorderWrapper"] {
          background: var(--jiig-surface);
          border-color: var(--jiig-border);
          border-radius: 12px;
          box-shadow: 0 1px 2px rgba(16, 24, 40, .04);
        }
        div[data-testid="stMetric"] {
          background: var(--jiig-surface);
          border: 1px solid var(--jiig-border);
          border-radius: 10px;
          padding: 16px 18px;
          min-height: 112px;
        }
        div[data-testid="stMetricLabel"] { color: var(--jiig-muted); }
        div[data-testid="stMetricValue"] { color: var(--jiig-ink); font-weight: 680; }
        .jiig-topbar {
          display: flex; align-items: center; justify-content: space-between;
          gap: 24px; margin-bottom: 16px;
        }
        .jiig-brand { display: flex; align-items: center; gap: 12px; }
        .jiig-mark {
          width: 38px; height: 38px; border-radius: 9px; background: var(--jiig-navy);
          color: white; display: grid; place-items: center; font-weight: 800; letter-spacing: -1px;
        }
        .jiig-brand-name { font-size: 21px; font-weight: 760; letter-spacing: -.35px; }
        .jiig-brand-sub { color: var(--jiig-muted); font-size: 13px; margin-top: 1px; }
        .jiig-freshness { color: var(--jiig-muted); font-size: 12px; text-align: right; }
        .jiig-live { display: inline-block; width: 7px; height: 7px; border-radius: 50%; background: #12B76A; margin-right: 6px; }
        .jiig-section { margin: 20px 0 10px; }
        .jiig-eyebrow { color: #246BFD; text-transform: uppercase; letter-spacing: .11em; font-size: 11px; font-weight: 760; }
        .jiig-section h2 { margin: 3px 0 3px; font-size: 24px; letter-spacing: -.35px; }
        .jiig-section p { margin: 0; color: var(--jiig-muted); font-size: 14px; }
        .jiig-incident-hero {
          background: #FFFFFF; border: 1px solid var(--jiig-border); border-radius: 12px;
          padding: 20px 22px; margin: 12px 0 16px;
        }
        .jiig-incident-row { display: flex; justify-content: space-between; align-items: flex-start; gap: 24px; }
        .jiig-incident-name { font-size: 25px; line-height: 1.25; font-weight: 730; letter-spacing: -.4px; margin: 7px 0 5px; }
        .jiig-incident-meta { color: var(--jiig-muted); font-size: 13px; }
        .jiig-severity { display: inline-flex; align-items: center; border-radius: 999px; padding: 5px 9px; font-size: 11px; font-weight: 780; letter-spacing: .06em; }
        .severity-critical { background: #FEE4E2; color: #B42318; }
        .severity-high { background: #FEF0C7; color: #B54708; }
        .severity-moderate { background: #EFF4FF; color: #3538CD; }
        .severity-isolated { background: #F2F4F7; color: #475467; }
        .jiig-tag { display: inline-flex; padding: 3px 7px; border-radius: 5px; background: #F2F4F7; color: #344054; font-size: 11px; font-weight: 680; margin-right: 5px; }
        .jiig-classification { color: #475467; font-size: 12px; font-weight: 700; text-transform: uppercase; letter-spacing: .07em; }
        .jiig-callout { border: 1px solid #FEC84B; background: #FFFAEB; color: #7A2E0E; border-radius: 9px; padding: 11px 13px; font-size: 13px; }
        .jiig-success { border: 1px solid #ABEFC6; background: #ECFDF3; color: #05603A; border-radius: 10px; padding: 18px; }
        .jiig-owner-chip { display: inline-block; border: 1px solid #D0D5DD; background: white; border-radius: 999px; padding: 5px 9px; margin: 3px 4px 3px 0; color: #344054; font-size: 12px; }
        .jiig-statline { display: flex; gap: 12px; flex-wrap: wrap; margin-top: 12px; }
        .jiig-statline > div { min-width: 120px; }
        .jiig-stat-value { font-size: 20px; font-weight: 740; }
        .jiig-stat-label { color: var(--jiig-muted); font-size: 11px; text-transform: uppercase; letter-spacing: .05em; }
        .jiig-legend { display: flex; flex-wrap: wrap; gap: 8px 12px; color: #475467; font-size: 12px; margin: 5px 0 10px; }
        .jiig-dot { display: inline-block; width: 9px; height: 9px; border-radius: 50%; margin-right: 5px; }
        .jiig-side-title { color: #344054; font-size: 13px; font-weight: 730; margin: 4px 0 9px; }
        .jiig-mix-row { margin-bottom: 10px; }
        .jiig-mix-head { display: flex; justify-content: space-between; color: #475467; font-size: 12px; margin-bottom: 4px; }
        .jiig-mix-track { height: 6px; background: #EAECF0; border-radius: 999px; overflow: hidden; }
        .jiig-mix-fill { height: 100%; background: #246BFD; }
        .stButton > button, .stDownloadButton > button { border-radius: 8px; font-weight: 680; }
        [data-testid="stDataFrame"] { border: 1px solid var(--jiig-border); border-radius: 9px; overflow: hidden; }
        div[role="radiogroup"] { gap: 8px; }
        div[role="radiogroup"] label { background: #FFFFFF; border: 1px solid var(--jiig-border); padding: 7px 12px; border-radius: 8px; }
        @media (max-width: 900px) {
          .jiig-topbar, .jiig-incident-row { flex-direction: column; }
          .jiig-freshness { text-align: left; }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _s(value) -> str:
    return "" if value is None or pd.isna(value) else str(value)


def _h(value) -> str:
    return html.escape(_s(value))


def format_timestamp(value) -> str:
    timestamp = pd.to_datetime(value, utc=True, errors="coerce")
    if pd.isna(timestamp):
        return "Not available"
    return timestamp.strftime("%Y-%m-%d %H:%M UTC")


def format_relative(value) -> str:
    timestamp = pd.to_datetime(value, utc=True, errors="coerce")
    if pd.isna(timestamp):
        return "unknown"
    seconds = max(0, int((pd.Timestamp.now(tz="UTC") - timestamp).total_seconds()))
    if seconds < 60:
        return "just now"
    if seconds < 3600:
        return f"{seconds // 60}m ago"
    if seconds < 86400:
        return f"{seconds // 3600}h ago"
    return f"{seconds // 86400}d ago"


def section_title(eyebrow: str, title: str, description: str):
    st.markdown(
        f"""
        <div class="jiig-section">
          <div class="jiig-eyebrow">{_h(eyebrow)}</div>
          <h2>{_h(title)}</h2>
          <p>{_h(description)}</p>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_topbar(freshness):
    fresh_text = format_timestamp(freshness)
    st.markdown(
        f"""
        <div class="jiig-topbar">
          <div class="jiig-brand">
            <div class="jiig-mark">JG</div>
            <div>
              <div class="jiig-brand-name">JIIG</div>
              <div class="jiig-brand-sub">Lakeflow incident intelligence</div>
            </div>
          </div>
          <div class="jiig-freshness"><span class="jiig-live"></span>Snapshot ready<br>{_h(fresh_text)}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def create_node_styles():
    if not LINK_ANALYSIS_AVAILABLE:
        return []
    return [
        NodeStyle(f"{entity_type}_{role}", color, "name", icon)
        for entity_type, icon in TYPE_ICONS.items()
        for role, color in ROLE_COLORS.items()
    ]


def create_edge_styles():
    if not LINK_ANALYSIS_AVAILABLE:
        return []
    return [EdgeStyle(label, caption="caption_text", directed=True) for label in EDGE_LABELS]


def recompute_failed_window(nodes_df: pd.DataFrame, window_hours: float) -> pd.DataFrame:
    out = nodes_df.copy()
    now_utc = pd.Timestamp.now(tz="UTC")
    since = now_utc - pd.Timedelta(hours=float(window_hours))
    failed_time = out["last_failed_time"]
    out["is_failed"] = failed_time.notna() & (failed_time >= since) & (failed_time <= now_utc)
    out["status"] = out["is_failed"].map({True: "FAILED", False: "HEALTHY"})
    return out


def edge_caption(row) -> str:
    if str(row.type).lower() == "trigger":
        return "triggers"
    tables = _s(row.connecting_tables)
    first = tables.split(", ")[0] if tables else "dependency"
    count = int(row.edge_table_count) if pd.notna(row.edge_table_count) else 0
    return f"{first} (+{count - 1})" if count > 1 else first


def build_elements(nodes_df, edges_df, roles: dict):
    nodes = []
    for row in nodes_df.itertuples(index=False):
        node_id = str(row.id)
        entity_type = str(row.type).upper()
        role = roles.get(node_id, "HEALTHY")
        nodes.append({"data": {
            "id": node_id,
            "label": f"{entity_type}_{role}",
            "name": _s(row.name),
            "entity_type": entity_type,
            "role": role,
            "status": _s(row.status),
            "failure_count": int(row.failure_count),
            "failure_detail": _s(row.failure_detail),
            "last_failed_time": _s(row.last_failed_time),
            "creator": _s(row.creator_email),
            "run_as": _s(row.run_as_email),
            "downstream_reach": int(row.downstream_reach),
            "in_degree": int(row.in_degree),
            "out_degree": int(row.out_degree),
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


def render_graph(nodes_df, edges_df, roles, layout, key, height=590):
    if not LINK_ANALYSIS_AVAILABLE:
        lines = ["digraph JIIG {", "rankdir=LR;", "graph [bgcolor=transparent, pad=0.2];", "node [shape=box, style=filled, fontname=Helvetica, fontsize=10, color=white];"]
        for row in nodes_df.itertuples(index=False):
            node_id = str(row.id).replace('"', "")
            label = _s(row.name).replace('"', "'")
            if len(label) > 28:
                label = label[:27] + "…"
            color = ROLE_COLORS.get(roles.get(str(row.id), "HEALTHY"), ROLE_COLORS["HEALTHY"])
            lines.append(f'"{node_id}" [label="{label}", fillcolor="{color}", fontcolor="white"];')
        for row in edges_df.itertuples(index=False):
            caption = edge_caption(row).replace('"', "'")
            lines.append(f'"{row.source_id}" -> "{row.target_id}" [label="{caption}", color="#98A2B3", fontcolor="#667085", fontsize=8];')
        lines.append("}")
        st.graphviz_chart("\n".join(lines), width="stretch")
        return None

    layout_config = {"name": layout, "animate": False, "fit": True, "padding": 50}
    if layout == "dagre":
        layout_config["rankDir"] = "LR"
    result = st_link_analysis(
        build_elements(nodes_df, edges_df, roles),
        layout=layout_config,
        node_styles=create_node_styles(),
        edge_styles=create_edge_styles(),
        events=[Event("clicked_node", "click tap", "node")],
        key=key,
        height=height,
    )
    if isinstance(result, dict) and result.get("action") == "clicked_node":
        target_id = (result.get("data") or {}).get("target_id")
        if isinstance(target_id, (str, int)):
            return str(target_id)
    return None


def render_graph_legend():
    items = [("ROOT", "Selected failure"), ("FAILED", "Other failure"), ("AFFECTED", "Downstream impact"), ("CONTEXT", "Upstream context")]
    content = "".join(
        f'<span><i class="jiig-dot" style="background:{ROLE_COLORS[role]}"></i>{label}</span>'
        for role, label in items
    )
    st.markdown(f'<div class="jiig-legend">{content}</div>', unsafe_allow_html=True)


def entity_url(row) -> str:
    host = os.getenv("DATABRICKS_HOST", "").strip().rstrip("/")
    if host and not host.startswith("http"):
        host = f"https://{host}"
    if not host:
        return ""
    entity_type = str(row["type"]).lower()
    entity_id = _s(row.get("entity_id"))
    if entity_type == "job":
        return f"{host}/jobs/{entity_id}"
    if entity_type == "pipeline":
        return f"{host}/pipelines/{entity_id}"
    return ""


def node_detail_panel(nodes_df: pd.DataFrame, node_id: str):
    selected = nodes_df[nodes_df["id"] == str(node_id)].head(1)
    if selected.empty:
        return
    row = selected.iloc[0]
    with st.container(border=True):
        left, right = st.columns([5, 1])
        with left:
            st.markdown(f"**{_s(row['name'])}**")
            st.caption(
                f"{str(row['type']).upper()} · owner {_s(row['run_as_email']) or _s(row['creator_email']) or 'unresolved'} · "
                f"{row['downstream_reach']} downstream at configured max depth · {row['in_degree']} direct upstream"
            )
            if _s(row["failure_detail"]):
                st.caption(f"Latest failure: {_s(row['failure_detail'])} · {format_timestamp(row['last_failed_time'])}")
        url = entity_url(row)
        with right:
            if url:
                st.link_button("Open asset", url, width="stretch")


def severity_for(reach: int, terminal_count: int):
    score = int(reach) + int(terminal_count) * 2
    if score >= 25:
        return "CRITICAL", "critical"
    if score >= 8:
        return "HIGH", "high"
    if score >= 1:
        return "MODERATE", "moderate"
    return "ISOLATED", "isolated"


def render_incident_hero(root_row, classification, reach, terminal_count, owner_count):
    severity, severity_class = severity_for(reach, terminal_count)
    metadata = (
        f"Last failed {format_relative(root_row['last_failed_time'])} · "
        f"{int(root_row['failure_count'])} failure{'s' if int(root_row['failure_count']) != 1 else ''} in {LOOKBACK_DAYS} days · "
        f"{_s(root_row['failure_detail']) or 'No failure code reported'}"
    )
    st.markdown(
        f"""
        <div class="jiig-incident-hero">
          <div class="jiig-incident-row">
            <div>
              <div><span class="jiig-severity severity-{severity_class}">{severity} EXPOSURE</span></div>
              <div class="jiig-incident-name">{_h(root_row['name'])}</div>
              <div class="jiig-incident-meta">{_h(metadata)}</div>
            </div>
            <div class="jiig-classification">{_h(classification)}</div>
          </div>
          <div class="jiig-statline">
            <div><div class="jiig-stat-value">{reach}</div><div class="jiig-stat-label">Assets at risk</div></div>
            <div><div class="jiig-stat-value">{terminal_count}</div><div class="jiig-stat-label">Business surfaces</div></div>
            <div><div class="jiig-stat-value">{owner_count}</div><div class="jiig-stat-label">Owners identified</div></div>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


IMPACT_WORDING = {
    "job": "input tables may be stale; rerun after the upstream fix",
    "pipeline": "input tables may be stale; refresh after the upstream fix",
    "dashboard": "may be showing stale data",
    "genie": "may answer from stale data",
    "query": "may return stale results",
    "alert": "may fire or stay silent on stale data",
}


def notification_text(root_row, affected_df: pd.DataFrame, depth: int) -> str:
    owners = owners_of(affected_df, set(affected_df["id"])) if not affected_df.empty else []
    lines = [
        f"[JIIG] {_s(root_row['name'])} is currently failing",
        f"Last failed: {format_timestamp(root_row['last_failed_time'])}",
        f"Failure detail: {_s(root_row['failure_detail']) or 'Not reported'}",
        f"Potential impact within {depth} hops: {len(affected_df)} assets, {len(owners)} identified owners",
    ]
    for kind in sorted(set(affected_df["type"])) if not affected_df.empty else []:
        subset = affected_df[affected_df["type"] == kind]
        lines.extend(["", f"{kind.upper()} — {IMPACT_WORDING.get(kind, 'may be affected')}"])
        for row in subset.itertuples(index=False):
            owner = _s(row.run_as_email) or _s(row.creator_email) or "OWNER UNRESOLVED"
            lines.append(f"- [hop {row.hop}] {_s(row.name)} — {owner}")
    unresolved = affected_df[
        affected_df["run_as_email"].isna() & affected_df["creator_email"].isna()
    ] if not affected_df.empty else affected_df
    if not unresolved.empty:
        lines.extend(["", f"Owner resolution required for {len(unresolved)} affected asset(s)."])
    return "\n".join(lines)


def path_text(path_info, name_map):
    nodes = path_info["node_path"]
    tables = path_info["edge_tables"]
    parts = [name_map.get(nodes[0], nodes[0])]
    for index, node_id in enumerate(nodes[1:]):
        evidence = tables[index]
        parts.append(evidence[0] if evidence else "orchestration trigger")
        parts.append(name_map.get(node_id, node_id))
    return " → ".join(parts)


def render_asset_mix(affected_df):
    counts = affected_df["type"].value_counts() if not affected_df.empty else pd.Series(dtype=int)
    total = max(1, int(counts.sum()))
    rows = []
    for entity_type, count in counts.items():
        width = max(5, int(count / total * 100))
        rows.append(
            f'<div class="jiig-mix-row"><div class="jiig-mix-head"><span>{_h(entity_type.title())}</span><strong>{int(count)}</strong></div>'
            f'<div class="jiig-mix-track"><div class="jiig-mix-fill" style="width:{width}%"></div></div></div>'
        )
    st.markdown('<div class="jiig-side-title">Impact composition</div>' + "".join(rows), unsafe_allow_html=True)


def incident_view(user_key, incidents_df, stats, layout, depth, window_hours):
    section_title(
        "Incident command",
        "See the blast radius before users feel it",
        "Open failures are prioritized by downstream exposure. The highest-impact incident is selected automatically.",
    )
    if incidents_df.empty:
        st.markdown(
            '<div class="jiig-success"><strong>No open Job or Pipeline incidents.</strong><br>The latest runs in the selected window are healthy.</div>',
            unsafe_allow_html=True,
        )
        return

    failed_ids = tuple(incidents_df["id"].astype(str))
    failed_times = dict(zip(incidents_df["id"].astype(str), incidents_df["last_failed_time"]))
    reach_by_id, failed_pairs = load_incident_signals(user_key, failed_ids, depth)
    failed_forward, _ = build_adjacency(failed_pairs)
    classifications = classify_incidents(list(failed_ids), failed_forward, depth, failed_times)

    ranked = incidents_df.copy()
    ranked["reach_in_scope"] = ranked["id"].map(reach_by_id).fillna(0).astype(int)
    ranked["classification"] = ranked["id"].map(
        lambda node_id: "LIKELY CASCADE" if classifications[str(node_id)]["is_cascade"] else "LIKELY ROOT"
    )
    ranked = ranked.sort_values(
        ["reach_in_scope", "last_failed_time"], ascending=[False, False]
    ).reset_index(drop=True)

    likely_roots = int((ranked["classification"] == "LIKELY ROOT").sum())
    owner_gap_count = int((ranked["run_as_email"].isna() & ranked["creator_email"].isna()).sum())
    metrics = st.columns(4)
    metrics[0].metric("Open incidents", len(ranked), help="Latest run or update is still failed")
    metrics[1].metric("Likely root incidents", likely_roots, help="No earlier failed upstream was found within scope")
    metrics[2].metric(f"Largest reach · {depth} hops", int(ranked["reach_in_scope"].max()))
    metrics[3].metric("Incident owner gaps", owner_gap_count)

    labels = {
        row.id: f"{row.name}  ·  {row.classification.lower()}  ·  {row.reach_in_scope} downstream"
        for row in ranked.itertuples(index=False)
    }
    selected_id = st.selectbox(
        "Focused incident",
        options=ranked["id"].tolist(),
        format_func=lambda value: labels[value],
        help="Search by Job or Pipeline name, then select an incident.",
    )

    with st.spinner("Tracing downstream dependencies and owners..."):
        sub_nodes, sub_edges = load_subgraph(user_key, selected_id, depth)
    sub_nodes = recompute_failed_window(sub_nodes, window_hours)
    root_match = sub_nodes[sub_nodes["id"] == selected_id]
    if root_match.empty:
        st.error("The selected incident is missing from the graph snapshot.")
        return
    root_row = root_match.iloc[0]
    forward, reverse = build_adjacency(sub_edges)
    paths = downstream_paths(forward, selected_id, depth)
    affected = {node_id: info["hop"] for node_id, info in paths.items()}
    affected_df = (
        sub_nodes[sub_nodes["id"].isin(affected)]
        .assign(hop=lambda frame: frame["id"].map(affected))
        .sort_values(["hop", "type", "name"])
    )
    owners = owners_of(sub_nodes, set(affected))
    terminal_count = int(affected_df["type"].isin(TERMINAL_TYPES).sum()) if not affected_df.empty else 0
    classification = ranked.loc[ranked["id"] == selected_id, "classification"].iloc[0]
    render_incident_hero(root_row, classification, len(affected), terminal_count, len(owners))

    roles = {node_id: "AFFECTED" for node_id in affected}
    for node_id in sub_nodes[sub_nodes["is_failed"]]["id"]:
        roles[node_id] = "FAILED"
    for node_id in upstream_neighbors(reverse, {selected_id}):
        roles.setdefault(node_id, "CONTEXT")
    roles[selected_id] = "ROOT"

    graph_column, action_column = st.columns([1.85, 0.75], gap="large")
    with graph_column:
        with st.container(border=True):
            st.markdown("**Causal dependency graph**")
            st.caption("Left to right: failed producer → connecting table or trigger → downstream consumer")
            render_graph_legend()
            clicked = render_graph(sub_nodes, sub_edges, roles, layout, key=f"incident_{selected_id}")
            node_detail_panel(sub_nodes, clicked or selected_id)

    with action_column:
        with st.container(border=True):
            render_asset_mix(affected_df)
            st.divider()
            st.markdown('<div class="jiig-side-title">Owners to notify</div>', unsafe_allow_html=True)
            if owners:
                chips = "".join(f'<span class="jiig-owner-chip">{_h(owner)}</span>' for owner in owners[:8])
                st.markdown(chips, unsafe_allow_html=True)
                if len(owners) > 8:
                    st.caption(f"{len(owners) - 8} more owners in the impact table")
            else:
                st.markdown('<div class="jiig-callout">No downstream owner could be resolved. Assign ownership before closing this incident.</div>', unsafe_allow_html=True)
            unresolved = affected_df[
                affected_df["run_as_email"].isna() & affected_df["creator_email"].isna()
            ] if not affected_df.empty else affected_df
            if not unresolved.empty:
                st.warning(f"{len(unresolved)} affected asset(s) have no resolvable owner.")
            brief = notification_text(root_row, affected_df, depth)
            st.download_button(
                "Download incident brief",
                data=brief,
                file_name=f"jiig-{_s(root_row['entity_id'])}-incident.txt",
                mime="text/plain",
                width="stretch",
            )
            with st.expander("Preview notification"):
                st.code(brief, language=None)

    section_title(
        "Evidence",
        "Every affected asset with its causal path",
        "Paths include the intermediate tables that turn an upstream failure into stale downstream data.",
    )
    name_map = dict(zip(sub_nodes["id"], sub_nodes["name"]))
    if affected_df.empty:
        st.info("No downstream consumers were found in the selected lineage scope.")
    else:
        evidence = affected_df.copy()
        evidence["causal_path"] = evidence["id"].map(lambda node_id: path_text(paths[node_id], name_map))
        evidence["owner"] = evidence["run_as_email"].fillna(evidence["creator_email"]).fillna("Unresolved")
        evidence["state"] = evidence["is_failed"].map({True: "FAILED", False: "AT RISK"})
        st.dataframe(
            evidence[["hop", "name", "type", "state", "owner", "causal_path"]].rename(
                columns={"name": "affected asset", "type": "asset type"}
            ),
            hide_index=True,
            width="stretch",
            height=min(480, 58 + len(evidence) * 35),
            column_config={"causal_path": st.column_config.TextColumn("causal path", width="large")},
        )

    tables = critical_tables(forward, selected_id, affected)
    if tables:
        st.markdown("**Critical data contracts**")
        st.caption("Unique downstream assets exposed through each connecting table in this incident subtree.")
        st.dataframe(
            pd.DataFrame(tables, columns=["table", "assets exposed"]),
            hide_index=True,
            width="stretch",
        )

    with st.expander("Open incident queue"):
        queue = ranked[["name", "type", "classification", "last_failed_time", "failure_count", "reach_in_scope", "failure_detail"]].copy()
        queue["last_failed_time"] = queue["last_failed_time"].map(format_timestamp)
        st.dataframe(queue, hide_index=True, width="stretch")


def intelligence_view(user_key: str, stats: dict):
    section_title(
        "Dependency intelligence",
        "Find hubs, authorities, and concentrated risk",
        "Hubs expose many downstream assets when they fail. Authorities depend on many upstream producers and concentrate inbound risk.",
    )
    hub_nodes = load_leaders(user_key, "hub", 12)
    authority_nodes = load_leaders(user_key, "authority", 12)
    overview = load_overview(user_key, 50)

    metrics = st.columns(4)
    metrics[0].metric("Tracked assets", stats.get("total_nodes", 0))
    metrics[1].metric("Dependency edges", stats.get("total_edges", 0))
    metrics[2].metric("Business surfaces", stats.get("terminal_consumers", 0))
    metrics[3].metric("Owner gaps", stats.get("owners_unresolved", 0))

    left, right = st.columns(2, gap="large")
    with left:
        with st.container(border=True):
            st.markdown("**Top dependency hubs**")
            st.caption("Ranked by downstream reach, then direct consumers.")
            if not hub_nodes.empty:
                st.dataframe(
                    hub_nodes[["hub_rank", "name", "type", "downstream_reach", "out_degree", "is_failed"]].rename(
                        columns={"downstream_reach": "downstream", "out_degree": "direct consumers"}
                    ),
                    hide_index=True,
                    width="stretch",
                    height=390,
                )
    with right:
        with st.container(border=True):
            st.markdown("**Top dependency authorities**")
            st.caption("Ranked by inbound dependencies, then downstream reach.")
            if not authority_nodes.empty:
                st.dataframe(
                    authority_nodes[["authority_rank", "name", "type", "in_degree", "downstream_reach", "is_failed"]].rename(
                        columns={"in_degree": "upstream dependencies", "downstream_reach": "downstream"}
                    ),
                    hide_index=True,
                    width="stretch",
                    height=390,
                )

    if not overview.empty:
        st.markdown("**Dependency position map**")
        st.caption("Upper-right assets combine high inbound complexity with broad downstream exposure.")
        st.scatter_chart(
            overview,
            x="in_degree",
            y="downstream_reach",
            color="type",
            size="out_degree",
            width="stretch",
            height=430,
        )

    shared = load_shared_tables(user_key)
    if not shared.empty:
        st.markdown("**Shared-write tables outside dependency scoring**")
        st.caption("These tables exceed the configured writer fan-out cap. They remain visible as governance signals but do not create cartesian dependency edges.")
        st.dataframe(shared[["name", "writer_count"]], hide_index=True, width="stretch", height=230)


def explorer_view(user_key: str, layout: str, depth: int, window_hours: float):
    section_title(
        "Dependency explorer",
        "Trace any Job, Pipeline, or business surface",
        "Search one asset, then inspect the connected upstream inputs and downstream consumers without rendering the workspace as a hairball.",
    )
    with st.spinner("Loading the dependency index..."):
        nodes_df, edges_df = load_graph(user_key)
    if nodes_df.empty:
        st.info("No dependency data is available yet.")
        return
    nodes_df = recompute_failed_window(nodes_df, window_hours)
    labels = {
        row.id: f"{row.name} · {str(row.type).upper()} · {row.downstream_reach} downstream"
        for row in nodes_df.sort_values("criticality_rank").itertuples(index=False)
    }
    controls = st.columns([2, 1])
    selected_id = controls[0].selectbox(
        "Search asset",
        options=list(labels),
        format_func=lambda value: labels[value],
    )
    direction = controls[1].selectbox("Direction", ["Upstream + downstream", "Downstream only", "Upstream only"])

    forward, reverse = build_adjacency(edges_df)
    downstream = downstream_hops(forward, selected_id, depth) if direction != "Upstream only" else {}
    upstream = upstream_hops(reverse, selected_id, depth) if direction != "Downstream only" else {}
    visible_ids = {selected_id} | set(downstream) | set(upstream)
    visible_nodes = nodes_df[nodes_df["id"].isin(visible_ids)]
    visible_edges = edges_df[
        edges_df["source_id"].isin(visible_ids) & edges_df["target_id"].isin(visible_ids)
    ]
    roles = {node_id: "AFFECTED" for node_id in downstream}
    roles.update({node_id: "CONTEXT" for node_id in upstream})
    for node_id in visible_nodes[visible_nodes["is_failed"]]["id"]:
        roles[node_id] = "FAILED"
    roles[selected_id] = "ROOT"

    metrics = st.columns(4)
    metrics[0].metric("Upstream dependencies", len(upstream))
    metrics[1].metric("Downstream consumers", len(downstream))
    metrics[2].metric("Direct inputs", int(nodes_df.loc[nodes_df["id"] == selected_id, "in_degree"].iloc[0]))
    metrics[3].metric("Direct outputs", int(nodes_df.loc[nodes_df["id"] == selected_id, "out_degree"].iloc[0]))
    with st.container(border=True):
        render_graph_legend()
        clicked = render_graph(visible_nodes, visible_edges, roles, layout, key=f"explorer_{selected_id}_{direction}_{depth}", height=660)
        node_detail_panel(visible_nodes, clicked or selected_id)

    paths = downstream_paths(forward, selected_id, depth)
    if paths and direction != "Upstream only":
        name_map = dict(zip(nodes_df["id"], nodes_df["name"]))
        rows = [
            {"hop": info["hop"], "asset": name_map.get(node_id, node_id), "causal path": path_text(info, name_map)}
            for node_id, info in paths.items()
        ]
        st.dataframe(pd.DataFrame(rows).sort_values(["hop", "asset"]), hide_index=True, width="stretch")


def operations_view(user_key: str, stats: dict):
    section_title(
        "Operations",
        "Know whether the graph itself can be trusted",
        "Snapshot freshness, ownership coverage, entity coverage, and excluded shared tables are visible here.",
    )
    freshness = pd.to_datetime(stats.get("freshness"), utc=True, errors="coerce")
    age_minutes = None if pd.isna(freshness) else int((pd.Timestamp.now(tz="UTC") - freshness).total_seconds() / 60)
    metrics = st.columns(4)
    metrics[0].metric("Snapshot age", "Unknown" if age_minutes is None else f"{max(0, age_minutes)} min")
    metrics[1].metric("Tracked assets", stats.get("total_nodes", 0))
    metrics[2].metric("Owner gaps", stats.get("owners_unresolved", 0))
    metrics[3].metric("Excluded shared tables", stats.get("shared_tables", 0))
    if age_minutes is not None and age_minutes > 45:
        st.warning("The graph snapshot is older than the 30-minute schedule. Check the JIIG refresh job before acting on impact data.")
    else:
        st.success("The graph snapshot is within the expected refresh interval.")

    left, right = st.columns(2, gap="large")
    with left:
        st.markdown("**Asset coverage**")
        by_type = stats.get("by_type")
        if by_type is not None and not by_type.empty:
            st.dataframe(by_type, hide_index=True, width="stretch")
    with right:
        st.markdown("**Shared-write review queue**")
        shared = load_shared_tables(user_key)
        if shared.empty:
            st.info("No tables exceed the writer fan-out cap.")
        else:
            st.dataframe(shared[["name", "writer_count"]], hide_index=True, width="stretch")
    st.caption("Source: Databricks system.access.table_lineage and system.lakeflow timeline/metadata system tables. Snapshot values are produced by the scheduled JIIG SQL job.")


def sidebar_controls():
    st.sidebar.markdown("## Analysis scope")
    st.sidebar.caption("Controls apply consistently to incident status, graph colors, and visible reach.")
    maximum_hours = LOOKBACK_DAYS * 24
    window_hours = st.sidebar.slider(
        "Failure window",
        min_value=1,
        max_value=maximum_hours,
        value=min(24, maximum_hours),
        format="%d hours",
    )
    depth = st.sidebar.slider("Dependency depth", 1, 5, 3, format="%d hops")
    with st.sidebar.expander("Graph options"):
        layout = st.selectbox("Layout engine", LAYOUTS, index=0)
    if st.sidebar.button("Refresh snapshot", width="stretch"):
        st.cache_data.clear()
        st.rerun()
    st.sidebar.divider()
    st.sidebar.markdown("**Operational semantics**")
    st.sidebar.caption("Open incident: the latest Job run or Pipeline refresh in the lookback window is failed.")
    st.sidebar.caption("Likely cascade: an earlier failed upstream reaches this failure within the selected depth. Treat as evidence, not automated root-cause proof.")
    return window_hours, depth, layout


def main():
    inject_styles()
    window_hours, depth, layout = sidebar_controls()
    user_key = current_user_key()
    try:
        with st.spinner("Loading the latest incident snapshot..."):
            incidents_df = load_incidents(user_key, window_hours)
            stats = load_stats(user_key)
    except Exception as error:
        st.error(f"JIIG could not load the graph snapshot: {error}")
        st.caption("Check the SQL warehouse, output table permissions, and whether the scheduled JIIG job has completed.")
        return

    render_topbar(stats.get("freshness"))
    if not stats.get("total_nodes"):
        st.warning("No graph snapshot exists yet. Run the JIIG refresh job, then reload this app.")
        return

    navigation = st.segmented_control(
        "View",
        ["Incident Command", "Dependency Intelligence", "Explorer", "Operations"],
        default="Incident Command",
        label_visibility="collapsed",
    )
    if navigation == "Incident Command":
        incident_view(user_key, incidents_df, stats, layout, depth, window_hours)
    elif navigation == "Dependency Intelligence":
        intelligence_view(user_key, stats)
    elif navigation == "Explorer":
        explorer_view(user_key, layout, depth, window_hours)
    else:
        operations_view(user_key, stats)


if __name__ == "__main__":
    main()
