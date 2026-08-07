import os
from collections import deque

import pandas as pd


NODE_COLUMNS = [
    "id", "entity_id", "type", "subtype", "name", "description", "creator_email", "run_as_email",
    "is_failed", "last_failed_time", "first_failed_time", "failure_count", "failure_detail",
    "last_activity_time", "created_time", "status", "snapshot_time", "in_degree", "out_degree",
    "downstream_reach", "criticality_rank", "hub_rank", "authority_rank",
]
EDGE_COLUMNS = ["id", "type", "source_id", "target_id", "connecting_tables", "edge_table_count", "edge_kinds"]


def _now():
    value = os.getenv("JIIG_DEMO_NOW")
    if value:
        return pd.Timestamp(value, tz="UTC")
    return pd.Timestamp.now(tz="UTC").floor("min")


def _frames():
    now = _now()
    nodes = [
        ["JOB:90", "90", "job", None, "Raw Commerce CDC", "Ingests commerce source events", "data-platform@example.com", "svc-data-platform", False, None, None, 0, None, now - pd.Timedelta(minutes=11), now - pd.Timedelta(days=220), "HEALTHY", now, 0, 1, 9, 1, 1, 9],
        ["JOB:100", "100", "job", None, "Orders Bronze Ingestion", "Loads validated order events", "ingestion@example.com", "svc-ingestion", True, now - pd.Timedelta(hours=2, minutes=14), now - pd.Timedelta(days=2), 3, "RUN_EXECUTION_ERROR", now - pd.Timedelta(hours=2, minutes=14), now - pd.Timedelta(days=160), "FAILED", now, 1, 3, 8, 2, 2, 2],
        ["PIPELINE:200", "200", "pipeline", "MATERIALIZED_VIEW", "Customer 360 Pipeline", "MATERIALIZED_VIEW · serverless", "customer-data@example.com", "svc-customer-data", True, now - pd.Timedelta(hours=1, minutes=42), now - pd.Timedelta(hours=1, minutes=42), 1, "update_type=REFRESH", now - pd.Timedelta(hours=1, minutes=42), now - pd.Timedelta(days=110), "FAILED", now, 1, 3, 5, 3, 3, 3],
        ["JOB:110", "110", "job", None, "Revenue Mart Build", "Publishes finance-ready revenue marts", "finance-data@example.com", "svc-finance-data", False, None, None, 0, None, now - pd.Timedelta(hours=3), now - pd.Timedelta(days=130), "HEALTHY", now, 1, 2, 3, 4, 4, 4],
        ["PIPELINE:210", "210", "pipeline", "STREAMING_TABLE", "Inventory Availability Stream", "STREAMING_TABLE · continuous", "supply-data@example.com", "svc-supply-data", False, None, None, 0, None, now - pd.Timedelta(minutes=4), now - pd.Timedelta(days=80), "HEALTHY", now, 1, 1, 1, 5, 5, 5],
        ["DASHBOARD:rev-exec", "rev-exec", "dashboard", None, "Executive Revenue Pulse", "Business intelligence consumer", "finance-analytics@example.com", "finance-analytics@example.com", False, None, None, 0, None, now - pd.Timedelta(minutes=8), now - pd.Timedelta(days=72), "HEALTHY", now, 2, 0, 0, 7, 7, 1],
        ["GENIE:sales-ops", "sales-ops", "genie", None, "Sales Operations Genie", "Natural-language analytics consumer", "sales-ops@example.com", "sales-ops@example.com", False, None, None, 0, None, now - pd.Timedelta(minutes=6), now - pd.Timedelta(days=65), "HEALTHY", now, 1, 0, 0, 8, 8, 7],
        ["ALERT:margin-low", "margin-low", "alert", None, "Margin Guardrail Alert", "SQL alert consumer", "finops@example.com", "finops@example.com", False, None, None, 0, None, now - pd.Timedelta(minutes=13), now - pd.Timedelta(days=61), "HEALTHY", now, 1, 0, 0, 6, 6, 6],
        ["QUERY:daily-close", "daily-close", "query", None, "Daily Finance Close Extract", "Scheduled SQL consumer", None, None, False, None, None, 0, None, now - pd.Timedelta(hours=5), now - pd.Timedelta(days=55), "HEALTHY", now, 1, 0, 0, 10, 10, 8],
        ["JOB:300", "300", "job", "UNREGISTERED", "Job 300 [UNREGISTERED]", "Seen in run history and lineage only", "growth-data@example.com", "growth-data@example.com", True, now - pd.Timedelta(hours=7), now - pd.Timedelta(hours=7), 1, "TIMED_OUT", now - pd.Timedelta(hours=7), None, "FAILED", now, 0, 0, 0, 9, 9, 10],
    ]
    edges = [
        ["JOB:90->JOB:100", "dependency", "JOB:90", "JOB:100", "main.bronze.order_events", 1, ["DEPENDENCY"]],
        ["JOB:100->PIPELINE:200", "dependency", "JOB:100", "PIPELINE:200", "main.silver.orders", 1, ["DEPENDENCY"]],
        ["JOB:100->JOB:110", "dependency", "JOB:100", "JOB:110", "main.silver.orders, main.silver.payments", 2, ["DEPENDENCY"]],
        ["JOB:100->PIPELINE:210", "dependency+table_trigger", "JOB:100", "PIPELINE:210", "main.silver.order_items", 1, ["DEPENDENCY", "TABLE_TRIGGER"]],
        ["PIPELINE:200->DASHBOARD:rev-exec", "dependency", "PIPELINE:200", "DASHBOARD:rev-exec", "main.gold.customer_360", 1, ["DEPENDENCY"]],
        ["PIPELINE:200->GENIE:sales-ops", "dependency", "PIPELINE:200", "GENIE:sales-ops", "main.gold.customer_360", 1, ["DEPENDENCY"]],
        ["PIPELINE:200->ALERT:margin-low", "dependency", "PIPELINE:200", "ALERT:margin-low", "main.gold.customer_margin", 1, ["DEPENDENCY"]],
        ["JOB:110->DASHBOARD:rev-exec", "dependency", "JOB:110", "DASHBOARD:rev-exec", "main.gold.daily_revenue", 1, ["DEPENDENCY"]],
        ["JOB:110->QUERY:daily-close", "dependency", "JOB:110", "QUERY:daily-close", "main.gold.daily_revenue", 1, ["DEPENDENCY"]],
        ["PIPELINE:210->GENIE:sales-ops", "dependency", "PIPELINE:210", "GENIE:sales-ops", "main.gold.inventory_availability", 1, ["DEPENDENCY"]],
    ]
    return pd.DataFrame(nodes, columns=NODE_COLUMNS), pd.DataFrame(edges, columns=EDGE_COLUMNS)


def _adjacency(edges):
    forward, reverse = {}, {}
    for row in edges.itertuples(index=False):
        forward.setdefault(row.source_id, []).append(row.target_id)
        reverse.setdefault(row.target_id, []).append(row.source_id)
    return forward, reverse


def _hops(adjacency, root, depth):
    found = {root: 0}
    queue = deque([root])
    while queue:
        node = queue.popleft()
        if found[node] >= depth:
            continue
        for target in adjacency.get(node, []):
            if target not in found:
                found[target] = found[node] + 1
                queue.append(target)
    found.pop(root, None)
    return found


def load_incidents(window_hours):
    nodes, _ = _frames()
    since = _now() - pd.Timedelta(hours=float(window_hours))
    return nodes[nodes["is_failed"] & (nodes["last_failed_time"] >= since)].copy()


def load_subgraph(root_id, depth):
    nodes, edges = _frames()
    forward, reverse = _adjacency(edges)
    ids = {root_id} | set(_hops(forward, root_id, depth)) | set(reverse.get(root_id, []))
    return (
        nodes[nodes["id"].isin(ids)].copy(),
        edges[edges["source_id"].isin(ids) & edges["target_id"].isin(ids)].copy(),
    )


def load_overview(limit):
    nodes, _ = _frames()
    return nodes.sort_values("criticality_rank").head(limit).copy()


def load_leaders(dimension, limit):
    nodes, _ = _frames()
    return nodes.sort_values(f"{dimension}_rank").head(limit).copy()


def load_shared_tables():
    return pd.DataFrame(
        [
            ["TABLE:platform.audit_events", "platform.audit_events", 48],
            ["TABLE:shared.feature_flags", "shared.feature_flags", 27],
        ],
        columns=["id", "name", "writer_count"],
    )


def load_stats():
    nodes, edges = _frames()
    by_type = (
        nodes.assign(subtype=nodes["subtype"].fillna("-"))
        .groupby(["type", "subtype"], as_index=False)
        .agg(entities=("id", "count"), failed=("is_failed", "sum"))
        .sort_values("entities", ascending=False)
    )
    return {
        "total_nodes": len(nodes),
        "total_edges": len(edges),
        "shared_tables": 2,
        "failed_nodes": int(nodes["is_failed"].sum()),
        "terminal_consumers": int(nodes["type"].isin(["dashboard", "genie", "query", "alert"]).sum()),
        "owners_unresolved": int((nodes["run_as_email"].isna() & nodes["creator_email"].isna()).sum()),
        "freshness": _now(),
        "by_type": by_type,
    }


def load_incident_signals(failed_ids, depth):
    _, edges = _frames()
    forward, _ = _adjacency(edges)
    reach = {root: len(_hops(forward, root, depth)) for root in failed_ids}
    failed = set(failed_ids)
    pairs = []
    for root in failed_ids:
        for target, hop in _hops(forward, root, depth).items():
            if target in failed and target != root:
                pairs.append([f"{root}=>{target}", "dependency", root, target, "", 0, None])
    return reach, pd.DataFrame(pairs, columns=EDGE_COLUMNS)


def load_graph():
    return _frames()
