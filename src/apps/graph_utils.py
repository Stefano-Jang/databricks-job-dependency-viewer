"""Pure graph/insight helpers for the JIIG app (no Streamlit imports)."""
from collections import deque

import pandas as pd


def build_adjacency(edges_df: pd.DataFrame):
    """Return (forward, reverse) adjacency dicts: node_id -> list of edge dicts."""
    forward, reverse = {}, {}
    if edges_df is None or edges_df.empty:
        return forward, reverse
    for row in edges_df.itertuples(index=False):
        edge = {
            "source": str(row.source_id),
            "target": str(row.target_id),
            "kind": str(row.type),
            "tables": str(row.connecting_tables or ""),
        }
        forward.setdefault(edge["source"], []).append(edge)
        reverse.setdefault(edge["target"], []).append(edge)
    return forward, reverse


def downstream_hops(forward: dict, root: str, max_depth: int) -> dict:
    """BFS from root following forward edges; returns {node_id: min_hop}, root excluded."""
    hops = {root: 0}
    queue = deque([root])
    while queue:
        node = queue.popleft()
        if hops[node] >= max_depth:
            continue
        for edge in forward.get(node, []):
            nxt = edge["target"]
            if nxt not in hops:
                hops[nxt] = hops[node] + 1
                queue.append(nxt)
    hops.pop(root, None)
    return hops


def downstream_paths(forward: dict, root: str, max_depth: int) -> dict:
    """Return the shortest causal path from root to every downstream node."""
    paths = {
        root: {
            "hop": 0,
            "node_path": [root],
            "edge_tables": [],
            "edge_kinds": [],
        }
    }
    queue = deque([root])
    while queue:
        node = queue.popleft()
        current = paths[node]
        if current["hop"] >= max_depth:
            continue
        for edge in forward.get(node, []):
            target = edge["target"]
            if target in paths:
                continue
            tables = [table.strip() for table in edge["tables"].split(", ") if table.strip()]
            paths[target] = {
                "hop": current["hop"] + 1,
                "node_path": current["node_path"] + [target],
                "edge_tables": current["edge_tables"] + [tables],
                "edge_kinds": current["edge_kinds"] + [edge["kind"]],
            }
            queue.append(target)
    paths.pop(root, None)
    return paths


def upstream_hops(reverse: dict, root: str, max_depth: int) -> dict:
    """BFS from root following reverse edges; root is excluded."""
    hops = {root: 0}
    queue = deque([root])
    while queue:
        node = queue.popleft()
        if hops[node] >= max_depth:
            continue
        for edge in reverse.get(node, []):
            source = edge["source"]
            if source not in hops:
                hops[source] = hops[node] + 1
                queue.append(source)
    hops.pop(root, None)
    return hops


def upstream_neighbors(reverse: dict, node_ids: set) -> set:
    """Direct (1-hop) upstream neighbors of the given nodes, excluding themselves."""
    ups = set()
    for nid in node_ids:
        for edge in reverse.get(nid, []):
            ups.add(edge["source"])
    return ups - set(node_ids)


def classify_incidents(failed_ids: list, forward: dict, max_depth: int,
                       failed_times: dict = None) -> dict:
    """For each failed entity: blast radius + ROOT/CASCADE classification.

    An incident is a CASCADE when the failed entity sits downstream of another
    failed entity (within max_depth) — its failure is likely a consequence,
    not a root cause. Failures forming a dependency cycle would all mark each
    other as cascades; when failed_times is given, the earliest failure in a
    pure cycle is promoted back to ROOT CAUSE so every cycle has an entry point.
    """
    failed_set = set(failed_ids)
    result = {}
    for fid in failed_ids:
        hops = downstream_hops(forward, fid, max_depth)
        result[fid] = {
            "affected": hops,            # {node_id: hop}
            "downstream_failed": sorted(failed_set & set(hops)),
            "is_cascade": False,
        }
    for fid, info in result.items():
        for other in info["downstream_failed"]:
            if not failed_times:
                result[other]["is_cascade"] = True
                continue
            upstream_time = pd.to_datetime(failed_times.get(fid), utc=True, errors="coerce")
            downstream_time = pd.to_datetime(failed_times.get(other), utc=True, errors="coerce")
            if pd.isna(upstream_time) or pd.isna(downstream_time) or upstream_time <= downstream_time:
                result[other]["is_cascade"] = True

    if failed_times:
        reach = {fid: set(result[fid]["downstream_failed"]) for fid in result}
        for fid, info in result.items():
            if not info["is_cascade"]:
                continue
            ancestors = {a for a in reach if fid in reach[a] and a != fid}
            mutual = {a for a in ancestors if a in reach[fid]}
            if ancestors and ancestors == mutual:
                group = mutual | {fid}
                earliest = min(group, key=lambda x: (str(failed_times.get(x)), str(x)))
                if fid == earliest:
                    info["is_cascade"] = False
    return result


def critical_tables(forward: dict, root: str, affected: dict, top_n: int = 10) -> list:
    """Rank tables by unique affected entities reachable through each table."""
    impacted = set(affected) | {root}
    exposure = {}
    for src in impacted:
        for edge in forward.get(src, []):
            if edge["target"] not in impacted or not edge["tables"]:
                continue
            descendants = set(downstream_hops(forward, edge["target"], len(impacted)))
            exposed = ({edge["target"]} | descendants) & impacted
            for tbl in edge["tables"].split(", "):
                tbl = tbl.strip()
                if tbl:
                    exposure.setdefault(tbl, set()).update(exposed)
    ranked = sorted(
        ((table, len(nodes)) for table, nodes in exposure.items()),
        key=lambda item: (-item[1], item[0]),
    )
    return ranked[:top_n]


def owners_of(nodes_df: pd.DataFrame, node_ids: set) -> list:
    """Distinct, sorted owner emails (run_as first, creator as fallback)."""
    if nodes_df is None or nodes_df.empty or not node_ids:
        return []
    sub = nodes_df[nodes_df["id"].astype(str).isin({str(n) for n in node_ids})]
    owners = set()
    for row in sub.itertuples(index=False):
        for owner in (getattr(row, "run_as_email", None), getattr(row, "creator_email", None)):
            if owner is not None and pd.notna(owner) and str(owner):
                owners.add(str(owner))
                break
    return sorted(owners)


def owners_by_kind(nodes_df: pd.DataFrame, node_ids: set) -> dict:
    """Map consumer node type -> list of owner emails for notifications.

    Allows notifications to be grouped per consumer kind (e.g., separate wording
    for dashboard owners vs job owners).
    """
    if nodes_df is None or nodes_df.empty or not node_ids:
        return {}
    sub = nodes_df[nodes_df["id"].astype(str).isin({str(n) for n in node_ids})]
    result = {}
    for row in sub.itertuples(index=False):
        ntype = str(getattr(row, "type", "unknown")).lower()
        owner = None
        for candidate in (getattr(row, "run_as_email", None), getattr(row, "creator_email", None)):
            if candidate is not None and pd.notna(candidate) and str(candidate):
                owner = str(candidate)
                break
        if owner:
            result.setdefault(ntype, set()).add(owner)
    return {k: sorted(v) for k, v in result.items()}
