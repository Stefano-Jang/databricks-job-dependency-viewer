import pandas as pd

from graph_utils import (
    build_adjacency,
    classify_incidents,
    critical_tables,
    downstream_paths,
    upstream_hops,
)


def edge_frame(rows):
    return pd.DataFrame(
        rows,
        columns=["source_id", "target_id", "type", "connecting_tables"],
    )


def test_downstream_paths_preserve_table_evidence():
    edges = edge_frame([
        ("JOB:1", "PIPELINE:2", "dependency", "catalog.silver.orders"),
        ("PIPELINE:2", "DASHBOARD:3", "dependency", "catalog.gold.revenue"),
    ])
    forward, _ = build_adjacency(edges)

    paths = downstream_paths(forward, "JOB:1", 3)

    assert paths["DASHBOARD:3"]["hop"] == 2
    assert paths["DASHBOARD:3"]["node_path"] == ["JOB:1", "PIPELINE:2", "DASHBOARD:3"]
    assert paths["DASHBOARD:3"]["edge_tables"] == [
        ["catalog.silver.orders"],
        ["catalog.gold.revenue"],
    ]


def test_cascade_classification_uses_collapsed_multi_hop_pair_and_time_order():
    collapsed = edge_frame([
        ("JOB:1", "JOB:3", "dependency", ""),
    ])
    forward, _ = build_adjacency(collapsed)
    times = {
        "JOB:1": pd.Timestamp("2026-08-07T01:00:00Z"),
        "JOB:3": pd.Timestamp("2026-08-07T01:20:00Z"),
    }

    result = classify_incidents(["JOB:1", "JOB:3"], forward, 3, times)

    assert result["JOB:1"]["is_cascade"] is False
    assert result["JOB:3"]["is_cascade"] is True


def test_later_upstream_failure_does_not_relabel_earlier_failure_as_cascade():
    collapsed = edge_frame([
        ("JOB:1", "JOB:3", "dependency", ""),
    ])
    forward, _ = build_adjacency(collapsed)
    times = {
        "JOB:1": pd.Timestamp("2026-08-07T02:00:00Z"),
        "JOB:3": pd.Timestamp("2026-08-07T01:20:00Z"),
    }

    result = classify_incidents(["JOB:1", "JOB:3"], forward, 3, times)

    assert result["JOB:3"]["is_cascade"] is False


def test_critical_tables_count_unique_exposed_assets():
    edges = edge_frame([
        ("JOB:1", "JOB:2", "dependency", "catalog.silver.orders"),
        ("JOB:2", "DASHBOARD:3", "dependency", "catalog.gold.revenue"),
        ("JOB:2", "ALERT:4", "dependency", "catalog.gold.revenue"),
    ])
    forward, reverse = build_adjacency(edges)

    ranked = critical_tables(
        forward,
        "JOB:1",
        {"JOB:2": 1, "DASHBOARD:3": 2, "ALERT:4": 2},
    )

    assert ranked[0] == ("catalog.silver.orders", 3)
    assert ranked[1] == ("catalog.gold.revenue", 2)
    assert upstream_hops(reverse, "DASHBOARD:3", 2) == {"JOB:2": 1, "JOB:1": 2}
