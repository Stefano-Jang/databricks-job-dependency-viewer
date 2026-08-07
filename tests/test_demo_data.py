import demo_data


def test_demo_graph_uses_kind_qualified_ids():
    nodes, edges = demo_data.load_graph()

    assert nodes["id"].str.contains(":", regex=False).all()
    assert nodes["id"].is_unique
    assert set(edges["source_id"]).issubset(set(nodes["id"]))
    assert set(edges["target_id"]).issubset(set(nodes["id"]))


def test_demo_incident_signals_find_cascade_through_graph():
    reach, pairs = demo_data.load_incident_signals(
        ("JOB:100", "PIPELINE:200", "JOB:300"),
        3,
    )

    assert reach["JOB:100"] >= 7
    assert ((pairs["source_id"] == "JOB:100") & (pairs["target_id"] == "PIPELINE:200")).any()


def test_demo_subgraph_is_closed():
    nodes, edges = demo_data.load_subgraph("JOB:100", 2)
    node_ids = set(nodes["id"])

    assert set(edges["source_id"]).issubset(node_ids)
    assert set(edges["target_id"]).issubset(node_ids)
