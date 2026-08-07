from pathlib import Path

from streamlit.testing.v1 import AppTest


def test_incident_command_renders_with_demo_data(monkeypatch):
    monkeypatch.setenv("JIIG_DEMO_MODE", "true")
    monkeypatch.setenv("JIIG_DEMO_NOW", "2026-08-07T01:00:00Z")
    app_path = Path(__file__).parents[1] / "src" / "apps" / "app.py"

    app = AppTest.from_file(str(app_path), default_timeout=20).run()

    assert not app.exception
    assert [metric.label for metric in app.metric] == [
        "Open incidents",
        "Likely root incidents",
        "Largest reach · 3 hops",
        "Incident owner gaps",
    ]
    assert app.selectbox[0].label == "Focused incident"
