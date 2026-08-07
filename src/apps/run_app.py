import os


port = os.getenv("DATABRICKS_APP_PORT", "8000")
os.execvp(
    "streamlit",
    ["streamlit", "run", "app.py", "--server.port", port, "--server.address", "0.0.0.0"],
)
