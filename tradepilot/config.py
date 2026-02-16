from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "data" / "tradepilot.duckdb"
DATA_PROVIDER = "mock"  # "mock" or "akshare"
