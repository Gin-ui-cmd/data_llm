from pathlib import Path
import sqlite3


def get_connection(db_path: str | Path) -> sqlite3.Connection:
    """
    Create and return a SQLite connection.

    Parameters
    ----------
    db_path : str | Path
        Path to the SQLite database file.

    Returns
    -------
    sqlite3.Connection
        An open SQLite connection.
    """
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    return conn