import sqlite3

from datasheet_ai.models import ColumnSchema, TableSchema


def execute_non_query(
    conn: sqlite3.Connection,
    sql: str,
    params: tuple = (),
) -> None:
    """
    Execute a non-SELECT SQL statement, such as CREATE/INSERT/UPDATE.

    Parameters
    ----------
    conn : sqlite3.Connection
        Open SQLite connection.
    sql : str
        SQL statement to execute.
    params : tuple
        Positional parameters for the SQL statement.
    """
    cursor = conn.cursor()
    cursor.execute(sql, params)
    conn.commit()


def execute_many(
    conn: sqlite3.Connection,
    sql: str,
    params_list: list[tuple],
) -> None:
    """
    Execute a parameterized SQL statement many times.

    Parameters
    ----------
    conn : sqlite3.Connection
        Open SQLite connection.
    sql : str
        SQL statement to execute.
    params_list : list[tuple]
        List of parameter tuples.
    """
    cursor = conn.cursor()
    cursor.executemany(sql, params_list)
    conn.commit()


def execute_query(
    conn: sqlite3.Connection,
    sql: str,
    params: tuple = (),
) -> tuple[list[str], list[tuple]]:
    """
    Execute a SELECT query and return column names plus row tuples.

    Parameters
    ----------
    conn : sqlite3.Connection
        Open SQLite connection.
    sql : str
        SELECT SQL statement.
    params : tuple
        Positional parameters for the SQL statement.

    Returns
    -------
    tuple[list[str], list[tuple]]
        A pair of (column_names, rows).
    """
    cursor = conn.cursor()
    cursor.execute(sql, params)

    columns = [desc[0] for desc in cursor.description] if cursor.description else []
    rows = [tuple(row) for row in cursor.fetchall()]
    return columns, rows


def list_tables(conn: sqlite3.Connection) -> list[str]:
    """
    Return all user-defined table names in the SQLite database.
    """
    sql = """
    SELECT name
    FROM sqlite_master
    WHERE type = 'table'
      AND name NOT LIKE 'sqlite_%'
    ORDER BY name;
    """
    _, rows = execute_query(conn, sql)
    return [row[0] for row in rows]


def table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    """
    Check whether a table exists.
    """
    sql = """
    SELECT 1
    FROM sqlite_master
    WHERE type = 'table' AND name = ?
    LIMIT 1;
    """
    _, rows = execute_query(conn, sql, (table_name,))
    return len(rows) > 0


def get_table_schema(conn: sqlite3.Connection, table_name: str) -> TableSchema:
    """
    Return the schema of a table using PRAGMA table_info.

    Parameters
    ----------
    conn : sqlite3.Connection
        Open SQLite connection.
    table_name : str
        Name of the table.

    Returns
    -------
    TableSchema
        Structured schema for the target table.

    Raises
    ------
    ValueError
        If the table does not exist.
    """
    if not table_exists(conn, table_name):
        raise ValueError(f"Table does not exist: {table_name}")

    cursor = conn.cursor()
    cursor.execute(f"PRAGMA table_info({table_name})")
    pragma_rows = cursor.fetchall()

    columns: list[ColumnSchema] = []
    for row in pragma_rows:
        columns.append(
            ColumnSchema(
                name=row["name"],
                dtype=row["type"],
            )
        )

    return TableSchema(table_name=table_name, columns=columns)