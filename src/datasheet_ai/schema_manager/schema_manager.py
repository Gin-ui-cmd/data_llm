import sqlite3

from datasheet_ai.db.sqlite_setup import get_table_schema, list_tables
from datasheet_ai.models import ColumnSchema, SchemaMatchResult, TableSchema


def _remove_auto_id_column(schema: TableSchema) -> TableSchema:
    """
    Return a copy of the schema without the auto-generated 'id' column.

    This is useful because CSV-derived schemas typically do not include the
    SQLite auto-increment primary key column.
    """
    filtered_columns = [
        ColumnSchema(name=col.name, dtype=col.dtype)
        for col in schema.columns
        if col.name != "id"
    ]
    return TableSchema(table_name=schema.table_name, columns=filtered_columns)


def _normalize_schema_for_compare(schema: TableSchema) -> TableSchema:
    """
    Normalize a schema for comparison.

    Current MVP behavior:
    - remove the auto-generated 'id' column
    - keep column order as-is
    """
    return _remove_auto_id_column(schema)


def get_existing_tables(conn: sqlite3.Connection) -> list[str]:
    """
    Return all existing user-defined tables.
    """
    return list_tables(conn)


def get_existing_schema(conn: sqlite3.Connection, table_name: str) -> TableSchema:
    """
    Return the schema of an existing table.
    """
    return get_table_schema(conn, table_name)


def compare_schemas(csv_schema: TableSchema, db_schema: TableSchema) -> SchemaMatchResult:
    """
    Compare a CSV-derived schema with an existing DB schema.

    Matching rules for MVP:
    - ignore the DB auto-generated 'id' column
    - number of columns must match
    - normalized column names must match in order
    - data types must match in order

    Parameters
    ----------
    csv_schema : TableSchema
        Schema inferred from the CSV file.
    db_schema : TableSchema
        Schema read from the existing SQLite table.

    Returns
    -------
    SchemaMatchResult
        Structured comparison result.
    """
    normalized_csv_schema = _normalize_schema_for_compare(csv_schema)
    normalized_db_schema = _normalize_schema_for_compare(db_schema)

    csv_columns = normalized_csv_schema.columns
    db_columns = normalized_db_schema.columns

    normalized_csv_column_names = [col.name for col in csv_columns]

    if len(csv_columns) != len(db_columns):
        return SchemaMatchResult(
            is_match=False,
            normalized_csv_columns=normalized_csv_column_names,
            reason=(
                f"Column count mismatch: "
                f"CSV has {len(csv_columns)} columns, "
                f"DB table has {len(db_columns)} columns."
            ),
        )

    for csv_col, db_col in zip(csv_columns, db_columns):
        if csv_col.name != db_col.name:
            return SchemaMatchResult(
                is_match=False,
                normalized_csv_columns=normalized_csv_column_names,
                reason=(
                    f"Column name mismatch: "
                    f"CSV column '{csv_col.name}' does not match "
                    f"DB column '{db_col.name}'."
                ),
            )

        if csv_col.dtype.upper() != db_col.dtype.upper():
            return SchemaMatchResult(
                is_match=False,
                normalized_csv_columns=normalized_csv_column_names,
                reason=(
                    f"Column type mismatch for '{csv_col.name}': "
                    f"CSV type '{csv_col.dtype}' does not match "
                    f"DB type '{db_col.dtype}'."
                ),
            )

    return SchemaMatchResult(
        is_match=True,
        normalized_csv_columns=normalized_csv_column_names,
        reason="Schemas match.",
    )


def should_append_to_existing_table(
    conn: sqlite3.Connection,
    csv_schema: TableSchema,
) -> tuple[bool, str]:
    """
    Determine whether the CSV schema matches an existing table.

    Parameters
    ----------
    conn : sqlite3.Connection
        Open SQLite connection.
    csv_schema : TableSchema
        Schema inferred from the CSV file.

    Returns
    -------
    tuple[bool, str]
        (should_append, matched_table_name)

        - (True, table_name) if a matching table is found
        - (False, "") if no matching table is found
    """
    existing_tables = get_existing_tables(conn)

    for table_name in existing_tables:
        db_schema = get_existing_schema(conn, table_name)
        match_result = compare_schemas(csv_schema, db_schema)

        if match_result.is_match:
            return True, table_name

    return False, ""