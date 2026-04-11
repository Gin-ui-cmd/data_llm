import sqlite3

from datasheet_ai.db.sqlite_setup import get_table_schema, list_tables
from datasheet_ai.models import ColumnSchema, SchemaMatchResult, TableSchema


def _remove_auto_id_column(schema: TableSchema) -> TableSchema:
    """
    Return a copy of the schema without the auto-generated 'id' column.

    CSV-based schemas usually do not include SQLite's auto-increment
    primary key, so we remove it before doing schema comparisons.
    """
    filtered_columns = [
        ColumnSchema(name=col.name, dtype=col.dtype)
        for col in schema.columns
        if col.name != "id"
    ]
    return TableSchema(table_name=schema.table_name, columns=filtered_columns)


def _normalize_schema_for_compare(schema: TableSchema) -> TableSchema:
    """
    Normalize a schema before comparison.

    Current MVP behavior is intentionally simple:
    - remove the auto-generated 'id' column
    - keep the original column order
    """
    return _remove_auto_id_column(schema)


def get_existing_tables(conn: sqlite3.Connection) -> list[str]:
    """
    Return all user-defined tables currently in the database.
    """
    return list_tables(conn)


def get_existing_schema(conn: sqlite3.Connection, table_name: str) -> TableSchema:
    """
    Return the schema for one existing table.
    """
    return get_table_schema(conn, table_name)


def compare_schemas(csv_schema: TableSchema, db_schema: TableSchema) -> SchemaMatchResult:
    """
    Compare a CSV-inferred schema with a schema from an existing DB table.

    MVP matching rules:
    - ignore the auto-generated 'id' column on the DB side
    - column count must match
    - column names must match in order
    - column data types must match in order
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

    # Compare columns one by one in order.
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
    Check whether the incoming CSV schema matches any existing table.

    Return:
    - (True, table_name) if we find a compatible table
    - (False, "") if nothing matches
    """
    existing_tables = get_existing_tables(conn)

    for table_name in existing_tables:
        db_schema = get_existing_schema(conn, table_name)
        match_result = compare_schemas(csv_schema, db_schema)

        if match_result.is_match:
            return True, table_name

    return False, ""