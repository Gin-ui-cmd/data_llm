from pathlib import Path
#this code is generated with the help of AI
import pandas as pd
import pytest

from datasheet_ai.data_loader.csv_loader import infer_table_schema
from datasheet_ai.db.connection import get_connection
from datasheet_ai.db.sqlite_setup import execute_non_query
from datasheet_ai.schema_manager.schema_manager import (
    compare_schemas,
    get_existing_schema,
    get_existing_tables,
    should_append_to_existing_table,
)


@pytest.fixture
def conn(tmp_path):
    db_path = tmp_path / "schema_manager.db"
    connection = get_connection(db_path)
    yield connection
    connection.close()


def test_get_existing_tables_returns_empty_list_for_new_db(conn):
    tables = get_existing_tables(conn)
    assert tables == []


def test_get_existing_tables_returns_created_tables(conn):
    execute_non_query(
        conn,
        """
        CREATE TABLE students (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            first_name TEXT,
            last_name TEXT,
            age INTEGER,
            major TEXT
        );
        """,
    )

    execute_non_query(
        conn,
        """
        CREATE TABLE classes (
            class_id INTEGER PRIMARY KEY AUTOINCREMENT,
            class_name TEXT,
            instructor TEXT,
            credits INTEGER
        );
        """,
    )

    tables = get_existing_tables(conn)

    assert "students" in tables
    assert "classes" in tables


def test_get_existing_schema_returns_structured_schema(conn):
    execute_non_query(
        conn,
        """
        CREATE TABLE students (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            first_name TEXT,
            last_name TEXT,
            age INTEGER,
            major TEXT
        );
        """,
    )

    schema = get_existing_schema(conn, "students")

    assert schema.table_name == "students"
    assert [col.name for col in schema.columns] == [
        "id",
        "first_name",
        "last_name",
        "age",
        "major",
    ]
    assert [col.dtype for col in schema.columns] == [
        "INTEGER",
        "TEXT",
        "TEXT",
        "INTEGER",
        "TEXT",
    ]


def test_compare_schemas_matches_when_db_has_auto_id_column(conn):
    execute_non_query(
        conn,
        """
        CREATE TABLE students (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            first_name TEXT,
            last_name TEXT,
            age INTEGER,
            major TEXT
        );
        """,
    )

    db_schema = get_existing_schema(conn, "students")

    df = pd.DataFrame(
        {
            "First Name": ["Alice", "Bob"],
            "Last Name": ["Johnson", "Smith"],
            "Age": [20, 22],
            "Major": ["CS", "Math"],
        }
    )
    csv_schema = infer_table_schema(df, "students")

    result = compare_schemas(csv_schema, db_schema)

    assert result.is_match is True
    assert result.reason == "Schemas match."
    assert result.normalized_csv_columns == [
        "first_name",
        "last_name",
        "age",
        "major",
    ]


def test_compare_schemas_detects_column_count_mismatch(conn):
    execute_non_query(
        conn,
        """
        CREATE TABLE students (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            first_name TEXT,
            age INTEGER
        );
        """,
    )

    db_schema = get_existing_schema(conn, "students")

    df = pd.DataFrame(
        {
            "First Name": ["Alice"],
            "Last Name": ["Johnson"],
            "Age": [20],
        }
    )
    csv_schema = infer_table_schema(df, "students")

    result = compare_schemas(csv_schema, db_schema)

    assert result.is_match is False
    assert "Column count mismatch" in result.reason


def test_compare_schemas_detects_column_name_mismatch(conn):
    execute_non_query(
        conn,
        """
        CREATE TABLE students (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            first_name TEXT,
            last_name TEXT,
            age INTEGER
        );
        """,
    )

    db_schema = get_existing_schema(conn, "students")

    df = pd.DataFrame(
        {
            "First Name": ["Alice"],
            "Family Name": ["Johnson"],
            "Age": [20],
        }
    )
    csv_schema = infer_table_schema(df, "students")

    result = compare_schemas(csv_schema, db_schema)

    assert result.is_match is False
    assert "Column name mismatch" in result.reason
    assert "family_name" in result.reason


def test_compare_schemas_detects_column_type_mismatch(conn):
    execute_non_query(
        conn,
        """
        CREATE TABLE students (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            first_name TEXT,
            age TEXT
        );
        """,
    )

    db_schema = get_existing_schema(conn, "students")

    df = pd.DataFrame(
        {
            "First Name": ["Alice"],
            "Age": [20],
        }
    )
    csv_schema = infer_table_schema(df, "students")

    result = compare_schemas(csv_schema, db_schema)

    assert result.is_match is False
    assert "Column type mismatch" in result.reason
    assert "age" in result.reason


def test_should_append_to_existing_table_returns_true_for_matching_schema(conn):
    execute_non_query(
        conn,
        """
        CREATE TABLE people (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            first_name TEXT,
            last_name TEXT,
            age INTEGER
        );
        """,
    )

    df = pd.DataFrame(
        {
            "First Name": ["Alice"],
            "Last Name": ["Johnson"],
            "Age": [20],
        }
    )
    csv_schema = infer_table_schema(df, "new_import")

    should_append, matched_table = should_append_to_existing_table(conn, csv_schema)

    assert should_append is True
    assert matched_table == "people"


def test_should_append_to_existing_table_returns_false_when_no_match(conn):
    execute_non_query(
        conn,
        """
        CREATE TABLE people (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            first_name TEXT,
            age INTEGER
        );
        """,
    )

    df = pd.DataFrame(
        {
            "First Name": ["Alice"],
            "Last Name": ["Johnson"],
            "Age": [20],
        }
    )
    csv_schema = infer_table_schema(df, "new_import")

    should_append, matched_table = should_append_to_existing_table(conn, csv_schema)

    assert should_append is False
    assert matched_table == ""


def test_should_append_to_existing_table_checks_multiple_existing_tables(conn):
    execute_non_query(
        conn,
        """
        CREATE TABLE classes (
            class_id INTEGER PRIMARY KEY AUTOINCREMENT,
            class_name TEXT,
            instructor TEXT
        );
        """,
    )

    execute_non_query(
        conn,
        """
        CREATE TABLE employees (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            first_name TEXT,
            last_name TEXT,
            department TEXT
        );
        """,
    )

    execute_non_query(
        conn,
        """
        CREATE TABLE students (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            first_name TEXT,
            last_name TEXT,
            age INTEGER
        );
        """,
    )

    df = pd.DataFrame(
        {
            "First Name": ["Alice"],
            "Last Name": ["Johnson"],
            "Age": [20],
        }
    )
    csv_schema = infer_table_schema(df, "uploaded_file")

    should_append, matched_table = should_append_to_existing_table(conn, csv_schema)

    assert should_append is True
    assert matched_table == "students"