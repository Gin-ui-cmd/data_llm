from pathlib import Path
#this code is generated with the help of AI
import pandas as pd
import pytest

from datasheet_ai.data_loader.csv_loader import (
    build_create_table_sql,
    build_insert_sql,
    dataframe_to_rows,
    infer_sqlite_type,
    infer_table_schema,
    normalize_column_name,
    normalize_dataframe_columns,
    read_csv_file,
)
from datasheet_ai.models import TableSchema


def create_csv(path: Path, data: dict) -> Path:
    df = pd.DataFrame(data)
    df.to_csv(path, index=False)
    return path


def test_read_csv_file_reads_valid_csv(tmp_path):
    csv_path = create_csv(
        tmp_path / "students.csv",
        {
            "First Name": ["Alice", "Bob"],
            "Age": [20, 22],
        },
    )

    df = read_csv_file(csv_path)

    assert list(df.columns) == ["First Name", "Age"]
    assert len(df) == 2


def test_read_csv_file_raises_when_file_missing(tmp_path):
    missing_path = tmp_path / "missing.csv"

    with pytest.raises(FileNotFoundError):
        read_csv_file(missing_path)


def test_normalize_column_name_basic_cases():
    assert normalize_column_name(" First Name ") == "first_name"
    assert normalize_column_name("Last-Name") == "last_name"
    assert normalize_column_name("AGE") == "age"


def test_normalize_dataframe_columns_returns_new_dataframe():
    df = pd.DataFrame(
        {
            "First Name": ["Alice"],
            "Last-Name": ["Johnson"],
            " Age ": [20],
        }
    )

    normalized_df = normalize_dataframe_columns(df)

    assert list(normalized_df.columns) == ["first_name", "last_name", "age"]
    # 原 df 不应被原地修改
    assert list(df.columns) == ["First Name", "Last-Name", " Age "]


def test_infer_sqlite_type_integer_series():
    series = pd.Series([1, 2, 3])
    assert infer_sqlite_type(series) == "INTEGER"


def test_infer_sqlite_type_float_series():
    series = pd.Series([1.5, 2.0, 3.8])
    assert infer_sqlite_type(series) == "REAL"


def test_infer_sqlite_type_text_series():
    series = pd.Series(["Alice", "Bob", "Charlie"])
    assert infer_sqlite_type(series) == "TEXT"


def test_infer_sqlite_type_bool_series():
    series = pd.Series([True, False, True])
    assert infer_sqlite_type(series) == "INTEGER"


def test_infer_sqlite_type_empty_non_null_defaults_to_text():
    series = pd.Series([None, None, None])
    assert infer_sqlite_type(series) == "TEXT"


def test_infer_table_schema_builds_expected_schema():
    df = pd.DataFrame(
        {
            "First Name": ["Alice", "Bob"],
            "Age": [20, 22],
            "GPA": [3.8, 3.5],
            "Major": ["CS", "Math"],
        }
    )

    schema = infer_table_schema(df, "students")

    assert isinstance(schema, TableSchema)
    assert schema.table_name == "students"
    assert [col.name for col in schema.columns] == ["first_name", "age", "gpa", "major"]
    assert [col.dtype for col in schema.columns] == ["TEXT", "INTEGER", "REAL", "TEXT"]


def test_build_create_table_sql_contains_id_and_columns():
    df = pd.DataFrame(
        {
            "First Name": ["Alice"],
            "Age": [20],
            "Major": ["CS"],
        }
    )
    schema = infer_table_schema(df, "students")

    sql = build_create_table_sql(schema)

    assert "CREATE TABLE students" in sql
    assert "id INTEGER PRIMARY KEY AUTOINCREMENT" in sql
    assert "first_name TEXT" in sql
    assert "age INTEGER" in sql
    assert "major TEXT" in sql
    assert sql.strip().endswith(");")


def test_build_insert_sql_builds_parameterized_insert():
    sql = build_insert_sql("students", ["first_name", "age", "major"])

    assert sql == "INSERT INTO students (first_name, age, major) VALUES (?, ?, ?);"


def test_dataframe_to_rows_normalizes_columns_and_converts_to_tuples():
    df = pd.DataFrame(
        {
            "First Name": ["Alice", "Bob"],
            "Age": [20, 22],
            "Major": ["CS", "Math"],
        }
    )

    rows = dataframe_to_rows(df)

    assert rows == [
        ("Alice", 20, "CS"),
        ("Bob", 22, "Math"),
    ]


def test_dataframe_to_rows_converts_nan_to_none():
    df = pd.DataFrame(
        {
            "First Name": ["Alice", "Bob"],
            "Age": [20, None],
            "Major": ["CS", "Math"],
        }
    )

    rows = dataframe_to_rows(df)

    assert rows[0] == ("Alice", 20.0, "CS")
    assert rows[1] == ("Bob", None, "Math")