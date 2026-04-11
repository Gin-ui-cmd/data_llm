from pathlib import Path
#this code is generated with the help of AI
import pandas as pd
import pytest

from datasheet_ai.db.connection import get_connection
from datasheet_ai.db.sqlite_setup import execute_non_query
from datasheet_ai.llm.llm_adapter import LLMAdapter
from datasheet_ai.query_service.query_service import QueryService


@pytest.fixture
def db_path(tmp_path):
    return tmp_path / "query_service.db"


@pytest.fixture
def service(db_path):
    return QueryService(db_path=db_path, llm_adapter=LLMAdapter())


def create_csv(path: Path, data: dict) -> Path:
    df = pd.DataFrame(data)
    df.to_csv(path, index=False)
    return path


def seed_students_table(db_path: Path) -> None:
    with get_connection(db_path) as conn:
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
            INSERT INTO students (first_name, last_name, age, major)
            VALUES
                ('Alice', 'Johnson', 20, 'Computer Science'),
                ('Bob', 'Smith', 22, 'Mathematics');
            """,
        )


def seed_students_and_classes_tables(db_path: Path) -> None:
    with get_connection(db_path) as conn:
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
            INSERT INTO students (first_name, last_name, age, major)
            VALUES
                ('Alice', 'Johnson', 20, 'Computer Science'),
                ('Bob', 'Smith', 22, 'Mathematics');
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

        execute_non_query(
            conn,
            """
            INSERT INTO classes (class_name, instructor, credits)
            VALUES
                ('Database Systems', 'Prof. Lee', 4),
                ('Operating Systems', 'Prof. Wang', 3);
            """,
        )


def test_list_tables_returns_existing_tables(service, db_path):
    seed_students_and_classes_tables(db_path)

    tables = service.list_tables()

    assert "students" in tables
    assert "classes" in tables


def test_get_table_schema_text_returns_human_readable_schema(service, db_path):
    seed_students_table(db_path)

    schema_text = service.get_table_schema_text("students")

    assert "Table: students" in schema_text
    assert "- first_name: TEXT" in schema_text
    assert "- age: INTEGER" in schema_text


def test_get_table_schema_text_returns_error_for_missing_table(service):
    schema_text = service.get_table_schema_text("missing_table")

    assert schema_text.startswith("Error:")


def test_execute_user_sql_runs_valid_select(service, db_path):
    seed_students_table(db_path)

    result = service.execute_user_sql("SELECT first_name, age FROM students;")

    assert result.success is True
    assert result.columns == ["first_name", "age"]
    assert len(result.rows) == 2
    assert result.executed_sql == "SELECT first_name, age FROM students;"


def test_execute_user_sql_rejects_non_select(service, db_path):
    seed_students_table(db_path)

    result = service.execute_user_sql("DELETE FROM students WHERE id = 1;")

    assert result.success is False
    assert result.error_message == "Only SELECT queries are allowed."


def test_execute_user_sql_rejects_unknown_table(service, db_path):
    seed_students_table(db_path)

    result = service.execute_user_sql("SELECT * FROM unknown_table;")

    assert result.success is False
    assert "unknown_table" in result.error_message


def test_load_csv_creates_new_table_when_explicit_name_not_exists(service, tmp_path):
    csv_path = create_csv(
        tmp_path / "employees.csv",
        {
            "First Name": ["John", "Betty"],
            "Last Name": ["Doe", "Smith"],
            "Age": [31, 28],
            "Department": ["Sales", "HR"],
        },
    )

    result = service.load_csv(csv_path=csv_path, table_name="employees")

    assert result.success is True
    assert result.table_name == "employees"
    assert result.inserted_rows == 2
    assert result.created_new_table is True

    query_result = service.execute_user_sql("SELECT first_name, last_name, age, department FROM employees;")
    assert query_result.success is True
    assert len(query_result.rows) == 2


def test_load_csv_appends_to_existing_table_when_explicit_name_matches_schema(service, db_path, tmp_path):
    with get_connection(db_path) as conn:
        execute_non_query(
            conn,
            """
            CREATE TABLE employees (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                first_name TEXT,
                last_name TEXT,
                age INTEGER,
                department TEXT
            );
            """,
        )

        execute_non_query(
            conn,
            """
            INSERT INTO employees (first_name, last_name, age, department)
            VALUES ('Alice', 'Johnson', 20, 'Engineering');
            """,
        )

    csv_path = create_csv(
        tmp_path / "more_employees.csv",
        {
            "First Name": ["Bob"],
            "Last Name": ["Smith"],
            "Age": [22],
            "Department": ["Finance"],
        },
    )

    result = service.load_csv(csv_path=csv_path, table_name="employees")

    assert result.success is True
    assert result.table_name == "employees"
    assert result.inserted_rows == 1
    assert result.created_new_table is False

    query_result = service.execute_user_sql("SELECT * FROM employees;")
    assert query_result.success is True
    assert len(query_result.rows) == 2


def test_load_csv_returns_error_when_explicit_name_schema_mismatch(service, db_path, tmp_path):
    with get_connection(db_path) as conn:
        execute_non_query(
            conn,
            """
            CREATE TABLE employees (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                first_name TEXT,
                age INTEGER
            );
            """,
        )

    csv_path = create_csv(
        tmp_path / "employees.csv",
        {
            "First Name": ["John"],
            "Last Name": ["Doe"],
            "Age": [31],
        },
    )

    result = service.load_csv(csv_path=csv_path, table_name="employees")

    assert result.success is False
    assert "Schema mismatch" in result.error_message


def test_load_csv_auto_creates_table_from_csv_stem_when_no_match_exists(service, tmp_path):
    csv_path = create_csv(
        tmp_path / "customer_data.csv",
        {
            "First Name": ["John", "Betty"],
            "Last Name": ["Doe", "Doe"],
            "Age": [31, 28],
        },
    )

    result = service.load_csv(csv_path=csv_path)

    assert result.success is True
    assert result.table_name == "customer_data"
    assert result.inserted_rows == 2
    assert result.created_new_table is True

    query_result = service.execute_user_sql("SELECT first_name, last_name, age FROM customer_data;")
    assert query_result.success is True
    assert len(query_result.rows) == 2


def test_load_csv_auto_appends_to_matching_existing_table(service, db_path, tmp_path):
    with get_connection(db_path) as conn:
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

        execute_non_query(
            conn,
            """
            INSERT INTO people (first_name, last_name, age)
            VALUES ('Alice', 'Johnson', 20);
            """,
        )

    csv_path = create_csv(
        tmp_path / "new_import.csv",
        {
            "First Name": ["Bob"],
            "Last Name": ["Smith"],
            "Age": [22],
        },
    )

    result = service.load_csv(csv_path=csv_path)

    assert result.success is True
    assert result.table_name == "people"
    assert result.inserted_rows == 1
    assert result.created_new_table is False

    query_result = service.execute_user_sql("SELECT first_name, last_name, age FROM people;")
    assert query_result.success is True
    assert len(query_result.rows) == 2


def test_ask_natural_language_executes_valid_generated_sql(service, db_path):
    seed_students_table(db_path)

    result = service.ask_natural_language("show all students")

    assert result.success is True
    assert "first_name" in result.columns
    assert len(result.rows) == 2
    assert result.executed_sql == "SELECT * FROM students;"


def test_ask_natural_language_remains_safe_when_llm_is_wrong(service, db_path):
    seed_students_table(db_path)

    result = service.ask_natural_language("something the stub cannot understand")

    assert result.success is False
    assert "unknown_table" in result.error_message


def test_ask_natural_language_returns_error_when_llm_adapter_missing(db_path):
    service_without_llm = QueryService(db_path=db_path, llm_adapter=None)

    result = service_without_llm.ask_natural_language("show all students")

    assert result.success is False
    assert result.error_message == "LLM adapter is not configured."