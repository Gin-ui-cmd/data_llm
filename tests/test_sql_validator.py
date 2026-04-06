import pytest

from datasheet_ai.db.connection import get_connection
from datasheet_ai.db.sqlite_setup import execute_non_query
from datasheet_ai.validator.sql_validator import (
    contains_disallowed_keywords,
    extract_selected_columns,
    extract_table_names,
    has_multiple_statements,
    has_sql_comments,
    is_select_only,
    normalize_sql,
    validate_columns_exist,
    validate_query_structure,
    validate_select_query,
    validate_tables_exist,
)


@pytest.fixture
def conn(tmp_path):
    db_path = tmp_path / "test_validator.db"
    connection = get_connection(db_path)

    execute_non_query(
        connection,
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
        connection,
        """
        CREATE TABLE classes (
            class_id INTEGER PRIMARY KEY AUTOINCREMENT,
            class_name TEXT,
            instructor TEXT,
            credits INTEGER
        );
        """,
    )

    yield connection
    connection.close()


def test_normalize_sql():
    sql = "  SELECT   first_name,   age   FROM   students   ;  "
    normalized = normalize_sql(sql)
    assert normalized == "SELECT first_name, age FROM students ;"


def test_has_sql_comments_detects_line_comment():
    sql = "SELECT * FROM students -- comment"
    assert has_sql_comments(sql) is True


def test_has_sql_comments_detects_block_comment():
    sql = "SELECT /* hidden */ * FROM students"
    assert has_sql_comments(sql) is True


def test_has_multiple_statements_detects_two_statements():
    sql = "SELECT * FROM students; DROP TABLE students;"
    assert has_multiple_statements(sql) is True


def test_has_multiple_statements_allows_single_trailing_semicolon():
    sql = "SELECT * FROM students;"
    assert has_multiple_statements(sql) is False


def test_is_select_only_accepts_valid_select():
    assert is_select_only("SELECT * FROM students") is True


def test_is_select_only_rejects_delete():
    assert is_select_only("DELETE FROM students") is False


def test_contains_disallowed_keywords_detects_create():
    sql = "SELECT * FROM students CREATE TABLE bad_table (id INTEGER)"
    assert contains_disallowed_keywords(sql) is True


def test_validate_query_structure_accepts_simple_select():
    result = validate_query_structure("SELECT first_name, age FROM students;")
    assert result.is_valid is True
    assert result.normalized_sql == "SELECT first_name, age FROM students;"


def test_validate_query_structure_rejects_empty_sql():
    result = validate_query_structure("   ")
    assert result.is_valid is False
    assert result.error_message == "SQL query is empty."


def test_validate_query_structure_rejects_non_select():
    result = validate_query_structure("DELETE FROM students WHERE id = 1;")
    assert result.is_valid is False
    assert result.error_message == "Only SELECT queries are allowed."


def test_validate_query_structure_rejects_comments():
    result = validate_query_structure("SELECT * FROM students -- hidden")
    assert result.is_valid is False
    assert result.error_message == "SQL comments are not allowed."


def test_validate_query_structure_rejects_multiple_statements():
    result = validate_query_structure("SELECT * FROM students; DROP TABLE students;")
    assert result.is_valid is False
    assert result.error_message == "Multiple SQL statements are not allowed."


def test_extract_table_names_simple_from():
    sql = "SELECT first_name FROM students"
    assert extract_table_names(sql) == ["students"]


def test_extract_table_names_from_join():
    sql = """
    SELECT students.first_name, classes.class_name
    FROM students
    JOIN classes ON students.id = classes.class_id
    """
    assert extract_table_names(sql) == ["students", "classes"]


def test_extract_selected_columns_simple():
    sql = "SELECT first_name, age FROM students"
    assert extract_selected_columns(sql) == ["first_name", "age"]


def test_extract_selected_columns_with_qualified_names():
    sql = "SELECT students.first_name, students.age FROM students"
    assert extract_selected_columns(sql) == ["students.first_name", "students.age"]


def test_extract_selected_columns_with_wildcard():
    sql = "SELECT * FROM students"
    assert extract_selected_columns(sql) == ["*"]


def test_extract_selected_columns_with_table_wildcard():
    sql = "SELECT students.* FROM students"
    assert extract_selected_columns(sql) == ["students.*"]


def test_validate_tables_exist_accepts_existing_table(conn):
    result = validate_tables_exist(conn, ["students"])
    assert result.is_valid is True


def test_validate_tables_exist_rejects_missing_table(conn):
    result = validate_tables_exist(conn, ["unknown_table"])
    assert result.is_valid is False
    assert "unknown_table" in result.error_message


def test_validate_columns_exist_accepts_simple_columns(conn):
    result = validate_columns_exist(conn, ["students"], ["first_name", "age"])
    assert result.is_valid is True


def test_validate_columns_exist_accepts_star(conn):
    result = validate_columns_exist(conn, ["students"], ["*"])
    assert result.is_valid is True


def test_validate_columns_exist_accepts_table_star(conn):
    result = validate_columns_exist(conn, ["students"], ["students.*"])
    assert result.is_valid is True


def test_validate_columns_exist_accepts_qualified_column(conn):
    result = validate_columns_exist(conn, ["students"], ["students.first_name"])
    assert result.is_valid is True


def test_validate_columns_exist_rejects_missing_column(conn):
    result = validate_columns_exist(conn, ["students"], ["full_name"])
    assert result.is_valid is False
    assert "full_name" in result.error_message


def test_validate_columns_exist_rejects_missing_qualified_column(conn):
    result = validate_columns_exist(conn, ["students"], ["students.full_name"])
    assert result.is_valid is False
    assert "students.full_name" in result.error_message


def test_validate_select_query_accepts_valid_query(conn):
    sql = "SELECT first_name, age FROM students;"
    result = validate_select_query(conn, sql)

    assert result.is_valid is True
    assert result.normalized_sql == "SELECT first_name, age FROM students;"


def test_validate_select_query_rejects_unknown_table(conn):
    sql = "SELECT first_name FROM unknown_table;"
    result = validate_select_query(conn, sql)

    assert result.is_valid is False
    assert "unknown_table" in result.error_message


def test_validate_select_query_rejects_unknown_column(conn):
    sql = "SELECT full_name FROM students;"
    result = validate_select_query(conn, sql)

    assert result.is_valid is False
    assert "full_name" in result.error_message


def test_validate_select_query_accepts_join_query(conn):
    sql = """
    SELECT students.first_name, classes.class_name
    FROM students
    JOIN classes ON students.id = classes.class_id;
    """
    result = validate_select_query(conn, sql)

    assert result.is_valid is True


def test_validate_select_query_rejects_llm_wrong_table(conn):
    sql = "SELECT * FROM unknown_table;"
    result = validate_select_query(conn, sql)

    assert result.is_valid is False
    assert "unknown_table" in result.error_message