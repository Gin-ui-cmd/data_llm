import re
import sqlite3

from datasheet_ai.db.sqlite_setup import get_table_schema, list_tables
from datasheet_ai.models import ValidationResult


DISALLOWED_KEYWORDS = {
    "INSERT",
    "UPDATE",
    "DELETE",
    "DROP",
    "ALTER",
    "CREATE",
    "PRAGMA",
    "ATTACH",
    "DETACH",
    "REPLACE",
    "TRUNCATE",
}


_IDENTIFIER_PATTERN = r"[A-Za-z_][A-Za-z0-9_]*"


def normalize_sql(sql: str) -> str:
    """
    Normalize whitespace and strip leading/trailing spaces.
    """
    return re.sub(r"\s+", " ", sql.strip())


def has_sql_comments(sql: str) -> bool:
    """
    Reject SQL comments to reduce injection and parsing ambiguity.

    This validator rejects:
    - line comments: --
    - block comments: /* ... */
    """
    return "--" in sql or "/*" in sql or "*/" in sql


def has_multiple_statements(sql: str) -> bool:
    """
    Reject multiple SQL statements separated by semicolons.

    A single trailing semicolon is allowed.
    """
    stripped = sql.strip()
    if not stripped:
        return False

    if stripped.endswith(";"):
        stripped = stripped[:-1].rstrip()

    return ";" in stripped


def is_select_only(sql: str) -> bool:
    """
    Return True only if the SQL begins with SELECT.
    """
    normalized = normalize_sql(sql).upper()
    return normalized.startswith("SELECT ")


def contains_disallowed_keywords(sql: str) -> bool:
    """
    Reject dangerous SQL keywords anywhere in the statement.
    """
    normalized = normalize_sql(sql).upper()
    for keyword in DISALLOWED_KEYWORDS:
        if re.search(rf"\b{keyword}\b", normalized):
            return True
    return False


def validate_query_structure(sql: str) -> ValidationResult:
    """
    Perform structure-level validation.

    Checks:
    - non-empty
    - no comments
    - no multi-statement SQL
    - SELECT only
    - no disallowed keywords
    """
    normalized = normalize_sql(sql)

    if not normalized:
        return ValidationResult(
            is_valid=False,
            normalized_sql="",
            error_message="SQL query is empty.",
        )

    if has_sql_comments(normalized):
        return ValidationResult(
            is_valid=False,
            normalized_sql=normalized,
            error_message="SQL comments are not allowed.",
        )

    if has_multiple_statements(normalized):
        return ValidationResult(
            is_valid=False,
            normalized_sql=normalized,
            error_message="Multiple SQL statements are not allowed.",
        )

    if not is_select_only(normalized):
        return ValidationResult(
            is_valid=False,
            normalized_sql=normalized,
            error_message="Only SELECT queries are allowed.",
        )

    if contains_disallowed_keywords(normalized):
        return ValidationResult(
            is_valid=False,
            normalized_sql=normalized,
            error_message="This query contains disallowed SQL keywords.",
        )

    return ValidationResult(
        is_valid=True,
        normalized_sql=normalized,
        error_message="",
    )


def extract_table_names(sql: str) -> list[str]:
    """
    Extract referenced table names from FROM and JOIN clauses.

    MVP limitations:
    - supports simple SELECT ... FROM table ...
    - supports JOIN table ...
    - does not fully parse nested subqueries
    """
    normalized = normalize_sql(sql)
    pattern = re.compile(
        rf"\b(?:FROM|JOIN)\s+({_IDENTIFIER_PATTERN})\b",
        re.IGNORECASE,
    )
    matches = pattern.findall(normalized)

    seen: list[str] = []
    for name in matches:
        if name not in seen:
            seen.append(name)
    return seen


def extract_selected_columns(sql: str) -> list[str]:
    """
    Extract selected column expressions between SELECT and FROM.

    Returns simplified raw column tokens, for example:
    - "*"
    - "students.*"
    - "name"
    - "students.name"

    MVP behavior:
    - ignores expressions like COUNT(*), age + 1, CASE ...
    - removes aliases introduced by AS
    """
    normalized = normalize_sql(sql)

    match = re.search(
        r"\bSELECT\s+(.*?)\s+\bFROM\b",
        normalized,
        re.IGNORECASE,
    )
    if not match:
        return []

    select_part = match.group(1).strip()

    if select_part.upper().startswith("DISTINCT "):
        select_part = select_part[9:].strip()

    raw_items = [item.strip() for item in select_part.split(",") if item.strip()]
    columns: list[str] = []

    for item in raw_items:
        # Remove trailing alias: "col AS alias"
        item = re.sub(r"\s+AS\s+\w+$", "", item, flags=re.IGNORECASE).strip()

        # Remove trailing bare alias: "col alias"
        if " " in item and "(" not in item and ")" not in item:
            item = item.split()[0].strip()

        if item == "*":
            columns.append("*")
            continue

        if re.fullmatch(rf"{_IDENTIFIER_PATTERN}\.\*", item):
            columns.append(item)
            continue

        if re.fullmatch(rf"{_IDENTIFIER_PATTERN}\.{_IDENTIFIER_PATTERN}", item):
            columns.append(item)
            continue

        if re.fullmatch(rf"{_IDENTIFIER_PATTERN}", item):
            columns.append(item)
            continue

        # For MVP, skip validating computed expressions/functions.
        # They are allowed to pass structure validation, but only simple
        # identifiers are validated at the column level.
        continue

    return columns


def validate_tables_exist(
    conn: sqlite3.Connection,
    table_names: list[str],
) -> ValidationResult:
    """
    Check whether all referenced tables exist.
    """
    if not table_names:
        return ValidationResult(
            is_valid=False,
            error_message="No table name could be extracted from the query.",
        )

    existing_tables = list_tables(conn)
    existing_lookup = {name.lower(): name for name in existing_tables}

    missing_tables = [name for name in table_names if name.lower() not in existing_lookup]
    if missing_tables:
        return ValidationResult(
            is_valid=False,
            error_message=f"Referenced table(s) do not exist: {', '.join(missing_tables)}",
        )

    return ValidationResult(is_valid=True)


def validate_columns_exist(
    conn: sqlite3.Connection,
    table_names: list[str],
    selected_columns: list[str],
) -> ValidationResult:
    """
    Check whether selected columns exist in referenced tables.

    MVP rules:
    - '*' is always allowed
    - 'table.*' is allowed if the table exists
    - 'table.column' must exist in that table
    - 'column' must exist in at least one referenced table
    - computed expressions are ignored upstream and are not validated here
    """
    if not selected_columns:
        return ValidationResult(
            is_valid=True,
            error_message="",
        )

    existing_tables = list_tables(conn)
    table_lookup = {name.lower(): name for name in existing_tables}

    schema_lookup: dict[str, set[str]] = {}
    for table_name in table_names:
        actual_table_name = table_lookup[table_name.lower()]
        schema = get_table_schema(conn, actual_table_name)
        schema_lookup[actual_table_name.lower()] = {col.name.lower() for col in schema.columns}

    all_columns = set()
    for cols in schema_lookup.values():
        all_columns.update(cols)

    for token in selected_columns:
        if token == "*":
            continue

        if re.fullmatch(rf"{_IDENTIFIER_PATTERN}\.\*", token):
            table_name = token.split(".", 1)[0].lower()
            if table_name not in schema_lookup:
                return ValidationResult(
                    is_valid=False,
                    error_message=f"Referenced table does not exist for wildcard selection: {token}",
                )
            continue

        if re.fullmatch(rf"{_IDENTIFIER_PATTERN}\.{_IDENTIFIER_PATTERN}", token):
            table_name, column_name = token.split(".", 1)
            table_name = table_name.lower()
            column_name = column_name.lower()

            if table_name not in schema_lookup:
                return ValidationResult(
                    is_valid=False,
                    error_message=f"Referenced table does not exist: {table_name}",
                )

            if column_name not in schema_lookup[table_name]:
                return ValidationResult(
                    is_valid=False,
                    error_message=f"Referenced column does not exist: {token}",
                )
            continue

        column_name = token.lower()
        if column_name not in all_columns:
            return ValidationResult(
                is_valid=False,
                error_message=f"Referenced column does not exist: {token}",
            )

    return ValidationResult(is_valid=True)


def validate_select_query(conn: sqlite3.Connection, sql: str) -> ValidationResult:
    """
    Full validator entrypoint.

    Validation flow:
    1. structure validation
    2. table existence validation
    3. column existence validation
    """
    structure_result = validate_query_structure(sql)
    if not structure_result.is_valid:
        return structure_result

    normalized_sql = structure_result.normalized_sql
    table_names = extract_table_names(normalized_sql)

    tables_result = validate_tables_exist(conn, table_names)
    if not tables_result.is_valid:
        return ValidationResult(
            is_valid=False,
            normalized_sql=normalized_sql,
            error_message=tables_result.error_message,
        )

    selected_columns = extract_selected_columns(normalized_sql)
    columns_result = validate_columns_exist(conn, table_names, selected_columns)
    if not columns_result.is_valid:
        return ValidationResult(
            is_valid=False,
            normalized_sql=normalized_sql,
            error_message=columns_result.error_message,
        )

    return ValidationResult(
        is_valid=True,
        normalized_sql=normalized_sql,
        error_message="",
    )