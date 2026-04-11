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
    Clean up the SQL a bit so later checks are easier.

    We just squeeze repeated whitespace into single spaces
    and trim the string at both ends.
    """
    return re.sub(r"\s+", " ", sql.strip())


def has_sql_comments(sql: str) -> bool:
    """
    Block SQL comments to keep things simpler and safer.

    This catches:
    - line comments like: --
    - block comments like: /* ... */
    """
    return "--" in sql or "/*" in sql or "*/" in sql


def has_multiple_statements(sql: str) -> bool:
    """
    Reject multi-statement SQL.

    One trailing semicolon is fine, but anything more than that
    means the query likely contains multiple statements.
    """
    stripped = sql.strip()
    if not stripped:
        return False

    if stripped.endswith(";"):
        stripped = stripped[:-1].rstrip()

    return ";" in stripped


def is_select_only(sql: str) -> bool:
    """
    Only allow queries that start with SELECT.
    """
    normalized = normalize_sql(sql).upper()
    return normalized.startswith("SELECT ")


def contains_disallowed_keywords(sql: str) -> bool:
    """
    Check whether the SQL contains any blocked keywords.
    """
    normalized = normalize_sql(sql).upper()

    for keyword in DISALLOWED_KEYWORDS:
        if re.search(rf"\b{keyword}\b", normalized):
            return True

    return False


def validate_query_structure(sql: str) -> ValidationResult:
    """
    Run the basic structure checks first.

    What we check here:
    - the query is not empty
    - no SQL comments
    - no multiple statements
    - SELECT only
    - no dangerous keywords
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
    Pull table names from FROM and JOIN clauses.

    Current MVP scope:
    - handles simple SELECT ... FROM table ...
    - handles JOIN table ...
    - does not try to fully parse nested subqueries
    """
    normalized = normalize_sql(sql)

    pattern = re.compile(
        rf"\b(?:FROM|JOIN)\s+({_IDENTIFIER_PATTERN})\b",
        re.IGNORECASE,
    )
    matches = pattern.findall(normalized)

    # Keep original order and remove duplicates.
    seen: list[str] = []
    for name in matches:
        if name not in seen:
            seen.append(name)

    return seen


def extract_selected_columns(sql: str) -> list[str]:
    """
    Pull out the selected column tokens between SELECT and FROM.

    Examples of what we keep:
    - "*"
    - "students.*"
    - "name"
    - "students.name"

    MVP notes:
    - expressions like COUNT(*), age + 1, CASE ... are ignored here
    - aliases added with AS are stripped off
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
        # Example: "col AS alias" -> "col"
        item = re.sub(r"\s+AS\s+\w+$", "", item, flags=re.IGNORECASE).strip()

        # Example: "col alias" -> "col"
        # Only do this for simple identifiers, not function calls.
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

        # For MVP, computed expressions and function calls are allowed
        # to get past structure validation, but we skip column-level
        # validation for them here.
        continue

    return columns


def validate_tables_exist(
    conn: sqlite3.Connection,
    table_names: list[str],
) -> ValidationResult:
    """
    Make sure every referenced table actually exists.
    """
    if not table_names:
        return ValidationResult(
            is_valid=False,
            error_message="No table name could be extracted from the query.",
        )

    existing_tables = list_tables(conn)
    existing_lookup = {name.lower(): name for name in existing_tables}

    missing_tables = [
        name for name in table_names
        if name.lower() not in existing_lookup
    ]

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
    Check whether the selected columns exist in the referenced tables.

    MVP rules:
    - '*' is always okay
    - 'table.*' is okay if the table exists
    - 'table.column' must exist in that table
    - 'column' must exist in at least one referenced table
    - computed expressions are skipped before this step
    """
    if not selected_columns:
        return ValidationResult(
            is_valid=True,
            error_message="",
        )

    existing_tables = list_tables(conn)
    table_lookup = {name.lower(): name for name in existing_tables}

    # Build a quick lowercase schema map for the tables involved.
    schema_lookup: dict[str, set[str]] = {}
    for table_name in table_names:
        actual_table_name = table_lookup[table_name.lower()]
        schema = get_table_schema(conn, actual_table_name)
        schema_lookup[actual_table_name.lower()] = {
            col.name.lower()
            for col in schema.columns
        }

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
    Main entry point for validating a SELECT query.

    Validation flow:
    1. structure check
    2. table existence check
    3. column existence check
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