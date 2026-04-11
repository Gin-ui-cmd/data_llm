from pathlib import Path
from typing import Optional

from datasheet_ai.config import DEFAULT_DB_PATH, ERROR_LOG_PATH, ensure_project_dirs
from datasheet_ai.data_loader.csv_loader import (
    build_create_table_sql,
    build_insert_sql,
    dataframe_to_rows,
    infer_table_schema,
    normalize_column_name,
    normalize_dataframe_columns,
    read_csv_file,
)
from datasheet_ai.db.connection import get_connection
from datasheet_ai.db.sqlite_setup import (
    execute_many,
    execute_non_query,
    execute_query,
    get_table_schema,
    list_tables,
    table_exists,
)
from datasheet_ai.models import CSVLoadResult, GeneratedSQL, QueryResult
from datasheet_ai.schema_manager.schema_manager import (
    compare_schemas,
    should_append_to_existing_table,
)
from datasheet_ai.validator.sql_validator import validate_select_query


class QueryService:
    """
    Main application service for database-related workflows.

    This class sits between the CLI / upper layer and the low-level DB helpers.
    It coordinates CSV loading, schema inspection, SQL validation, SQL execution,
    and the optional natural-language-to-SQL flow.
    """

    def __init__(self, db_path: str | Path = DEFAULT_DB_PATH, llm_adapter=None):
        self.db_path = Path(db_path)
        self.llm_adapter = llm_adapter
        ensure_project_dirs()

    def list_tables(self) -> list[str]:
        """
        Return all user-defined table names in the current database.
        """
        with get_connection(self.db_path) as conn:
            return list_tables(conn)

    def get_table_schema_text(self, table_name: str) -> str:
        """
        Return a readable text version of one table schema.

        This is mainly useful for CLI output or debugging.
        """
        try:
            with get_connection(self.db_path) as conn:
                schema = get_table_schema(conn, table_name)

            lines = [f"Table: {schema.table_name}"]
            for column in schema.columns:
                lines.append(f"- {column.name}: {column.dtype}")

            return "\n".join(lines)

        except Exception as exc:
            self._log_error(f"Failed to get schema for table '{table_name}': {exc}")
            return f"Error: {exc}"

    def execute_user_sql(self, sql: str) -> QueryResult:
        """
        Validate a user-written SQL query first, then execute it.

        Only SELECT statements are allowed to pass through.
        """
        try:
            with get_connection(self.db_path) as conn:
                validation = validate_select_query(conn, sql)
                if not validation.is_valid:
                    return QueryResult(
                        success=False,
                        error_message=validation.error_message,
                        executed_sql=validation.normalized_sql,
                    )

                columns, rows = execute_query(conn, validation.normalized_sql)

                return QueryResult(
                    success=True,
                    columns=columns,
                    rows=rows,
                    error_message="",
                    executed_sql=validation.normalized_sql,
                )

        except Exception as exc:
            self._log_error(f"Failed to execute SQL '{sql}': {exc}")
            return QueryResult(
                success=False,
                error_message=str(exc),
                executed_sql=sql,
            )

    def load_csv(self, csv_path: str | Path, table_name: str | None = None) -> CSVLoadResult:
        """
        Load one CSV file into SQLite.

        MVP behavior:
        - read the CSV
        - normalize column names
        - infer the schema
        - decide whether to append or create a table
        """
        try:
            csv_file_path = Path(csv_path)
            df = read_csv_file(csv_file_path)
            df = normalize_dataframe_columns(df)

            # If the caller gives a table name, use it after normalization.
            # Otherwise derive one from the CSV filename.
            target_table_name = (
                normalize_column_name(table_name)
                if table_name
                else self._derive_table_name(csv_file_path)
            )

            csv_schema = infer_table_schema(df, target_table_name)
            rows = dataframe_to_rows(df)

            if not rows:
                return CSVLoadResult(
                    success=False,
                    table_name=target_table_name,
                    inserted_rows=0,
                    created_new_table=False,
                    error_message="CSV contains no data rows.",
                )

            with get_connection(self.db_path) as conn:
                if table_name:
                    return self._load_csv_with_explicit_table_name(
                        conn=conn,
                        csv_schema=csv_schema,
                        rows=rows,
                        requested_table_name=target_table_name,
                    )

                return self._load_csv_with_auto_resolution(
                    conn=conn,
                    csv_schema=csv_schema,
                    rows=rows,
                    fallback_table_name=target_table_name,
                )

        except Exception as exc:
            self._log_error(f"Failed to load CSV '{csv_path}': {exc}")
            return CSVLoadResult(
                success=False,
                table_name=table_name or "",
                inserted_rows=0,
                created_new_table=False,
                error_message=str(exc),
            )

    def ask_natural_language(self, question: str) -> QueryResult:
        """
        Turn a natural-language question into SQL, then validate and run it.

        Important safety rule:
        - the LLM only generates SQL text
        - the generated SQL is still treated as untrusted input
        """
        if self.llm_adapter is None:
            return QueryResult(
                success=False,
                error_message="LLM adapter is not configured.",
            )

        try:
            schema_context = self._build_schema_context()
            generated: GeneratedSQL = self.llm_adapter.generate_sql(question, schema_context)

            if not generated.success:
                return QueryResult(
                    success=False,
                    error_message=generated.error_message or "Failed to generate SQL.",
                )

            return self.execute_user_sql(generated.sql)

        except Exception as exc:
            self._log_error(f"Failed natural-language query '{question}': {exc}")
            return QueryResult(
                success=False,
                error_message=str(exc),
            )

    def _load_csv_with_explicit_table_name(
        self,
        conn,
        csv_schema,
        rows: list[tuple],
        requested_table_name: str,
    ) -> CSVLoadResult:
        """
        Handle the CSV load flow when the caller explicitly gives a table name.

        In this mode:
        - append if the table already exists and the schema matches
        - create a new table if it does not exist
        - fail if the table exists but the schema does not match
        """
        if table_exists(conn, requested_table_name):
            db_schema = get_table_schema(conn, requested_table_name)
            match_result = compare_schemas(csv_schema, db_schema)

            if not match_result.is_match:
                return CSVLoadResult(
                    success=False,
                    table_name=requested_table_name,
                    inserted_rows=0,
                    created_new_table=False,
                    error_message=(
                        f"Schema mismatch for existing table '{requested_table_name}': "
                        f"{match_result.reason}"
                    ),
                )

            insert_sql = build_insert_sql(
                requested_table_name,
                [column.name for column in csv_schema.columns],
            )
            execute_many(conn, insert_sql, rows)

            return CSVLoadResult(
                success=True,
                table_name=requested_table_name,
                inserted_rows=len(rows),
                created_new_table=False,
                error_message="",
            )

        # If the table does not exist yet, create it first and then insert data.
        create_sql = build_create_table_sql(csv_schema)
        execute_non_query(conn, create_sql)

        insert_sql = build_insert_sql(
            requested_table_name,
            [column.name for column in csv_schema.columns],
        )
        execute_many(conn, insert_sql, rows)

        return CSVLoadResult(
            success=True,
            table_name=requested_table_name,
            inserted_rows=len(rows),
            created_new_table=True,
            error_message="",
        )

    def _load_csv_with_auto_resolution(
        self,
        conn,
        csv_schema,
        rows: list[tuple],
        fallback_table_name: str,
    ) -> CSVLoadResult:
        """
        Handle the CSV load flow when no table name is explicitly provided.

        In this mode:
        - try to append to an existing compatible table
        - otherwise fall back to a derived table name
        - create a new table if needed
        """
        should_append, matched_table_name = should_append_to_existing_table(conn, csv_schema)

        if should_append:
            insert_sql = build_insert_sql(
                matched_table_name,
                [column.name for column in csv_schema.columns],
            )
            execute_many(conn, insert_sql, rows)

            return CSVLoadResult(
                success=True,
                table_name=matched_table_name,
                inserted_rows=len(rows),
                created_new_table=False,
                error_message="",
            )

        if table_exists(conn, fallback_table_name):
            db_schema = get_table_schema(conn, fallback_table_name)
            match_result = compare_schemas(csv_schema, db_schema)

            if not match_result.is_match:
                return CSVLoadResult(
                    success=False,
                    table_name=fallback_table_name,
                    inserted_rows=0,
                    created_new_table=False,
                    error_message=(
                        f"Derived table name '{fallback_table_name}' already exists, "
                        f"but schema does not match: {match_result.reason}"
                    ),
                )

            insert_sql = build_insert_sql(
                fallback_table_name,
                [column.name for column in csv_schema.columns],
            )
            execute_many(conn, insert_sql, rows)

            return CSVLoadResult(
                success=True,
                table_name=fallback_table_name,
                inserted_rows=len(rows),
                created_new_table=False,
                error_message="",
            )

        create_sql = build_create_table_sql(csv_schema)
        execute_non_query(conn, create_sql)

        insert_sql = build_insert_sql(
            fallback_table_name,
            [column.name for column in csv_schema.columns],
        )
        execute_many(conn, insert_sql, rows)

        return CSVLoadResult(
            success=True,
            table_name=fallback_table_name,
            inserted_rows=len(rows),
            created_new_table=True,
            error_message="",
        )

    def _derive_table_name(self, csv_path: Path) -> str:
        """
        Build a normalized table name from the CSV filename.
        """
        return normalize_column_name(csv_path.stem)

    def _build_schema_context(self) -> str:
        """
        Build a plain-text summary of all current table schemas.

        This is passed to the LLM so it knows what tables and columns exist.
        """
        table_texts: list[str] = []

        with get_connection(self.db_path) as conn:
            for table_name in list_tables(conn):
                schema = get_table_schema(conn, table_name)

                lines = [f"Table: {schema.table_name}"]
                for column in schema.columns:
                    lines.append(f"  - {column.name}: {column.dtype}")

                table_texts.append("\n".join(lines))

        return "\n\n".join(table_texts)

    def _log_error(self, message: str) -> None:
        """
        Append one error line to the project error log file.
        """
        ERROR_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        with ERROR_LOG_PATH.open("a", encoding="utf-8") as f:
            f.write(message + "\n")