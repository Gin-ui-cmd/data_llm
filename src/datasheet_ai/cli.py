from pathlib import Path

from datasheet_ai.config import DEFAULT_DB_PATH, ensure_project_dirs
from datasheet_ai.llm.llm_adapter import LLMAdapter
from datasheet_ai.query_service.query_service import QueryService


def print_menu() -> None:
    """
    Print the available CLI commands.
    """
    print("\n=== Datasheet AI CLI ===")
    print("1. tables                  -> list all tables")
    print("2. schema <table_name>     -> show schema for a table")
    print("3. load <csv_path>         -> load CSV, auto-resolve table")
    print("4. load <csv_path> <name>  -> load CSV into a specific table")
    print("5. sql                     -> enter a SQL query")
    print("6. ask                     -> enter a natural-language question")
    print("7. help                    -> show this menu")
    print("8. exit                    -> quit")


def _print_query_result(result) -> None:
    """
    Pretty-print a QueryResult.
    """
    if not result.success:
        print(f"Error: {result.error_message}")
        if result.executed_sql:
            print(f"SQL: {result.executed_sql}")
        return

    if result.executed_sql:
        print(f"SQL: {result.executed_sql}")

    if not result.columns:
        print("Query executed successfully, but no columns were returned.")
        return

    print(" | ".join(result.columns))
    print("-" * (len(" | ".join(result.columns))))

    if not result.rows:
        print("(no rows)")
        return

    for row in result.rows:
        print(" | ".join(str(value) if value is not None else "NULL" for value in row))


def _handle_load_command(service: QueryService, command: str) -> None:
    """
    Handle CSV load commands.

    Supported forms:
    - load <csv_path>
    - load <csv_path> <table_name>
    """
    parts = command.split(maxsplit=2)

    if len(parts) < 2:
        print("Usage: load <csv_path> [table_name]")
        return

    csv_path = parts[1]
    table_name = parts[2] if len(parts) == 3 else None

    result = service.load_csv(csv_path=csv_path, table_name=table_name)

    if result.success:
        action = "created new table" if result.created_new_table else "appended to existing table"
        print(
            f"Success: loaded {result.inserted_rows} row(s) into '{result.table_name}' "
            f"({action})."
        )
    else:
        print(f"Error: {result.error_message}")


def main() -> None:
    """
    CLI entrypoint.

    CLI must not access the database directly.
    All user actions go through QueryService.
    """
    ensure_project_dirs()

    llm_adapter = LLMAdapter()
    service = QueryService(db_path=DEFAULT_DB_PATH, llm_adapter=llm_adapter)

    print(f"Using database: {Path(DEFAULT_DB_PATH).resolve()}")
    print_menu()

    while True:
        try:
            command = input("\nEnter command: ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nExiting.")
            break

        if not command:
            continue

        lowered = command.lower()

        if lowered == "exit":
            print("Goodbye.")
            break

        if lowered == "help":
            print_menu()
            continue

        if lowered == "tables":
            tables = service.list_tables()
            if not tables:
                print("(no tables)")
            else:
                for table in tables:
                    print(table)
            continue

        if lowered.startswith("schema "):
            parts = command.split(maxsplit=1)
            if len(parts) != 2 or not parts[1].strip():
                print("Usage: schema <table_name>")
                continue

            table_name = parts[1].strip()
            print(service.get_table_schema_text(table_name))
            continue

        if lowered.startswith("load "):
            _handle_load_command(service, command)
            continue

        if lowered == "sql":
            sql = input("Enter SELECT query: ").strip()
            result = service.execute_user_sql(sql)
            _print_query_result(result)
            continue

        if lowered == "ask":
            question = input("Ask a question: ").strip()
            result = service.ask_natural_language(question)
            _print_query_result(result)
            continue

        print("Unknown command. Type 'help' to see available commands.")


if __name__ == "__main__":
    main()