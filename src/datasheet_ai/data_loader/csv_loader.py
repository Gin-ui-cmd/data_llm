from pathlib import Path

import pandas as pd

from datasheet_ai.models import ColumnSchema, TableSchema


def read_csv_file(csv_path: str | Path) -> pd.DataFrame:
    """
    Read a CSV file into a pandas DataFrame.

    We do two basic checks here:
    - the file must exist
    - the file cannot be completely empty
    """
    path = Path(csv_path)
    if not path.exists():
        raise FileNotFoundError(f"CSV file does not exist: {path}")

    df = pd.read_csv(path)

    if df.empty and len(df.columns) == 0:
        raise ValueError(f"CSV file is empty: {path}")

    return df


def normalize_column_name(name: str) -> str:
    """
    Clean up one column name so it is easier to use in SQLite.

    Current normalization rules:
    - trim spaces
    - lowercase everything
    - turn spaces into underscores
    - turn hyphens into underscores
    """
    normalized = str(name).strip().lower()
    normalized = normalized.replace(" ", "_")
    normalized = normalized.replace("-", "_")
    return normalized


def normalize_dataframe_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Return a copy of the DataFrame with normalized column names.

    We keep the original DataFrame untouched and work on a copy instead.
    """
    df_copy = df.copy()
    df_copy.columns = [normalize_column_name(col) for col in df_copy.columns]
    return df_copy


def infer_sqlite_type(series: pd.Series) -> str:
    """
    Infer a SQLite type from one pandas Series.

    We only map into a small SQLite-friendly set:
    - INTEGER
    - REAL
    - TEXT
    """
    non_null = series.dropna()

    # If the whole column is empty, default to TEXT.
    if non_null.empty:
        return "TEXT"

    if pd.api.types.is_integer_dtype(non_null):
        return "INTEGER"

    if pd.api.types.is_float_dtype(non_null):
        return "REAL"

    # SQLite does not have a dedicated BOOLEAN type,
    # so booleans are stored as integers.
    if pd.api.types.is_bool_dtype(non_null):
        return "INTEGER"

    return "TEXT"


def infer_table_schema(df: pd.DataFrame, table_name: str) -> TableSchema:
    """
    Build a TableSchema object from a DataFrame.

    The DataFrame columns are normalized first, then each column
    gets a best-effort SQLite type.
    """
    normalized_df = normalize_dataframe_columns(df)

    columns: list[ColumnSchema] = []
    for col in normalized_df.columns:
        dtype = infer_sqlite_type(normalized_df[col])
        columns.append(ColumnSchema(name=col, dtype=dtype))

    return TableSchema(table_name=table_name, columns=columns)


def build_create_table_sql(schema: TableSchema) -> str:
    """
    Build a CREATE TABLE statement from a TableSchema.

    By design, every table also gets:
        id INTEGER PRIMARY KEY AUTOINCREMENT
    """
    column_defs = ["id INTEGER PRIMARY KEY AUTOINCREMENT"]

    for column in schema.columns:
        column_defs.append(f"{column.name} {column.dtype}")

    columns_sql = ",\n    ".join(column_defs)
    return f"CREATE TABLE {schema.table_name} (\n    {columns_sql}\n);"


def build_insert_sql(table_name: str, column_names: list[str]) -> str:
    """
    Build a parameterized INSERT statement.

    We use placeholders so later inserts can safely pass row values
    through executemany.
    """
    columns_sql = ", ".join(column_names)
    placeholders = ", ".join(["?"] * len(column_names))

    return (
        f"INSERT INTO {table_name} ({columns_sql}) "
        f"VALUES ({placeholders});"
    )


def dataframe_to_rows(df: pd.DataFrame) -> list[tuple]:
    """
    Convert a DataFrame into row tuples for SQLite insertion.

    One important detail here:
    pandas uses NaN, but SQLite expects None for NULL values,
    so we convert those before returning the rows.
    """
    normalized_df = normalize_dataframe_columns(df)

    # Switch to object dtype first, otherwise pandas may turn
    # None values back into NaN during the conversion.
    normalized_df = normalized_df.astype(object)
    normalized_df = normalized_df.where(pd.notna(normalized_df), None)

    return [tuple(row) for row in normalized_df.itertuples(index=False, name=None)]