from pathlib import Path

import pandas as pd

from datasheet_ai.models import ColumnSchema, TableSchema


def read_csv_file(csv_path: str | Path) -> pd.DataFrame:
    """
    Read a CSV file into a pandas DataFrame.

    Parameters
    ----------
    csv_path : str | Path
        Path to the CSV file.

    Returns
    -------
    pd.DataFrame
        Loaded DataFrame.

    Raises
    ------
    FileNotFoundError
        If the CSV file does not exist.
    ValueError
        If the CSV file is empty.
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
    Normalize a single column name.

    Rules
    -----
    - strip leading/trailing spaces
    - lowercase
    - replace spaces with underscores
    - replace hyphens with underscores

    Parameters
    ----------
    name : str
        Original column name.

    Returns
    -------
    str
        Normalized column name.
    """
    normalized = str(name).strip().lower()
    normalized = normalized.replace(" ", "_")
    normalized = normalized.replace("-", "_")
    return normalized


def normalize_dataframe_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Return a copy of DataFrame with normalized column names.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame.

    Returns
    -------
    pd.DataFrame
        DataFrame with normalized column names.
    """
    df_copy = df.copy()
    df_copy.columns = [normalize_column_name(col) for col in df_copy.columns]
    return df_copy


def infer_sqlite_type(series: pd.Series) -> str:
    """
    Infer SQLite type from a pandas Series.

    Returns one of:
    - INTEGER
    - REAL
    - TEXT
    """
    non_null = series.dropna()

    if non_null.empty:
        return "TEXT"

    if pd.api.types.is_integer_dtype(non_null):
        return "INTEGER"

    if pd.api.types.is_float_dtype(non_null):
        return "REAL"

    if pd.api.types.is_bool_dtype(non_null):
        return "INTEGER"

    return "TEXT"


def infer_table_schema(df: pd.DataFrame, table_name: str) -> TableSchema:
    """
    Infer a TableSchema from a DataFrame.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame.
    table_name : str
        Target table name.

    Returns
    -------
    TableSchema
        Structured schema for the table.
    """
    normalized_df = normalize_dataframe_columns(df)

    columns: list[ColumnSchema] = []
    for col in normalized_df.columns:
        dtype = infer_sqlite_type(normalized_df[col])
        columns.append(ColumnSchema(name=col, dtype=dtype))

    return TableSchema(table_name=table_name, columns=columns)


def build_create_table_sql(schema: TableSchema) -> str:
    """
    Build a CREATE TABLE SQL statement.

    The table always includes:
        id INTEGER PRIMARY KEY AUTOINCREMENT

    Parameters
    ----------
    schema : TableSchema
        Target schema.

    Returns
    -------
    str
        CREATE TABLE statement.
    """
    column_defs = ["id INTEGER PRIMARY KEY AUTOINCREMENT"]

    for column in schema.columns:
        column_defs.append(f"{column.name} {column.dtype}")

    columns_sql = ",\n    ".join(column_defs)
    return f"CREATE TABLE {schema.table_name} (\n    {columns_sql}\n);"


def build_insert_sql(table_name: str, column_names: list[str]) -> str:
    """
    Build a parameterized INSERT SQL statement.

    Parameters
    ----------
    table_name : str
        Target table name.
    column_names : list[str]
        Ordered list of column names.

    Returns
    -------
    str
        INSERT statement with placeholders.
    """
    columns_sql = ", ".join(column_names)
    placeholders = ", ".join(["?"] * len(column_names))

    return (
        f"INSERT INTO {table_name} ({columns_sql}) "
        f"VALUES ({placeholders});"
    )


def dataframe_to_rows(df: pd.DataFrame) -> list[tuple]:
    """
    Convert a DataFrame into a list of row tuples suitable for executemany.

    NaN values are converted to None so SQLite can store them as NULL.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame.

    Returns
    -------
    list[tuple]
        Row tuples.
    """
    normalized_df = normalize_dataframe_columns(df)

    # Convert to object dtype first, otherwise pandas may coerce None back to NaN
    normalized_df = normalized_df.astype(object)
    normalized_df = normalized_df.where(pd.notna(normalized_df), None)

    return [tuple(row) for row in normalized_df.itertuples(index=False, name=None)]