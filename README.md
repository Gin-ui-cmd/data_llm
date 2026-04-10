# Data_LLM

A modular Python project that loads structured CSV data into SQLite, validates SQL queries, and supports optional LLM-assisted natural language querying.

## Project Goal

The goal of this project is to build a small but structured system that:

- loads structured data into a SQL database
- allows users to query data
- optionally uses an LLM to translate natural language into SQL
- keeps the system correct and safe even when the LLM is wrong

This project uses SQLite as the database and Python as the main implementation language.

## Key Design Idea

The main design idea is separation of concerns.

The CLI does not access the database directly.  
All requests go through the Query Service.  
The LLM Adapter never executes SQL.  
All generated SQL must pass through the SQL Validator before execution.

This means the system can remain correct even if the LLM produces bad SQL.

## Project Structure

```text
data_llm/
├─ .github/
│  └─ workflows/
│     └─ ci.yml
├─ data/
│  ├─ databases/
│  │  └─ app.db
│  └─ sample_csv/
├─ src/
│  └─ datasheet_ai/
│     ├─ cli.py
│     ├─ config.py
│     ├─ models.py
│     ├─ db/
│     ├─ data_loader/
│     ├─ schema_manager/
│     ├─ query_service/
│     ├─ validator/
│     └─ llm/
├─ tests/
├─ README.md
├─ requirements.txt
└─ pyproject.toml
````

## Main Components

### 1. CSV Loader

This module reads CSV files using pandas, normalizes column names, infers SQLite data types, and builds SQL statements manually.

Important: `df.to_sql()` is not used.

### 2. Schema Manager

This module inspects existing database tables and compares schemas.
If a CSV matches an existing table, data can be appended.
If not, a new table can be created.

### 3. SQL Validator

This module protects the database by validating user SQL and LLM-generated SQL.

At the current stage, it:

* only allows `SELECT` queries
* rejects multiple SQL statements
* rejects dangerous keywords like `DELETE`, `DROP`, `UPDATE`
* rejects queries with unknown tables
* rejects queries with unknown columns

### 4. Query Service

This is the main service layer of the system.

It handles:

* CSV loading
* schema inspection
* SQL validation
* SQL execution
* natural language querying through the LLM Adapter

### 5. LLM Adapter

This module converts natural language into SQL.

Right now, it is a stub version for MVP testing.
It does not execute SQL.
It only returns SQL, which must still be validated.

### 6. CLI

This is the command-line interface for the user.

Supported commands include:

* `tables`
* `schema <table_name>`
* `load <csv_path>`
* `sql`
* `ask`
* `exit`

## Why the System Is Safe

A key requirement of this project is:

> The system must remain correct even when the LLM is wrong.

This is handled by design.

The LLM output is treated as untrusted input.
Even if the LLM generates invalid SQL, the SQL Validator checks it before execution.

For example, when the user asks something the stub cannot understand, the LLM Adapter may generate:

```sql
SELECT * FROM unknown_table;
```

This query is rejected by the validator because the table does not exist.

So the system stays correct and safe.

## Installation

Create and activate a virtual environment first.

### Git Bash on Windows

```bash
python -m venv .venv
source .venv/Scripts/activate
pip install -e .[dev]
```

## Running the CLI

```bash
export PYTHONPATH=src
python -m datasheet_ai.cli
```

## Running Tests

```bash
pytest -q
```

## Example CLI Usage

### List all tables

```text
tables
```

### Show schema

```text
schema students
```

### Load CSV

```text
load data/sample_csv/students.csv
```

### Run SQL

```text
sql
SELECT * FROM students;
```

### Ask in natural language

```text
ask
show all students
```

## Example of LLM Failure Being Handled Correctly

User input:

```text
ask
something the stub cannot understand
```

Stub-generated SQL:

```sql
SELECT * FROM unknown_table;
```

System response:

```text
Error: Referenced table(s) do not exist: unknown_table
```

This demonstrates that the validator catches the issue and prevents invalid execution.

## Testing Status

The project currently includes unit tests for:

* CSV Loader
* Schema Manager
* SQL Validator
* Query Service

All tests pass locally.

## CI

This project includes a GitHub Actions workflow that installs dependencies and runs pytest automatically on push and pull request.

