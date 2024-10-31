# DBFS Finder

A simple utility to find files in Databricks File System (DBFS).

## Installation

```bash
pip install -e .
```

## Usage

```bash
# Using environment variables
dbfs-finder example.py

# With command line arguments
dbfs-finder example.py --workspace-url https://your-workspace.cloud.databricks.com --token your_access_token

# Quick search only
dbfs-finder example.py --quick
```

## Development

```bash
# Install development dependencies
pip install -e ".[dev]"

# Run tests
pytest

# Format code
black .
isort .

# Type checking
mypy .
```

## Environment Variables

Create a `.env` file with:

```
DATABRICKS_WORKSPACE_URL=https://your-workspace.cloud.databricks.com
DATABRICKS_ACCESS_TOKEN=your_access_token
```
