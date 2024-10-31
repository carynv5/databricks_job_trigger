# Databricks Deployment Tool

This tool automates the end-to-end process of deploying Python packages and jobs to Databricks workspaces. It handles the complete deployment pipeline: building Python wheel packages locally, uploading them to Databricks File System (DBFS), creating or updating job configurations, and managing job execution. The tool is particularly useful for data engineering teams who need to maintain consistent deployment processes across multiple Databricks jobs and packages. When you run the tool, it executes the following steps:

1. Validates your Databricks connection and environment setup
2. Lists available clusters and verifies cluster accessibility
3. Builds your Python package into a wheel file
4. Creates necessary DBFS directories
5. Uploads the wheel file and any requirements to DBFS
6. Deploys additional bundle files to the Databricks workspace
7. Creates or updates the Databricks job configuration
8. Initiates the job with specified parameters (if requested)

The setup process is straightforward: after cloning the repository, you'll need to configure your Databricks credentials in a `.env` file, install the required dependencies using pip, and install the package in editable mode using uv. This creates a development installation that allows you to modify the source code without reinstalling the package.

## Features

- 🏗️ Automated wheel package building
- 📤 DBFS file upload management
- 🔄 Job creation and updates
- 🚀 Bundle deployment automation
- 📋 Cluster management and validation
- ⚡ Direct job execution capability

## Prerequisites

- Python 3.x
- Access to a Databricks workspace
- Databricks CLI configured or environment variables set
- `python-build` package installed

## Environment Setup

Create a `.env` file in the project root with the following variables:

```
DATABRICKS_WORKSPACE_URL=<your-workspace-url>
DATABRICKS_ACCESS_TOKEN=<your-access-token>
```

## Installation

1. Clone this repository
2. Create and activate a virtual environment, then install the package:
   ```bash
   uv venv --python 3.12
   source .venv/bin/activate
   uv pip install -e .
   uv sync
   ```

## Usage

### Basic Deployment

```python
from databricks_manager import DatabricksManager

dbx = DatabricksManager()
dbx.deploy_bundle("path/to/your/bundle")
```

### Running a Job

```python
parameters = {
    "date": "2024-03-01",
    "region": "NA"
}

dbx.run_job(
    job_name="Survey Processing Job",
    parameters=parameters
)
```

## Project Structure

Your Databricks bundle should follow this structure:

```
your_bundle/
├── databricks.yml     # Job configuration
├── requirements.txt   # Package dependencies
├── pyproject.toml     # Package configuration
├── setup.py           # Package setup file
└── src/               # Source code
```

It's best practice to maintain a second virtual env for the bundle being deployed:
```bash
uv venv --python 3.12
source .venv/bin/activate
uv pip install -e .
uv sync
```

This then allows you to dump all pyproject.toml requirements to a requirements.txt:
```bash
uv pip compile pyproject.toml -o requirements.txt
```

The included `db_sp_handler` folder in this repository is an example Databricks bundle that processes survey data. You can use this as a template or replace it entirely with your own bundle. To use your own bundle:

1. Ensure your bundle follows the structure above
2. Replace the bundle path in the deployment command:
   ```python
   # Instead of using the example bundle:
   # dbx.deploy_bundle("db_sp_handler")
   
   # Use your own bundle:
   dbx.deploy_bundle("path/to/your/bundle")
   ```

## Configuration

### databricks.yml Example

```yaml
resources:
  jobs:
    survey_job:
      name: "Survey Processing Job"
      existing_cluster_id: "your-cluster-id"
      email_notifications:
        on_success:
          - "success@example.com"
        on_failure:
          - "failure@example.com"
```

## Key Features Explained

### Wheel Building
The tool automatically builds your Python package into a wheel file for deployment:
```python
dbx.build_wheel(bundle_path)
```

### DBFS Upload
Handles uploading of wheel files and other artifacts to DBFS:
```python
dbx.upload_wheel(wheel_path)
```

### Job Management
Creates or updates Databricks jobs based on your configuration:
```python
dbx.create_or_update_job(bundle_path, wheel_path)
```

## Error Handling

The tool includes comprehensive error handling and logging:
- Connection validation
- Environment variable verification
- Deployment status checks
- Job creation/update validation

## Best Practices

1. Always validate your cluster status before deployment:
   ```python
   dbx.validate_cluster(cluster_id)
   ```

2. Test your connection before performing operations:
   ```python
   dbx.test_connection(dbx.client)
   ```

3. Keep your bundle structure organized and consistent

## Troubleshooting

Common issues and solutions:

1. **Connection Failed**
   - Verify your environment variables
   - Check your Databricks token permissions

2. **Wheel Build Failed**
   - Ensure your `setup.py` is properly configured
   - Check for missing dependencies

3. **Job Creation Failed**
   - Validate your `databricks.yml` format
   - Confirm cluster availability

## Contributing

1. Fork the repository
2. Create a feature branch
3. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.