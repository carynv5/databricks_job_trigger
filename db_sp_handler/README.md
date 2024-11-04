# Survey Processing Package

This package provides tools for processing survey data using Databricks. It is designed to be deployed as a wheel package and run as a Databricks job.

## Project Structure
```
db_sp_handler/
├── MANIFEST.in
├── README.md
├── databricks.yml    # Databricks bundle configuration
├── pyproject.toml    # Python package configuration
├── requirements.txt
├── survey_processing/
│   ├── __init__.py
│   └── main.py      # Main entry point
└── dist/            # Built wheel packages
```

## Build Steps

### 1. Package Configuration
Ensure your `pyproject.toml` is properly configured:
```toml
[build-system]
requires = ["setuptools>=61.0"]
build-backend = "setuptools.build_meta"

[project]
name = "survey_processing"
version = "0.1.2"
description = "Survey Processing Package"
requires-python = ">=3.8"
dependencies = [
    # Add your dependencies here
]

[project.scripts]
survey-processing = "survey_processing.main:main"

[tool.setuptools.packages.find]
where = ["."]
include = ["survey_processing*"]
```

### 2. Building the Wheel
Clean any existing builds and create a new wheel:
```bash
# Clean previous builds
rm -rf dist/ build/ *.egg-info/

# Build new wheel
python -m build
```

This will create two files in the `dist/` directory:
- `survey_processing-0.1.2.tar.gz` (source distribution)
- `survey_processing-0.1.2-py3-none-any.whl` (wheel distribution)

## Databricks Deployment

### 1. Bundle Configuration
Configure your `databricks.yml`:
```yaml
bundle:
  name: "survey-processing-bundle"

resources:
  jobs:
    survey_processing_job:
      name: "Survey Processing Job"
      job_clusters:
        - job_cluster_key: "main"
          new_cluster:
            spark_version: "13.3.x-scala2.12"
            node_type_id: "i3.xlarge"
            num_workers: 1
      tasks:
        - task_key: "main_task"
          job_cluster_key: "main"
          python_wheel_task:
            package_name: "survey_processing"
            entry_point: "main"
            named_parameters:
              date: "2024-11-05"
              region: "EU"
          libraries:
            - whl: "dist/survey_processing-0.1.2-py3-none-any.whl"
```

### 2. Deploy the Bundle
Deploy your package to Databricks:
```bash
databricks bundle deploy
```

To deploy with debug information:
```bash
databricks bundle deploy --verbose
```

## Running Jobs

### 1. Basic Job Run
Run the job with default parameters:
```bash
databricks bundle run survey_processing_job
```

### 2. Run with Custom Parameters
Run the job with specific parameters:
```bash
databricks bundle run survey_processing_job \
  --python-named-params "date=2024-11-05,region=EU"
```

### 3. Available CLI Options
```bash
databricks bundle run [flags] KEY

Flags:
  --python-named-params stringToString   Named parameters for Python wheel tasks
  --debug                               Enable debug logging
  -o, --output type                     Output type: text or json
  -t, --target string                   Bundle target to use
  --no-wait                             Don't wait for run completion
```

## Monitoring and Logs

### Viewing Job Runs
1. Access the Databricks workspace
2. Navigate to the Jobs page
3. Find your job run
4. Click on the task name to view:
   - Output tab: Script stdout/stderr and logging
   - Logs tab: Cluster and Spark logs
   - Results tab: Task results and metrics

### Log Location
Your job's logs can be found in several places:
1. Task Output: Contains your Python script's stdout/stderr
2. Driver Logs: Contains Spark driver logs
3. Cluster Logs: Contains cluster-level logs

## Troubleshooting

### Common Issues
1. **ImportError or ModuleNotFoundError**
   - Check that all dependencies are listed in `pyproject.toml`
   - Verify the wheel file contains all necessary files
   - Check the package structure matches the imports

2. **Parameter Errors**
   - Verify parameter names match between CLI and script
   - Check parameter formatting in the CLI command
   - Ensure required parameters are provided

3. **Build Issues**
   - Clean old builds before creating new ones
   - Check `pyproject.toml` configuration
   - Verify package structure is correct

### Debug Steps
1. Build with clean environment:
   ```bash
   rm -rf dist/ build/ *.egg-info/
   python -m build
   ```

2. Test locally before deployment:
   ```bash
   pip install dist/survey_processing-0.1.2-py3-none-any.whl
   survey-processing --date 2024-11-05 --region EU
   ```

3. Deploy with verbose logging:
   ```bash
   databricks bundle deploy --verbose
   ```

## Development Tips

### Local Testing
Test your package locally before deployment:
```bash
# Install in editable mode
pip install -e .

# Run with test parameters
survey-processing --date 2024-11-05 --region EU
```

### Version Updates
1. Update version in `pyproject.toml`
2. Rebuild the wheel
3. Update the wheel path in `databricks.yml`
4. Redeploy the bundle

### Adding Dependencies
1. Add to `pyproject.toml` dependencies
2. Rebuild the wheel
3. Redeploy the bundle