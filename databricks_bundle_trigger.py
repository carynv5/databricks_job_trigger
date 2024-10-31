import os
import json
import base64
import yaml
import requests
import traceback
import subprocess
from typing import Dict, Any, Optional
from dotenv import load_dotenv
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ImportFormat
from databricks.sdk.service.jobs import JobSettings, PythonWheelTask, Task

class DatabricksManager:
    def __init__(self):
        self.client = WorkspaceClient()
    
    def list_clusters(self):
        """
        Lists all clusters and their IDs
        """
        try:
            clusters = self.client.clusters.list()
            print("\nAvailable clusters:")
            for cluster in clusters:
                print(f"- {cluster.cluster_name}: {cluster.cluster_id}")
                print(f"  State: {cluster.state}")
                print(f"  URL: {os.getenv('DATABRICKS_WORKSPACE_URL')}/#cluster/{cluster.cluster_id}")
        except Exception as e:
            print(f"Failed to list clusters: {str(e)}")
            raise

    def validate_cluster(self, cluster_id: str) -> bool:
        """
        Validates that a cluster exists and is running
        """
        try:
            cluster = self.client.clusters.get(cluster_id=cluster_id)
            print(f"\nCluster status: {cluster.state}")
            return cluster.state == "RUNNING"
        except Exception as e:
            print(f"Failed to validate cluster: {str(e)}")
            return False

    def build_wheel(self, bundle_path: str) -> str:
        """
        Builds the Python wheel package
        """
        try:
            print("\nBuilding wheel package...")
            subprocess.run(
                ["python", "-m", "build", "--wheel"],
                cwd=bundle_path,
                check=True
            )
            
            # Find the built wheel file
            dist_dir = os.path.join(bundle_path, "dist")
            wheel_files = [f for f in os.listdir(dist_dir) if f.endswith('.whl')]
            if not wheel_files:
                raise FileNotFoundError("No wheel file found after build")
                
            wheel_path = os.path.join(dist_dir, wheel_files[0])
            print(f"Wheel built successfully: {wheel_files[0]}")
            return wheel_path
            
        except Exception as e:
            print(f"Failed to build wheel: {str(e)}")
            raise

    def upload_wheel(self, wheel_path: str) -> str:
        """
        Uploads the wheel file to DBFS
        """
        try:
            wheel_name = os.path.basename(wheel_path)
            dbfs_path = f"/FileStore/jars/{wheel_name}"
            
            print(f"\nUploading wheel to DBFS: {dbfs_path}")
            
            # Make sure the directory exists
            try:
                self.client.dbfs.mkdirs(os.path.dirname(dbfs_path))
            except Exception:
                # Directory might already exist
                pass
                
            # Read and upload the wheel file in chunks
            chunk_size = 1024 * 1024  # 1MB chunks
            with open(wheel_path, 'rb') as f:
                handle = self.client.dbfs.create(
                    path=dbfs_path,
                    overwrite=True
                )
                
                while True:
                    chunk = f.read(chunk_size)
                    if not chunk:
                        break
                    
                    # Add block to DBFS
                    self.client.dbfs.add_block(
                        handle=handle.handle,
                        data=base64.b64encode(chunk).decode()
                    )
                
                # Close the handle
                self.client.dbfs.close(handle=handle.handle)
            
            # Verify the upload
            try:
                file_info = self.client.dbfs.get_status(dbfs_path)
                print(f"Wheel uploaded successfully to: dbfs:{dbfs_path}")
                print(f"File size: {file_info.file_size} bytes")
            except Exception as e:
                print(f"Warning: Could not verify upload: {str(e)}")
            
            # Return the proper DBFS path format for job configuration
            return f"dbfs:{dbfs_path}"
            
        except Exception as e:
            print(f"Failed to upload wheel: {str(e)}")
            print(f"Full error details: {traceback.format_exc()}")
            raise

    def deploy_bundle(self, bundle_path: str) -> None:
        """
        Deploys a Databricks bundle to the workspace
        """
        try:
            # Directories to ignore
            ignore_dirs = {'.git', '__pycache__', 'dist', 'build', '.pytest_cache', '.venv'}
            # File patterns to ignore
            ignore_patterns = {'.pyc', '.pyo', '.pyd', '.so', '.dylib', '.DS_Store'}
            
            wheel_path = self.build_wheel(bundle_path)
            dbfs_wheel_path = self.upload_wheel(wheel_path)
            
            workspace_path = f"/Shared/bundles/{os.path.basename(bundle_path)}"
            created_dirs = set()
            
            print("\nDeploying bundle files...")
            for root, dirs, files in os.walk(bundle_path):
                # Remove ignored directories
                dirs[:] = [d for d in dirs if d not in ignore_dirs]
                
                for file in files:
                    # Skip ignored files
                    if any(file.endswith(pat) for pat in ignore_patterns):
                        continue
                        
                    local_path = os.path.join(root, file)
                    relative_path = os.path.relpath(local_path, bundle_path)
                    
                    # Skip files in ignored directories
                    if any(part in ignore_dirs for part in relative_path.split(os.sep)):
                        continue
                        
                    target_path = os.path.join(workspace_path, relative_path)
                    
                    parent_dir = os.path.dirname(target_path)
                    if parent_dir not in created_dirs:
                        print(f"Creating directory: {parent_dir}")
                        self.client.workspace.mkdirs(parent_dir)
                        created_dirs.add(parent_dir)
                    
                    print(f"Uploading {relative_path} to {target_path}")
                    with open(local_path, 'rb') as f:
                        content = f.read()
                    
                    self.client.workspace.upload(
                        path=target_path,
                        content=content,
                        format=ImportFormat.AUTO,
                        overwrite=True
                    )
            
            print(f"\nBundle deployed successfully to {workspace_path}")
            
            # Create or update the job
            print("\nCreating/updating job configuration...")
            self.create_or_update_job(bundle_path, dbfs_wheel_path)
        except Exception as e:
            print(f"Failed to deploy bundle: {str(e)}")
            raise

    def create_or_update_job(self, bundle_path: str, wheel_path: str) -> None:
        """
        Creates or updates the job based on databricks.yml configuration using REST API
        """
        try:
            print("\nConfiguring job...")
            yml_path = os.path.join(bundle_path, "databricks.yml")
            print(f"Reading configuration from: {yml_path}")
            
            with open(yml_path, 'r') as f:
                config = yaml.safe_load(f)
                
            job_config = config['resources']['jobs']['survey_job']

            # Upload requirements.txt to DBFS if it exists
            requirements_path = os.path.join(bundle_path, "requirements.txt")
            dbfs_requirements = None
            if os.path.exists(requirements_path):
                dbfs_requirements_path = f"/FileStore/job-requirements/{os.path.basename(bundle_path)}/requirements.txt"
                try:
                    self.client.dbfs.mkdirs(os.path.dirname(dbfs_requirements_path))
                except:
                    pass

                with open(requirements_path, 'rb') as f:
                    handle = self.client.dbfs.create(
                        path=dbfs_requirements_path,
                        overwrite=True
                    )
                    
                    self.client.dbfs.add_block(
                        handle=handle.handle,
                        data=base64.b64encode(f.read()).decode()
                    )
                    
                    self.client.dbfs.close(handle=handle.handle)
                    
                    dbfs_requirements = f"dbfs:{dbfs_requirements_path}"
                    print(f"Requirements uploaded to: {dbfs_requirements}")

            # Create job settings dict
            job_settings = {
                "name": job_config['name'],
                "tasks": [{
                    "task_key": "process_surveys",
                    "python_wheel_task": {
                        "package_name": "survey_processing",
                        "entry_point": "main"
                    },
                    "existing_cluster_id": job_config['existing_cluster_id'],
                    "libraries": [
                        {"whl": wheel_path}
                        # Temporarily remove pip entry for testing
                        # {"pip": [dbfs_requirements]}
                    ]
                }],
                "email_notifications": job_config['email_notifications'],
                "schedule": job_config['schedule'],
                "max_concurrent_runs": 1
            }

            # Debug: Print libraries before filtering
            print("Libraries before filtering:", job_settings["tasks"][0]["libraries"])

            # Remove any empty library entries
            job_settings["tasks"][0]["libraries"] = [
                lib for lib in job_settings["tasks"][0]["libraries"] if lib
            ]

            # Debug: Print libraries after filtering
            print("Libraries after filtering:", job_settings["tasks"][0]["libraries"])

            # Manually serialize job settings to JSON
            serialized_job_settings = json.dumps(job_settings, indent=2)
            print("Serialized job settings:", serialized_job_settings)

            # Set up API headers
            headers = {
                "Authorization": f"Bearer {os.getenv('DATABRICKS_TOKEN')}",
                "Content-Type": "application/json"
            }
            base_url = os.getenv('DATABRICKS_WORKSPACE_URL').rstrip('/')

            # Check if job exists
            print("\nChecking for existing job...")
            existing_job = None
            response = requests.get(
                f"{base_url}/api/2.1/jobs/list",
                headers=headers
            )
            response.raise_for_status()

            for job in response.json().get('jobs', []):
                if job['settings']['name'] == job_config['name']:
                    existing_job = job
                    break

            if existing_job:
                print(f"Updating existing job: {job_config['name']} (ID: {existing_job['job_id']})")
                update_response = requests.post(
                    f"{base_url}/api/2.1/jobs/update",
                    headers=headers,
                    json={
                        "job_id": existing_job['job_id'],
                        "new_settings": job_settings  # Use manually constructed JSON here
                    }
                )
                update_response.raise_for_status()
                print("Job updated successfully")
            else:
                print(f"Creating new job: {job_config['name']}")
                create_response = requests.post(
                    f"{base_url}/api/2.1/jobs/create",
                    headers=headers,
                    json=job_settings  # Use manually constructed JSON here
                )
                try:
                    create_response.raise_for_status()
                except requests.exceptions.HTTPError as e:
                    print(f"Error response: {create_response.text}")
                    raise
                result = create_response.json()
                print(f"Created job with ID: {result.get('job_id')}")
            
        except Exception as e:
            print(f"Failed to create/update job: {str(e)}")
            print(f"Full error details: {traceback.format_exc()}")
            raise

    def run_job(self, job_name: str, parameters: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        """
        Runs a job by name
        """
        try:
            print(f"\nPreparing to run job: {job_name}")
            # Find the job
            job_id = None
            for job in self.client.jobs.list():
                if job.settings.name == job_name:
                    job_id = job.job_id
                    break
            
            if not job_id:
                raise ValueError(f"Job '{job_name}' not found in workspace")
            
            print(f"Found job ID: {job_id} for job: {job_name}")
            
            # Run the job with notebook parameters instead of named_parameters
            run = self.client.jobs.run_now(
                job_id=job_id,
                notebook_params=parameters  # Changed from named_parameters to notebook_params
            )
            
            job_url = f"{os.getenv('DATABRICKS_WORKSPACE_URL')}/#job/{run.run_id}"
            print(f"Job started successfully! Monitor at: {job_url}")
            
            return {"run_id": run.run_id}
            
        except Exception as e:
            print(f"Failed to run job: {str(e)}")
            raise

def validate_environment():
    """
    Validates that all required environment variables are set
    """
    required_vars = ['DATABRICKS_WORKSPACE_URL', 'DATABRICKS_ACCESS_TOKEN']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")
        
    # Set SDK environment variables
    os.environ['DATABRICKS_HOST'] = os.getenv('DATABRICKS_WORKSPACE_URL')
    os.environ['DATABRICKS_TOKEN'] = os.getenv('DATABRICKS_ACCESS_TOKEN')

def test_connection(client: WorkspaceClient) -> bool:
    """
    Tests the connection to Databricks
    """
    try:
        client.workspace.list('/')
        print("Successfully connected to Databricks!")
        return True
    except Exception as e:
        print(f"Failed to connect to Databricks: {str(e)}")
        return False

if __name__ == "__main__":
    # Load environment variables
    load_dotenv()
    
    try:
        # Validate environment
        validate_environment()
        
        # Set up paths
        script_dir = os.path.dirname(os.path.abspath(__file__))
        BUNDLE_PATH = os.path.join(script_dir, "db_sp_handler")
        
        print(f"Starting deployment...")
        print(f"Bundle path: {BUNDLE_PATH}")
        
        # Initialize manager
        dbx = DatabricksManager()
        
        # Test connection
        if not test_connection(dbx.client):
            raise Exception("Failed to connect to Databricks")
        
        # List available clusters
        print("\nListing available clusters...")
        dbx.list_clusters()
        
        # Deploy bundle
        dbx.deploy_bundle(BUNDLE_PATH)
        
        # Set up job parameters
        job_params = {
            "date": "2024-03-01",
            "region": "NA"
        }
        
        print(f"\nRunning job with parameters: {json.dumps(job_params, indent=2)}")
        
        # Run the job
        result = dbx.run_job(
            job_name="Survey Processing Job",
            parameters=job_params
        )
        
        print(f"\nProcess completed successfully!")
        
    except Exception as e:
        print(f"\nOperation failed: {str(e)}")
        import traceback
        print(f"Full traceback:\n{traceback.format_exc()}")