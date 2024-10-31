import os
import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

workspace_url = os.getenv('DATABRICKS_WORKSPACE_URL')
token = os.getenv('DATABRICKS_ACCESS_TOKEN')
api_path = "/api/2.1/jobs/run-now"

headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json"
}

# Define your parameters
job_params = {
    "date": "2024-03-01",
    "region": "NA"
}

data = {
    "job_id": 433278282343760,  # Corrected job ID
    "python_params": [
        "--date", "2024-03-01",
        "--region", "NA"
    ]
}

response = requests.post(
    f"{workspace_url}{api_path}",
    headers=headers,
    json=data
)

print(response.json())