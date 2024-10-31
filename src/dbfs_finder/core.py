"""Core functionality for DBFS Finder."""
from typing import List, Optional
import requests
import os
from dotenv import load_dotenv

class DBFSFileFinder:
    """DBFS file finder utility"""
    
    def __init__(self, workspace_url: Optional[str] = None, access_token: Optional[str] = None):
        """Initialize DBFS file finder"""
        load_dotenv()
        
        self.base_url = (workspace_url or os.getenv('DATABRICKS_WORKSPACE_URL', '')).rstrip('/')
        self.access_token = access_token or os.getenv('DATABRICKS_ACCESS_TOKEN')
        
        if not self.base_url or not self.access_token:
            raise ValueError(
                "Missing Databricks credentials. Please set DATABRICKS_WORKSPACE_URL and "
                "DATABRICKS_ACCESS_TOKEN environment variables or provide them as arguments."
            )
            
        self.headers = {
            'Authorization': f'Bearer {self.access_token}',
            'Content-Type': 'application/json'
        }

    def list_dbfs(self, path: str) -> List[dict]:
        """List contents of a DBFS directory"""
        try:
            response = requests.get(
                f"{self.base_url}/api/2.0/dbfs/list",
                headers=self.headers,
                params={'path': path}
            )
            if response.status_code == 404:
                print(f"Path not found: {path}")
                return []
            response.raise_for_status()
            return response.json().get('files', [])
        except requests.exceptions.RequestException as e:
            print(f"Error accessing {path}: {str(e)}")
            return []

    def get_file_info(self, path: str) -> Optional[dict]:
        """Get specific file information"""
        try:
            response = requests.get(
                f"{self.base_url}/api/2.0/dbfs/get-status",
                headers=self.headers,
                params={'path': path}
            )
            response.raise_for_status()
            return response.json()
        except:
            return None

    def quick_find(self, filename: str) -> List[str]:
        """Quick search in common locations"""
        found_paths: List[str] = []
        
        # Primary search paths based on your DBFS structure
        dbfs_paths = [
            '/FileStore/tables',  # Where custom_actions.py is located
            '/FileStore',
            '/dbfs/FileStore/tables',
            '/dbfs/FileStore'
        ]
        
        print(f"\nSearching DBFS locations...")
        for path in dbfs_paths:
            print(f"Checking {path}...")
            files = self.list_dbfs(path)
            for file in files:
                if os.path.basename(file.get('path', '')) == filename:
                    file_path = file['path']
                    # Clean up path format
                    if file_path.startswith('/dbfs/'):
                        file_path = file_path[5:]  # Remove /dbfs/ prefix
                    found_paths.append(f"dbfs:{file_path}")
                    
                    # Get file info
                    file_info = self.get_file_info(file_path)
                    if file_info:
                        print(f"\nFile details:")
                        print(f"Size: {file_info.get('file_size', 'N/A')} bytes")
                        print(f"Modified: {file_info.get('modification_time', 'N/A')}")
        
        return found_paths

    def find_file(self, filename: str) -> List[str]:
        """Deep search starting from /FileStore/tables"""
        return self.quick_find(filename)  # For this case, quick find is sufficient
