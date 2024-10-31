import pytest
from dbfs_finder.core import DBFSFileFinder

def test_dbfs_file_finder_initialization():
    workspace_url = "https://example.cloud.databricks.com"
    access_token = "test-token"
    
    finder = DBFSFileFinder(workspace_url, access_token)
    
    assert finder.base_url == workspace_url
    assert finder.headers['Authorization'] == f"Bearer {access_token}"
    assert finder.headers['Content-Type'] == 'application/json'

# Add more tests as needed
