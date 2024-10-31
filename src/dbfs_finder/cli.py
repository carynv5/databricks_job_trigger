"""Command line interface for DBFS Finder."""
import os
import argparse
from typing import Optional
from dotenv import load_dotenv
from .core import DBFSFileFinder

def main() -> None:
    """Main entry point for the CLI."""
    parser = argparse.ArgumentParser(description='Find files in Databricks File System (DBFS)')
    parser.add_argument('filename', help='Name of file to search for')
    parser.add_argument('--workspace-url', help='Databricks workspace URL')
    parser.add_argument('--token', help='Databricks access token')
    args = parser.parse_args()

    # Load environment variables
    load_dotenv()
    
    # Get configuration
    workspace_url = args.workspace_url or os.getenv('DATABRICKS_WORKSPACE_URL')
    access_token = args.token or os.getenv('DATABRICKS_ACCESS_TOKEN')
    
    if not workspace_url or not access_token:
        print("❌ Error: Missing workspace URL or access token")
        print("Please provide them via environment variables:")
        print("DATABRICKS_WORKSPACE_URL=https://your-workspace.cloud.databricks.com")
        print("DATABRICKS_ACCESS_TOKEN=your_access_token")
        return

    finder = DBFSFileFinder(workspace_url, access_token)
    
    print(f"\n🔍 Searching for '{args.filename}'...")
    
    try:
        found_files = finder.quick_find(args.filename)
        
        if found_files:
            print(f"\n✅ Found {len(found_files)} matching file(s):")
            for path in found_files:
                print(f"  • {path}")
            
            print("\nTo download this file, use:")
            for path in found_files:
                clean_path = path.replace('dbfs:', '')
                print(f"dbfs cp {path} /local/path/")
                print(f"# or using the Databricks CLI:")
                print(f"databricks fs cp {path} /local/path/")
        else:
            print(f"\n❌ No files matching '{args.filename}' found")
            print("\nChecked locations:")
            print("  • /FileStore/tables")
            print("  • /FileStore")
    
    except Exception as e:
        print(f"\n❌ Error during search: {str(e)}")
        print("\nPlease verify your Databricks credentials and try again.")

if __name__ == "__main__":
    main()
