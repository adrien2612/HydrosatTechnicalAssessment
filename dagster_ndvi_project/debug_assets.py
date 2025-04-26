# debug_assets.py
import sys
import os

# Print the current directory and Python path
print(f"Current directory: {os.getcwd()}")
print(f"Python path: {sys.path}")

# Import Dagster
try:
    from dagster import repository
    from dagster._core.definitions.asset_graph import AssetGraph
    
    # Define a repo to check assets
    @repository
    def debug_repo():
        from dagster_ndvi_project.definitions import definitions
        return definitions
    
    repo_def = debug_repo
    
    # Get all definitions
    defs = repo_def().get_all_repository_definition_names()
    print(f"Repository definitions: {defs}")
    
    # Get all assets
    assets_defs = repo_def().get_all_asset_nodes()
    print(f"Found {len(assets_defs)} asset definitions")
    
    # Print each asset key
    for asset in assets_defs:
        print(f"Asset: {asset.key}")
        
    # Check the job definition
    from dagster_ndvi_project.definitions import ndvi_processing_job
    print(f"Job name: {ndvi_processing_job.name}")
    print(f"Job selection: {ndvi_processing_job.selection}")
    
    # Get job ops and asset keys
    job_def = ndvi_processing_job
    job_assets = job_def.asset_selection
    print(f"Job asset selection: {job_assets}")
    
except Exception as e:
    print(f"Error in debug script: {e}")
    import traceback
    traceback.print_exc() 