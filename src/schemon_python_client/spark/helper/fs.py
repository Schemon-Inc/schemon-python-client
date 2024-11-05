from pathlib import Path

def list_folders_in_directory(directory):
    # Use pathlib to get only directories
    return [f.name for f in Path(directory).iterdir() if f.is_dir()]

