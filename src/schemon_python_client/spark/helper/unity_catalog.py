import fnmatch
from typing import List
from schemon_python_client.spark.helper.databricks import list_files


def list_files_in_volume(
    path: str, directories: List, extension: str, recursive: bool = False
) -> List[str]:
    files = []

    def list_files_in_directory(directory_path: str):
        """Helper function to list files in a given directory path."""
        file_objects = list_files(directory_path)
        for file in file_objects:
            if file.isDir():
                if recursive:
                    list_files_in_directory(file.path)
            else:
                if extension is None:
                    files.append(file.path.replace("dbfs:", ""))
                else:
                    if file.name.endswith(f".{extension}"):
                        files.append(file.path.replace("dbfs:", ""))

    if len(directories) > 0:
        for directory in directories:
            if "*" in directory:
                # Handle wildcard case
                base_path = directory.split("*")[0]
                directory_path = f"{path}{base_path}"
                subdirs = list_files(directory_path)

                for subdir in subdirs:
                    if fnmatch.fnmatch(subdir.path, f"*{directory}"):
                        list_files_in_directory(subdir.path)
            else:
                # Handle regular directory case
                directory_split = directory.split("/")
                if len(directory_split) > 1:
                    directory = f"/{directory_split[1]}"
                directory_path = f"{path}{directory}"
                list_files_in_directory(directory_path)
    else:
        list_files_in_directory(path)

    return files
