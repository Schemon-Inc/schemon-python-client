def remove_trailing_slash(file_path: str) -> str:
    """
    Removes the trailing slash from a file path if it exists.

    Args:
        file_path (str): The file path to check and modify.

    Returns:
        str: The file path without a trailing slash.
    """
    return file_path.rstrip("/") if file_path else file_path

def remove_leading_slash(file_path: str) -> str:
    """
    Removes the leading slash from a file path if it exists.

    Args:
        file_path (str): The file path to check and modify.

    Returns:
        str: The file path without a leading slash.
    """
    return file_path.lstrip('/') if file_path else file_path
