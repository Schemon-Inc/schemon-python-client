from databricks.sdk.runtime import *  # type: ignore

def get_secret_value(scope: str, key: str) -> str:
    return dbutils.secrets.get(scope=scope, key=key)  # type: ignore


def list_files(directory: str):
    return dbutils.fs.ls(directory)  # type: ignore


def get_all_widgets():
    return dbutils.widgets.getAll()  # type: ignore


def get_widget_value(name: str, default: str = None):
    widgets: dict = dbutils.widgets.getAll()  # type: ignore
    if name in widgets:
        return widgets.get(name, default)
    else:
        raise ValueError(f"Widget '{name}' not found in the notebook")


