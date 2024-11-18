try:
    from databricks.sdk.runtime import dbutils  # type: ignore
except ImportError:
    # Define a mock dbutils object if not running in Databricks
    class MockDbUtils:
        class Secrets:
            @staticmethod
            def get(scope: str, key: str) -> str:
                print(f"Mock: Secrets.get called with scope={scope}, key={key}")
                return "mock_secret_value"

        class FS:
            @staticmethod
            def ls(directory: str):
                print(f"Mock: FS.ls called with directory={directory}")
                return []

        class Widgets:
            _widgets = {}

            @staticmethod
            def getAll():
                print("Mock: Widgets.getAll called")
                return MockDbUtils.Widgets._widgets

            @staticmethod
            def text(name: str, default: str, description: str):
                MockDbUtils.Widgets._widgets[name] = default

        secrets = Secrets()
        fs = FS()
        widgets = Widgets()

    dbutils = MockDbUtils()

def get_secret_value(scope: str, key: str) -> str:
    return dbutils.secrets.get(scope=scope, key=key)

def list_files(directory: str):
    return dbutils.fs.ls(directory)

def get_all_widgets():
    return dbutils.widgets.getAll()

def get_widget_value(name: str, default: str = None):
    widgets: dict = dbutils.widgets.getAll()
    if name in widgets:
        return widgets.get(name, default)
    else:
        raise ValueError(f"Widget '{name}' not found in the notebook")
