from schemon_python_client.spark.base.credential_manager import CredentialManager


class UnityCatalogCredentialManager(CredentialManager):
    """
    Credential manager for MSSQL using basic authentication.
    """

    def __init__(self, service_provider="Databricks"):
        super().__init__(service_provider=service_provider)

    def encrypt_password(self, password):
        """
        Encrypt the password before storing it.
        """
        NotImplemented

    def decrypt_password(self, encrypted_password):
        """
        Decrypt the password when needed.
        """
        NotImplemented

    def get_credentials(self):
        """
        Retrieve credentials from the encrypted storage.
        """
        NotImplemented

    def set_credentials(self, credentials):
        """
        Set credentials (username and password).
        """
        NotImplemented
