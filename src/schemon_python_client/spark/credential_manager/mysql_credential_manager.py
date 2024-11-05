from schemon_python_client.spark.base.credential_manager import CredentialManager


class MySQLCredentialManager(CredentialManager):
    """
    Credential manager for MySQL using basic authentication.
    """

    def __init__(self, username=None, password=None, service_provider="MySQL"):
        super().__init__(service_provider=service_provider)
        if not username or not password:
            raise ValueError(
                "Username and password are required for basic authentication."
            )
        self.username = self.encrypt_password(username)
        self.password = self.encrypt_password(password)

    def encrypt_password(self, password):
        """
        Encrypt the password before storing it.
        """
        return self._cipher.encrypt(password.encode()).decode()

    def decrypt_password(self, encrypted_password):
        """
        Decrypt the password when needed.
        """
        return self._cipher.decrypt(encrypted_password.encode()).decode()

    def get_credentials(self):
        """
        Retrieve MySQL credentials from the encrypted storage.
        """
        decrypted_username = self.decrypt_password(self.username)
        decrypted_password = self.decrypt_password(self.password)
        return {"username": decrypted_username, "password": decrypted_password}

    def set_credentials(self, credentials):
        """
        Set MySQL credentials (username and password).
        """
        self.username = self.encrypt_password(credentials.get("username"))
        self.password = self.encrypt_password(credentials.get("password"))
        print("MySQL credentials set successfully.")
