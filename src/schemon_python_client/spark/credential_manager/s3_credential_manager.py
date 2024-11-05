from schemon_python_client.spark.base.credential_manager import CredentialManager


class S3CredentialManager(CredentialManager):
    """
    Credential manager for AWS S3 using AccessKey and SecretAccessKey.
    """

    def __init__(self, access_key=None, secret_access_key=None, service_provider="S3"):
        super().__init__(service_provider=service_provider)
        if not access_key or not secret_access_key:
            raise ValueError(
                "AccessKey and SecretAccessKey are required for key-based S3 authentication."
            )
        self.access_key = self.encrypt_key(access_key)
        self.secret_access_key = self.encrypt_key(secret_access_key)

    def encrypt_key(self, key):
        """
        Encrypt the access key or secret key.
        """
        return self._cipher.encrypt(key.encode()).decode()

    def decrypt_key(self, encrypted_key):
        """
        Decrypt the access key or secret key.
        """
        return self._cipher.decrypt(encrypted_key.encode()).decode()

    def get_credentials(self):
        """
        Retrieve S3 credentials.
        """
        decrypted_access_key = self.decrypt_key(self.access_key)
        decrypted_secret_access_key = self.decrypt_key(self.secret_access_key)
        return {"access_key": decrypted_access_key, "secret_access_key": decrypted_secret_access_key}

    def set_credentials(self, credentials):
        """
        Set S3 credentials (AccessKey and SecretAccessKey).
        """
        self.access_key = self.encrypt_key(credentials.get("access_key"))
        self.secret_access_key = self.encrypt_key(credentials.get("secret_access_key"))
        print("S3 credentials set successfully.")
