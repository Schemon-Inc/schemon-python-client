from abc import abstractmethod
from cryptography.fernet import Fernet
from schemon_python_client.spark.base.base import Base
import os


class CredentialManager(Base):
    """
    Base class for managing credentials. Each subclass will define platform-specific
    credential handling like username/password or key-based auth.
    """

    def __init__(self, service_provider):
        """
        Initialize service provider property.
        :param service_provider: Name of the service provider (e.g., AWS, MySQL).
        """
        self.service_provider = service_provider
        self._encryption_key = os.getenv("ENCRYPTION_KEY") or Fernet.generate_key()
        self._cipher = Fernet(self._encryption_key)

    @abstractmethod
    def get_credentials(self):
        """
        Abstract method to get credentials.
        This method should be implemented by subclasses.
        """
        pass

    @abstractmethod
    def set_credentials(self, credentials):
        """
        Abstract method to set credentials.
        This method should be implemented by subclasses.
        """
        pass

    @abstractmethod
    def encrypt_key(self, key):
        """
        Abstract method to encrypt key.
        This method should be implemented by subclasses.
        """
        pass

    @abstractmethod
    def decrypt_key(self, key):
        """
        Abstract method to decrypt key.
        This method should be implemented by subclasses.
        """
        pass

    def display_info(self):
        """
        Method to display basic information about the credentials without exposing sensitive data.
        """
        print(f"Service Provider: {self.service_provider}")
        print("Credentials are securely stored and cannot be displayed.")
