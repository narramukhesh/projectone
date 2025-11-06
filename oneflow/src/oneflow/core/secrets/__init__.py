from abc import ABC, abstractmethod


class SecretManager(ABC):
    """This is the spark secret manager implementation"""

    @abstractmethod
    def resolve(self, *args, **kwargs):
        """This method is used for the resolving the secrets provided by the key and value"""
