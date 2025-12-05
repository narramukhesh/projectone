class AzureBlobCredentialsError(Exception):
    """This exception is used when azure blob credentials are missing from the provided credentials list"""


class AzureBlobConnectorInitError(Exception):
    """This exception is used when azure blob connector tried to initialize and problem raise while doing that"""


class AzureBlobLockAlreadyExists(Exception):
    """This exception is used when azure blob request for lock which already exists"""
