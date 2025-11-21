from typing import runtime_checkable, Protocol


class Connector(Protocol):
    """Protocol classs definition where sub classes should follow the same structure as this connector class"""

    @classmethod
    def build(cls, server_details):
        """This class method is used build the client for the connector"""
