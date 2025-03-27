# interfaces.py (or separate files)
import abc
from typing import Any, Dict, List, Generator, Optional
class TargetConnector(abc.ABC):
    """Abstract Base Class for Target Connectors."""

    @abc.abstractmethod
    def connect(self, config: Dict[str, Any]) -> None:
        """Establish a connection to the target system."""
        pass

    @abc.abstractmethod
    def disconnect(self) -> None:
        """Close the connection to the target system."""
        pass

    @abc.abstractmethod
    def apply_changes(self, changes: List[Dict[str, Any]]) -> None:
        """
        Apply a batch of standardized change events to the target.
        Implementations should handle INSERT, UPDATE, DELETE operations, ideally idempotently.

        Args:
            changes: A list of standardized change event dictionaries from the SourceConnector.
        """
        pass

    @abc.abstractmethod
    def create_schema_if_not_exists(self, schema_name: str) -> None:
        """Create the schema in the target if it doesn't already exist."""
        pass

    @abc.abstractmethod
    def create_table_if_not_exists(self, table_definition: Dict[str, Any]) -> None:
        """
        Create the table in the target based on a standardized definition if it doesn't exist.

        Args:
            table_definition: A standardized dictionary describing the table schema
                              (e.g., from SourceConnector.get_table_schema possibly after conversion).
        """
        pass

    def write_initial_load_chunk(self, schema_name: str, table_name: str, data_chunk: List[Dict[str, Any]]) -> None:
        """
        Write a chunk of data during initial load.
        (Optional: Provide a default implementation or leave abstract).

        Args:
            schema_name: The target schema name.
            table_name: The target table name.
            data_chunk: A list of dictionaries representing rows to be inserted.
        """
        # Default implementation might involve converting rows to INSERT statements
        # or using bulk loading capabilities if available. This needs specific implementation.
        raise NotImplementedError("Initial load writing not implemented by default.")

