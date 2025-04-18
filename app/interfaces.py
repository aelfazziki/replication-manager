# interfaces.py (or separate files)

import abc
from typing import Any, Dict, List, Generator, Optional

class SourceConnector(abc.ABC):
    """Abstract Base Class for Source Connectors."""

    @abc.abstractmethod
    def connect(self, config: Dict[str, Any]) -> None:
        """Establish a connection to the source database."""
        pass

    @abc.abstractmethod
    def disconnect(self) -> None:
        """Close the connection to the source database."""
        pass

    @abc.abstractmethod
    def get_changes(self, last_position: Optional[Dict[str, Any]]) -> tuple[List[Dict[str, Any]], Optional[Dict[str, Any]]]:
        """
        Fetch change data capture (CDC) events since the last_position.

        Args:
            last_position: A dictionary representing the point from where to start fetching changes
                           (e.g., {'scn': 12345} for Oracle, {'lsn': '0/1A00000'} for Postgres).
                           None indicates fetching from the earliest available point or current state
                           depending on the source type and configuration.

        Returns:
            A tuple containing:
            - A list of standardized change events. Each event should be a dictionary
              (e.g., {'operation': 'insert', 'schema': 'hr', 'table': 'employees',
                      'before': None, 'after': {'id': 1, 'name': 'Alice'}, 'timestamp': ...}).
            - The new position dictionary representing the point up to which changes were fetched.
              This should be persisted and passed to the next call. Returns None if no new position.
        """
        pass

    @abc.abstractmethod
    def get_current_position(self) -> Optional[Dict[str, Any]]:
        """
        Get the current position (SCN, LSN, timestamp, etc.) in the source's change stream.
        Useful for starting CDC from the current point in time.

        Returns:
            A dictionary representing the current position, or None if not applicable/supported.
        """
        pass

    @abc.abstractmethod
    def get_schemas_and_tables(self) -> Dict[str, List[str]]:
        """
        Retrieve a list of schemas and the tables within them.

        Returns:
            A dictionary where keys are schema names and values are lists of table names.
            Example: {'HR': ['EMPLOYEES', 'DEPARTMENTS'], 'SALES': ['ORDERS']}
        """
        pass

    @abc.abstractmethod
    def get_table_schema(self, schema_name: str, table_name: str) -> Dict[str, Any]:
        """
        Retrieve the schema definition for a specific table in a standardized format.

        Args:
            schema_name: The name of the schema.
            table_name: The name of the table.

        Returns:
            A dictionary representing the table schema (e.g., including columns, types, keys).
            Example: {'schema': 'HR', 'table': 'EMPLOYEES',
                      'columns': [{'name': 'ID', 'type': 'NUMBER', 'nullable': False, 'pk': True}, ...]}
        """
        pass

    def perform_initial_load_chunk(self, schema_name: str, table_name: str, chunk_size: int, offset: int) -> Generator[List[Dict[str, Any]], None, None]:
        """
        Generator function to fetch data for initial load in chunks.
        (Optional: Provide a default implementation or leave abstract).

        Args:
            schema_name: The name of the schema.
            table_name: The name of the table.
            chunk_size: The number of rows per chunk.
            offset: The starting row offset for the current chunk (implement pagination if needed).

        Yields:
            A list of dictionaries, where each dictionary represents a row.
        """
        # Default implementation - can be overridden by subclasses
        # This basic version might not work for all databases or large tables without pagination
        # Consider adding proper pagination (LIMIT/OFFSET or keyset pagination) in concrete implementations
        with self.connect({}) as conn: # Assuming connect can be used contextually or similar
             cursor = conn.execute(f"SELECT * FROM {schema_name}.{table_name}") # Example query
             while True:
                 rows = cursor.fetchmany(chunk_size)
                 if not rows:
                     break
                 yield [dict(row) for row in rows] # Assuming row objects can be dict-like


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
    @abc.abstractmethod
    def truncate_table(self, schema_name: str, table_name: str) -> None:
        """
        Removes all data from the specified table in the target.
        Implementations should use TRUNCATE or DELETE depending on database/permissions.

        Args:
            schema_name: The target schema name.
            table_name: The target table name.
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


class SchemaConverter(abc.ABC):
    """Abstract Base Class for Schema Converters."""

    @abc.abstractmethod
    def convert_schema(self, source_table_definition: Dict[str, Any], source_type: str, target_type: str) -> Dict[str, Any]:
        """
        Convert a table schema definition from the source format/types to the target format/types.

        Args:
            source_table_definition: Standardized table definition from the SourceConnector.
            source_type: String identifier for the source database type (e.g., 'oracle', 'postgres').
            target_type: String identifier for the target database type (e.g., 'bigquery', 'mysql').

        Returns:
            A standardized table definition suitable for the TargetConnector's create_table method.
        """
        pass