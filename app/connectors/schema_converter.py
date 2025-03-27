# interfaces.py (or separate files)
import abc
from typing import Any, Dict, List, Generator, Optional

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