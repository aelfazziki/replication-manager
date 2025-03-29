# schema_converter.py (or add to interfaces.py)

from typing import Any, Dict
# --- Import SQLAlchemy types ---
from sqlalchemy import (
    types, String, Numeric, DateTime, Text, LargeBinary, Integer,
    SmallInteger, BigInteger, Float, Boolean, TIMESTAMP, Interval
)

# Assuming interfaces.py is in the same directory or adjust import path
from app.interfaces import SchemaConverter


class BasicSqlAlchemyConverter(SchemaConverter):
    """
    A basic schema converter that maps common RDBMS types
    to generic SQLAlchemy types.
    """

    # --- Basic Type Mapping ---
    # Extend this map as needed for different source types and specific target nuances.
    # Keys should likely be upper-case base types from the source connector's get_table_schema.
    TYPE_MAP: Dict[str, types.TypeEngine] = {
        # Oracle Common Types
        'VARCHAR2': String,
        'NVARCHAR2': String,  # Consider sqlalchemy.types.NVARCHAR if unicode handling differs
        'CHAR': String,  # Consider sqlalchemy.types.CHAR for fixed-length
        'NCHAR': String,  # Consider sqlalchemy.types.NCHAR
        'NUMBER': Numeric,
        'FLOAT': Float,  # Oracle FLOAT is often NUMBER, map based on precision or use Float
        'BINARY_FLOAT': Float(precision=24),  # Or map to sqlalchemy.types.REAL
        'BINARY_DOUBLE': Float(precision=53),  # Or map to sqlalchemy.types.FLOAT or DOUBLE_PRECISION
        'DATE': DateTime,  # Oracle DATE includes time component
        'TIMESTAMP': TIMESTAMP,  # Use TIMESTAMP for timezone-aware types if applicable
        'TIMESTAMP WITH TIME ZONE': TIMESTAMP(timezone=True),
        'TIMESTAMP WITH LOCAL TIME ZONE': TIMESTAMP(timezone=True),
        # SQLAlchemy often treats this as timezone-aware UTC
        'INTERVAL YEAR TO MONTH': Interval,  # Requires testing with specific dialects
        'INTERVAL DAY TO SECOND': Interval,  # Requires testing with specific dialects
        'CLOB': Text,
        'NCLOB': Text,  # Consider sqlalchemy.types.UnicodeText
        'BLOB': LargeBinary,
        'RAW': LargeBinary,  # Map RAW based on typical usage/length
        'LONG': Text,  # Deprecated Oracle type
        'LONG RAW': LargeBinary,  # Deprecated Oracle type

        # Add other common types (PostgreSQL, MySQL etc.) if needed
        'TEXT': Text,
        'INTEGER': Integer,
        'INT': Integer,
        'SMALLINT': SmallInteger,
        'BIGINT': BigInteger,
        'DECIMAL': Numeric,
        'NUMERIC': Numeric,
        'REAL': Float,  # Often maps to FLOAT(24)
        'DOUBLE PRECISION': Float,  # Often maps to FLOAT(53)
        'BOOLEAN': Boolean,
        'BOOL': Boolean,
        'BYTEA': LargeBinary,  # PostgreSQL byte array
        'JSON': types.JSON,
        'JSONB': types.JSON,  # PostgreSQL JSON Binary
        'UUID': types.UUID,
        # Add more mappings...
    }

    def _get_sqlalchemy_type(self, source_col_def: Dict[str, Any], source_type_str: str) -> types.TypeEngine:
        """Maps a source column definition to a SQLAlchemy type instance."""
        base_type_name = source_col_def.get('type', '').upper()  # Use the base type name
        source_full_type = source_col_def.get('full_type', '').upper()  # e.g., VARCHAR2(100)

        # Attempt mapping based on base type
        mapped_type_class = self.TYPE_MAP.get(base_type_name)

        if not mapped_type_class:
            # Fallback: Try parsing full type if base type unknown (simple cases)
            if source_full_type.startswith('VARCHAR') or source_full_type.startswith('NVARCHAR'):
                mapped_type_class = String
            elif source_full_type.startswith('NUMBER') or source_full_type.startswith('DECIMAL'):
                mapped_type_class = Numeric
            elif source_full_type.startswith('CHAR') or source_full_type.startswith('NCHAR'):
                mapped_type_class = String  # Or CHAR if fixed-length important
            elif source_full_type.startswith('FLOAT'):
                mapped_type_class = Float
            # Add more fallbacks if needed
            else:
                print(
                    f"Warning: Unmapped source type '{base_type_name}' / '{source_full_type}'. Defaulting to String.")  # Replace with logging
                return String()  # Default fallback

        # Instantiate type with parameters (length, precision, scale) if applicable
        try:
            if issubclass(mapped_type_class, String):
                length = source_col_def.get('length')
                return mapped_type_class(length=length) if length else mapped_type_class()
            elif issubclass(mapped_type_class, (Numeric)):
                precision = source_col_def.get('precision')
                scale = source_col_def.get('scale')
                if precision is not None:
                    return mapped_type_class(precision=precision, scale=scale)  # scale defaults to 0 if None
                else:
                    # Handle Oracle NUMBER without precision/scale (maps to floating point conceptually)
                    # Mapping to Float might be better depending on target DB behavior
                    return mapped_type_class()  # Or maybe Float() ? Needs careful consideration.
            elif issubclass(mapped_type_class, (Float)):
                # Precision for Float might correspond to BINARY_FLOAT/BINARY_DOUBLE
                precision = source_col_def.get('precision')
                return mapped_type_class(precision=precision) if precision else mapped_type_class()
            elif issubclass(mapped_type_class, (TIMESTAMP)):
                # Handle timezone if source provided it
                is_tz = 'TIME ZONE' in source_full_type or 'TZ' in source_full_type
                return mapped_type_class(timezone=is_tz)
            else:
                # For types without parameters (Integer, Text, Boolean, etc.)
                return mapped_type_class()
        except TypeError as e:
            print(
                f"Warning: Could not instantiate type {mapped_type_class} with params for {source_col_def}. Using default instance. Error: {e}")  # Replace with logging
            return mapped_type_class()

    def convert_schema(self, source_table_definition: Dict[str, Any], source_type: str, target_type: str) -> Dict[
        str, Any]:
        """
        Convert a table schema definition from the source format/types
        to a definition with SQLAlchemy types suitable for the target.
        """
        # This basic converter doesn't use target_type yet, but could for dialect-specific adjustments.
        print(
            f"Converting schema for {source_table_definition.get('schema')}.{source_table_definition.get('table')} from {source_type} to SQLAlchemy types.")  # Replace with logging

        target_columns = []
        source_columns = source_table_definition.get('columns', [])

        for source_col in source_columns:
            col_name = source_col.get('name')
            if not col_name:
                print(f"Warning: Skipping column definition missing 'name': {source_col}")  # Replace with logging
                continue

            # Map source type to SQLAlchemy type
            sqlalchemy_type_instance = self._get_sqlalchemy_type(source_col, source_type)

            target_col_def = {
                'name': col_name,
                'type': sqlalchemy_type_instance,  # The SQLAlchemy type instance
                'nullable': source_col.get('nullable', True),
                'primary_key': source_col.get('pk', False)
                # Add other attributes like 'default', 'server_default' if needed and available
            }
            target_columns.append(target_col_def)

        # Return definition with converted columns list
        target_definition = {
            'schema': source_table_definition.get('schema'),
            'table': source_table_definition.get('table'),
            'columns': target_columns,
            'primary_key_columns': source_table_definition.get('primary_key', [])  # Keep original PK column names
            # Add constraints, indexes etc. if available and needed
        }

        return target_definition
