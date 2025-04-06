# logminer.py (Refactored)

import cx_Oracle
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, Generator # Added Generator

# Assuming interfaces.py is in the same directory or adjust import path
from app.interfaces import SourceConnector

# Placeholder - remove if psycopg2 is not needed here anymore
# import psycopg2

class OracleLogMinerConnector(SourceConnector):
    """
    SourceConnector implementation for Oracle using LogMiner.
    """

    def __init__(self):
        """Initialize the connector."""
        self.conn: Optional[cx_Oracle.Connection] = None
        self.config: Dict[str, Any] = {}
        self._logminer_started: bool = False

    def connect(self, config: Dict[str, Any]) -> None:
        """Establish connection to Oracle."""
        if self.conn:
            self.disconnect() # Ensure any existing connection is closed

        self.config = config
        try:
            # Ensure port is an integer if present
            port = int(config.get('port', 1521))
            dsn = cx_Oracle.makedsn(
                config['host'],
                port,
                service_name=config.get('service_name') # Use get for optional service_name
                # Consider adding SID support if needed: sid=config.get('sid')
            )
            # Consider connection pooling for efficiency in a real application
            self.conn = cx_Oracle.connect(
                user=config['username'],
                password=config['password'],
                dsn=dsn
            )
            self.conn.autocommit = False # Important for controlling transactions
            print("Successfully connected to Oracle.") # Replace with proper logging
        except cx_Oracle.Error as e:
            print(f"Error connecting to Oracle: {e}") # Replace with proper logging
            raise # Re-raise the exception to signal failure

    def disconnect(self) -> None:
        """Disconnect from Oracle."""
        if self._logminer_started and self.conn:
             try:
                 with self.conn.cursor() as cursor:
                     print("Ending LogMiner session...") # Replace with logging
                     cursor.execute("BEGIN DBMS_LOGMNR.END_LOGMNR(); END;")
                     self._logminer_started = False
             except cx_Oracle.Error as e:
                 # Log error but proceed with closing connection
                 print(f"Error ending LogMiner session: {e}") # Replace with logging
        if self.conn:
            try:
                self.conn.close()
                print("Disconnected from Oracle.") # Replace with logging
            except cx_Oracle.Error as e:
                print(f"Error closing Oracle connection: {e}") # Replace with logging
            finally:
                self.conn = None
                self.config = {}

    def _start_logminer_session(self, start_scn: int) -> None:
        """Starts a LogMiner session if not already started."""
        if not self.conn:
            raise ConnectionError("Not connected to Oracle.")
        if self._logminer_started:
            # Potentially end and restart if SCN changes significantly? For now, assume it's okay.
            return

        try:
            with self.conn.cursor() as cursor:
                print(f"Starting LogMiner session from SCN: {start_scn}") # Replace with logging
                # Options:
                # - DICT_FROM_ONLINE_CATALOG: Easiest, but requires supplemental logging.
                # - COMMITTED_DATA_ONLY: Shows only committed transactions.
                # - DDL_DICT_TRACKING: Needed if you want to capture DDL via LogMiner (complex).
                options = "DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG"
#                options = "DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG + DBMS_LOGMNR.COMMITTED_DATA_ONLY"
                sql = f"""
                    BEGIN
                        DBMS_LOGMNR.START_LOGMNR(
                            STARTSCN => :start_scn,
                            OPTIONS  => {options}
                        );
                    END;
                """
                cursor.execute(sql, {'start_scn': start_scn})
                self._logminer_started = True
                print("LogMiner session started successfully.") # Replace with logging
        except cx_Oracle.Error as e:
            print(f"Error starting LogMiner session: {e}") # Replace with logging
            raise

    def get_changes(self, last_position: Optional[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], Optional[Dict[str, Any]]]:
        """Fetch structured changes from Oracle LogMiner since the last SCN."""
        if not self.conn:
            raise ConnectionError("Not connected to Oracle.")

        start_scn = last_position.get('scn', 0) if last_position else 0
        if start_scn == 0:
             current_pos = self.get_current_position()
             start_scn = current_pos.get('scn', 0) if current_pos else 0
             if start_scn == 0:
                  print("Warning: Could not determine current SCN, starting LogMiner from SCN 0.") # Replace with logging

        self._start_logminer_session(start_scn)

        changes = []
        max_scn_in_batch = start_scn

        try:
            with self.conn.cursor() as cursor:
                # --- Query Modification ---
                # We need PRIMARY KEY info from supplemental logging for UPDATE/DELETE.
                # We need column values for INSERT/UPDATE.
                # The specific columns depend heavily on supplemental logging setup.
                # Example query assuming *some* supplemental data is logged:
                query = """
                    SELECT
                        SCN, TIMESTAMP, OPERATION_CODE, OPERATION,
                        SEG_OWNER, SEG_NAME, TABLE_NAME, ROW_ID, CSF,
                        SQL_REDO -- Keep for debugging/fallback, but aim not to rely on it
                        -- Add columns logged via supplemental logging, e.g.:
                        -- , COL1, COL2, PK_COL1, PK_COL2 ...
                    FROM V$LOGMNR_CONTENTS
                    WHERE OPERATION_CODE IN (1, 2, 3) -- INSERT, DELETE, UPDATE
                      AND SCN > :start_scn
                      -- Optional filtering: AND SEG_OWNER = '...' AND TABLE_NAME = '...'
                    ORDER BY SCN
                """
                # Set row factory to fetch as dictionaries for easier access by column name
                # This requires knowing the exact column names returned by the query
                # cursor.rowfactory = lambda *args: dict(zip([d[0] for d in cursor.description], args))
                # For simplicity now, stick to index-based access and simulate structure

                cursor.execute(query, {'start_scn': start_scn})
                raw_rows = cursor.fetchall() # Fetch all results for now

                # --- Processing Logic ---
                for row in raw_rows:
                    # Basic fields (adjust indices if query changes)
                    scn, ts, op_code, op_name, schema, seg_name, table_name, row_id, csf, sql_redo = row[:10]

                    # *** SIMULATED STRUCTURED DATA ***
                    # This assumes supplemental logging provides necessary data.
                    # In reality, you'd extract these from the actual columns returned by the query.
                    primary_keys = {} # e.g., {'ID': 123} - NEEDED for UPDATE/DELETE
                    before_data = {} # e.g., {'ID': 123, 'NAME': 'Old', 'VALUE': 10} - NEEDED for DELETE/UPDATE
                    after_data = {} # e.g., {'ID': 123, 'NAME': 'New', 'VALUE': 20} - NEEDED for INSERT/UPDATE

                    # Placeholder logic - needs real data extraction based on actual query results
                    if op_code == 1: # INSERT
                         op_name = 'insert'
                         # Assume 'after_data' can be populated from logged columns
                         after_data = {'placeholder_col': 'inserted_value', 'scn': scn} # SIMULATED
                         primary_keys = {'placeholder_pk': 'inserted_pk'} # SIMULATED (extract from after_data if possible)
                    elif op_code == 2: # DELETE
                         op_name = 'delete'
                         # Assume 'before_data' (esp PKs) can be populated from logged columns
                         before_data = {'placeholder_pk': 'deleted_pk', 'scn': scn} # SIMULATED
                         primary_keys = {'placeholder_pk': 'deleted_pk'} # SIMULATED (extract from before_data)
                    elif op_code == 3: # UPDATE
                         op_name = 'update'
                         # Assume PKs and changed columns (after_data) are logged.
                         # 'before_data' might only contain PKs unless specifically logged.
                         before_data = {'placeholder_pk': 'updated_pk'} # SIMULATED (PKs needed for WHERE clause)
                         after_data = {'placeholder_col': 'updated_value', 'scn': scn} # SIMULATED (Changed columns)
                         primary_keys = {'placeholder_pk': 'updated_pk'} # SIMULATED (extract from before/after)
                    else:
                         op_name = op_name.lower() # Should not happen with WHERE clause filter

                    # Standardized Change Event
                    change_event = {
                        'source': 'oracle_logminer',
                        'position': {'scn': scn},
                        'timestamp': ts,
                        'operation': op_name,
                        'schema': schema or seg_name, # Use SEG_OWNER if available
                        'table': table_name or seg_name, # Use TABLE_NAME if available
                        'primary_keys': primary_keys, # Key(s) and Value(s) identifying the row
                        'before_data': before_data, # Full row image before change (for DELETE, UPDATE)
                        'after_data': after_data,   # Full row image after change (for INSERT, UPDATE)
                        '_metadata': {
                            'row_id': row_id,
                            'csf': csf,
                            'raw_sql': sql_redo # Keep for debugging
                        }
                    }
                    changes.append(change_event)

                    if scn > max_scn_in_batch:
                        max_scn_in_batch = scn

                print(f"Fetched {len(changes)} structured changes from LogMiner (Simulated).") # Replace with logging

        except cx_Oracle.Error as e:
            print(f"Error querying V$LOGMNR_CONTENTS: {e}") # Replace with logging
            raise

        new_position = {'scn': max_scn_in_batch} if max_scn_in_batch > start_scn else last_position
        return changes, new_position

    # ... (get_current_position, get_schemas_and_tables, get_table_schema, perform_initial_load_chunk remain the same) ...

    def get_current_position(self) -> Optional[Dict[str, Any]]:
        """Get the current SCN from the Oracle database."""
        if not self.conn:
            raise ConnectionError("Not connected to Oracle.")
        try:
            with self.conn.cursor() as cursor:
                cursor.execute("SELECT CURRENT_SCN FROM V$DATABASE")
                result = cursor.fetchone()
                if result:
                    return {'scn': result[0]}
                return None
        except cx_Oracle.Error as e:
            print(f"Error fetching current SCN: {e}") # Replace with logging
            return None

    def get_schemas_and_tables(self) -> Dict[str, List[str]]:
        """Retrieve accessible schemas and their tables from Oracle."""
        if not self.conn:
            raise ConnectionError("Not connected to Oracle.")

        schemas_tables = {}
        try:
            with self.conn.cursor() as cursor:
                # Query ALL_TABLES for tables accessible to the connected user
                # Filter out common Oracle internal schemas if desired
                query = """
                    SELECT owner, table_name
                    FROM all_tables
                    WHERE owner NOT IN (
                        'SYS', 'SYSTEM', 'OUTLN', 'DBSNMP', 'APPQOSSYS', 'CTXSYS',
                        'DBSFWUSER', 'DIP', 'GGSYS', 'GSMADMIN_INTERNAL', 'LBACSYS',
                        'MDSYS', 'OJVMSYS', 'OLAPSYS', 'ORDSYS', 'ORDDATA', 'ORDPLUGINS',
                        'SI_INFORMTN_SCHEMA', 'WMSYS', 'XDB', 'AUDSYS', 'EXFSYS'
                        -- Add other schemas to exclude if needed
                    )
                    ORDER BY owner, table_name
                """
                cursor.execute(query)
                for row in cursor:
                    owner, table_name = row
                    if owner not in schemas_tables:
                        schemas_tables[owner] = []
                    schemas_tables[owner].append(table_name)
            return schemas_tables
        except cx_Oracle.Error as e:
            print(f"Error fetching schemas and tables: {e}") # Replace with logging
            return {}

    def get_table_schema(self, schema_name: str, table_name: str) -> Dict[str, Any]:
        """Retrieve column definitions and primary key for an Oracle table."""
        if not self.conn:
            raise ConnectionError("Not connected to Oracle.")

        table_def = {
            'schema': schema_name,
            'table': table_name,
            'columns': [],
            'primary_key': []
        }

        try:
            with self.conn.cursor() as cursor:
                # Get columns and types
                query_cols = """
                    SELECT column_name, data_type, data_length, data_precision, data_scale, nullable
                    FROM all_tab_columns
                    WHERE owner = :schema_name AND table_name = :table_name
                    ORDER BY column_id
                """
                cursor.execute(query_cols, {'schema_name': schema_name, 'table_name': table_name})
                columns_data = cursor.fetchall()

                if not columns_data:
                     raise ValueError(f"Table {schema_name}.{table_name} not found or no columns accessible.")

                for col in columns_data:
                    col_name, data_type, length, precision, scale, nullable_flag = col
                    # Basic type formatting, might need more detail (e.g., VARCHAR2(100))
                    full_type = data_type
                    if data_type in ('VARCHAR2', 'CHAR', 'NVARCHAR2') and length:
                         full_type = f"{data_type}({length})"
                    elif data_type == 'NUMBER' and precision is not None:
                         full_type = f"NUMBER({precision},{scale or 0})" # NUMBER(p,s) or NUMBER(p)

                    table_def['columns'].append({
                        'name': col_name,
                        'type': data_type, # Store base type
                        'full_type': full_type, # Store formatted type
                        'length': length,
                        'precision': precision,
                        'scale': scale,
                        'nullable': nullable_flag == 'Y'
                    })

                # Get primary key columns
                query_pk = """
                    SELECT cols.column_name
                    FROM all_cons_columns cols
                    JOIN all_constraints cons ON cols.constraint_name = cons.constraint_name AND cols.owner = cons.owner
                    WHERE cons.constraint_type = 'P'
                    AND cons.owner = :schema_name
                    AND cons.table_name = :table_name
                    ORDER BY cols.position
                """
                cursor.execute(query_pk, {'schema_name': schema_name, 'table_name': table_name})
                table_def['primary_key'] = [row[0] for row in cursor]

                # Add PK info to columns list
                for col_def in table_def['columns']:
                    if col_def['name'] in table_def['primary_key']:
                        col_def['pk'] = True
                    else:
                        col_def['pk'] = False


            return table_def
        except cx_Oracle.Error as e:
            print(f"Error fetching table schema for {schema_name}.{table_name}: {e}") # Replace with logging
            raise # Re-raise as it's likely a configuration or permissions issue

    # Optional: Implement initial load with pagination
    def perform_initial_load_chunk(self, schema_name: str, table_name: str, chunk_size: int = 1000) -> Generator[List[Dict[str, Any]], None, None]:
        """Generator to fetch table data in chunks using Oracle pagination."""
        if not self.conn:
            raise ConnectionError("Not connected to Oracle.")

        print(f"Starting initial load for {schema_name}.{table_name}...") # Replace with logging
        offset = 0
        try:
            with self.conn.cursor() as cursor:
                 # Get column names to build dictionaries correctly
                 table_schema = self.get_table_schema(schema_name, table_name)
                 column_names = [col['name'] for col in table_schema['columns']]
                 column_list_str = ", ".join([f'"{c}"' for c in column_names]) # Quote column names

                 while True:
                    query = f"""
                        SELECT {column_list_str}
                        FROM "{schema_name}"."{table_name}"
                        ORDER BY ROWID -- Or use primary key for deterministic order if possible
                        OFFSET :offset ROWS FETCH NEXT :chunk_size ROWS ONLY
                    """
                    cursor.execute(query, {'offset': offset, 'chunk_size': chunk_size})
                    rows = cursor.fetchall()

                    if not rows:
                        break # No more data

                    # Convert rows to list of dictionaries
                    chunk = [dict(zip(column_names, row)) for row in rows]
                    yield chunk

                    offset += len(rows) # Move to the next offset

                 print(f"Completed initial load for {schema_name}.{table_name}.") # Replace with logging

        except cx_Oracle.Error as e:
            print(f"Error during initial load for {schema_name}.{table_name}: {e}") # Replace with logging
            raise # Re-raise the exception

# --- Remove the old PostgresCDCHandler unless you plan to refactor it immediately ---
# class PostgresCDCHandler:
#    ...