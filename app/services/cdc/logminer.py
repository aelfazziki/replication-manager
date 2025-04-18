# logminer.py (Refactored)

import cx_Oracle
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, Generator # Added Generator

# Assuming interfaces.py is in the same directory or adjust import path
from app.interfaces import SourceConnector

# Placeholder - remove if psycopg2 is not needed here anymore
# import psycopg2
# *** Add logging import ***
import logging
logger = logging.getLogger(__name__) # Use standard Python logging

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
##############################################################################################
    def _add_logfiles(self, start_scn: Optional[int] = None):
        """Improved logfile handling with PDB awareness"""
        cursor = self.conn.cursor()
        try:
            # Check if we're in a PDB
            cursor.execute("SELECT CDB FROM V$DATABASE")
            is_cdb = cursor.fetchone()[0] == 'YES'

            # Add archived logs first
            archived_added = self._add_archived_logs(cursor, start_scn)

            # Carefully add online logs if not in PDB
            if not is_cdb and start_scn:
                self._add_online_logs(cursor, start_scn)

            return archived_added > 0

        finally:
            cursor.close()

    def _add_archived_logs(self, cursor, start_scn):
        """Helper to add archived logs"""
        query = """
            SELECT name, first_change#, next_change#
            FROM v$archived_log
            WHERE :scn BETWEEN first_change# AND next_change#
               OR next_change# > :scn
            ORDER BY first_change#
        """
        cursor.execute(query, scn=start_scn or 0)
        # ... (rest of archive log handling)
        logs_added_count = 0
        try:
            cursor.execute(query, scn=start_scn)
            archived_logs = [row[0] for row in cursor.fetchall()]
            logger.info(
                f"[LogMiner] Found {len(archived_logs)} potentially relevant archived logs based on SCN {start_scn}.")

            if not archived_logs:
                logger.warning(
                    f"[LogMiner] No relevant ARCHIVED logs found containing or after SCN {start_scn}. LogMiner might fail if no changes occurred or logs aren't available.")

            for log_file in archived_logs:
                try:
                    logger.debug(f"[LogMiner] Attempting to add archived log: {log_file}")
                    add_option = 2 if logs_added_count > 0 else 1  # Use ADDFILE(2) if not the first log, otherwise NEW(1)
                    cursor.execute("BEGIN DBMS_LOGMNR.ADD_LOGFILE(:log_name, :add_opt); END;", log_name=log_file,
                                   add_opt=add_option)
                    logs_added_count += 1
                    logger.info(f"[LogMiner] Successfully added archived log: {log_file}")
                except cx_Oracle.DatabaseError as add_err:
                    error_obj, = add_err.args
                    if error_obj.code == 1289:  # ORA-01289: cannot add duplicate logfile
                        logger.warning(f"[LogMiner] Archived log already added (ORA-01289): {log_file}")
                        # Don't increment logs_added_count again if duplicate error
                    elif error_obj.code == 1284:  # ORA-01284: file string cannot be opened
                        logger.error(
                            f"[LogMiner] Cannot open archived log file (ORA-01284): {log_file}. Check file existence and permissions.",
                            exc_info=True)
                    elif error_obj.code == 1291:  # ORA-01291: missing logfile
                        logger.error(
                            f"[LogMiner] Oracle reports missing archived log file (ORA-01291): {log_file}. Check registration and physical file.",
                            exc_info=True)
                    else:
                        logger.error(f"[LogMiner] Error adding archived log {log_file}: {add_err}", exc_info=True)

        except cx_Oracle.Error as query_err:
            logger.error(f"[LogMiner] Error querying V$ARCHIVED_LOG: {query_err}", exc_info=True)

    def _add_online_logs(self, cursor, start_scn):
        """Attempt to add online logs with PDB check"""
        try:
            cursor.execute("""
                SELECT GROUP#, STATUS, FIRST_CHANGE#, NEXT_CHANGE#
                FROM V$LOG 
                WHERE STATUS IN ('CURRENT','ACTIVE')
                  AND NEXT_CHANGE# > :scn
                ORDER BY FIRST_CHANGE#
            """, scn=start_scn)
            # Add logs using DBMS_LOGMNR.ADD_LOGFILE
        except cx_Oracle.DatabaseError as e:
            if e.args[0].code == 65040:  # PDB restriction
                logger.warning("Skipping online logs in PDB environment")
            else:
                raise

#    def _add_logfiles(self, start_scn: Optional[int] = None):
#        """Adds necessary archived redo logs to the LogMiner session. Skips online logs."""
#        if not self.conn:
#            raise ConnectionError("Oracle connection not established.")
#
#        cursor = self.conn.cursor()
#        logs_added_count = 0
#
#        # --- SKIP Adding Online Redo Logs ---
#        logger.warning("[LogMiner] Skipping addition of online redo logs due to potential PDB restrictions (ORA-65040). Relying on archived logs.")
#        # --- End Skip ---
#
#        # Add Archived Redo Logs (if start_scn provided or starting fresh)
#        if start_scn is None:
#            # If starting fresh (no last_position), get current SCN
#            start_scn = self.get_current_scn()
#            logger.info(f"[LogMiner] No start SCN provided, using current SCN: {start_scn}")
#
#        if start_scn:
#            logger.info(f"[LogMiner] Querying archived logs (V$ARCHIVED_LOG) covering or after SCN: {start_scn}...")
#            # Refined Query: Find logs containing the start SCN or any log after it.
#            # Requires logs to be registered correctly.
#            query = """
#                SELECT name
#                FROM v$archived_log
#                WHERE :scn < next_change# AND next_change# > 0 -- Log ends after start SCN (next_change#=0 means current online log)
#                   OR :scn BETWEEN first_change# AND next_change# -- Log contains the start SCN
#                ORDER BY sequence# ASC
#            """
#            try:
#                 cursor.execute(query, scn=start_scn)
#                 archived_logs = [row[0] for row in cursor.fetchall()]
#                 logger.info(f"[LogMiner] Found {len(archived_logs)} potentially relevant archived logs based on SCN {start_scn}.")
#
#                 if not archived_logs:
#                     logger.warning(f"[LogMiner] No relevant ARCHIVED logs found containing or after SCN {start_scn}. LogMiner might fail if no changes occurred or logs aren't available.")
#
#                 for log_file in archived_logs:
#                    try:
#                        logger.debug(f"[LogMiner] Attempting to add archived log: {log_file}")
#                        add_option = 2 if logs_added_count > 0 else 1 # Use ADDFILE(2) if not the first log, otherwise NEW(1)
#                        cursor.execute("BEGIN DBMS_LOGMNR.ADD_LOGFILE(:log_name, :add_opt); END;", log_name=log_file, add_opt=add_option)
#                        logs_added_count += 1
#                        logger.info(f"[LogMiner] Successfully added archived log: {log_file}")
#                    except cx_Oracle.DatabaseError as add_err:
#                        error_obj, = add_err.args
#                        if error_obj.code == 1289: # ORA-01289: cannot add duplicate logfile
#                            logger.warning(f"[LogMiner] Archived log already added (ORA-01289): {log_file}")
#                            # Don't increment logs_added_count again if duplicate error
#                        elif error_obj.code == 1284: # ORA-01284: file string cannot be opened
#                            logger.error(f"[LogMiner] Cannot open archived log file (ORA-01284): {log_file}. Check file existence and permissions.", exc_info=True)
#                        elif error_obj.code == 1291: # ORA-01291: missing logfile
#                             logger.error(f"[LogMiner] Oracle reports missing archived log file (ORA-01291): {log_file}. Check registration and physical file.", exc_info=True)
#                        else:
#                            logger.error(f"[LogMiner] Error adding archived log {log_file}: {add_err}", exc_info=True)
#
#            except cx_Oracle.Error as query_err:
#                 logger.error(f"[LogMiner] Error querying V$ARCHIVED_LOG: {query_err}", exc_info=True)
#        else:
#             logger.warning("[LogMiner] start_scn is None, cannot query relevant archived logs.")
#
#
#        cursor.close()
 #       # *** MODIFY THIS PART ***
 #       if logs_added_count == 0:
 #            if start_scn is not None:
 #                logger.warning(f"[LogMiner] No new archived logs found covering or after SCN {start_scn}. LogMiner will not start this cycle.")
 #                # Return False or None to indicate no logs were added/session shouldn't start
 #                return False
 #            else:
 #                logger.error("[LogMiner] No log files could be added to the session (start SCN was None)! Cannot start LogMiner.")
 #                raise Exception("LogMiner failed: No log files could be added.")
 #       else:
 #            logger.info(f"[LogMiner] Successfully added {logs_added_count} archived log file(s) in total.")
 #            return True # Indicate logs were successfully added
 #       # *** END MODIFICATION ***

    def _start_logminer_session(self, start_scn: Optional[int] = None):
        """Starts the LogMiner session."""
        options = (
                cx_Oracle.STARTLOGMNR_DICT_FROM_ONLINE_CATALOG |
                cx_Oracle.STARTLOGMNR_COMMITTED_DATA_ONLY |
                cx_Oracle.STARTLOGMNR_CONTINUOUS_MINE
        )

        # For Oracle 12c+ include PDB awareness
        if self._is_pdb():
            options |= cx_Oracle.STARTLOGMNR_PDB

        if not self.conn:
            raise ConnectionError("Oracle connection not established.")
        if self._logminer_started:
            logger.warning("[LogMiner] Session already started, skipping.")
            return

        logger.info(f"[LogMiner] Attempting to add log files to session...")
        # *** CAPTURE RETURN VALUE ***
        logs_were_added = self._add_logfiles(start_scn)

        # *** ADD CHECK: Only start if logs were actually added ***
        if not logs_were_added:
            logger.warning(
                "[LogMiner] No relevant log files were added. LogMiner session will not be started in this cycle.")
            # Ensure internal state reflects that session didn't start
            self._logminer_started = False
            return  # Exit the start function gracefully
        # *** END CHECK ***

        cursor = self.conn.cursor()
        try:
            # Prepare options - Removed CONTINUOUS_MINE for testing maybe?
            # *** USE CORRECT INTEGER CONSTANTS ***
            # DICT_FROM_ONLINE_CATALOG = 8
            # COMMITTED_DATA_ONLY = 1
            # PRINT_PRETTY_SQL = 4
            # CONTINUOUS_MINE = 2 (Currently disabled)
            # NO_SQL_DELIMITER = 128
            #options = 8 | 1 | 4 | 128 # Combine options using bitwise OR
            # if you want to re-enable continuous mine: options |= 2
            # *** END CORRECTION ***

            # Construct the START_LOGMNR call
            sql = "BEGIN DBMS_LOGMNR.START_LOGMNR("
            params = {'options': options}
            if start_scn:
                sql += " STARTSCN => :start_scn,"
                params['start_scn'] = start_scn
            sql += " OPTIONS => :options"
            sql += "); END;"

            logger.warning(f"[LogMiner] Starting LogMiner session with SCN {start_scn} and options {options}... SQL: {sql} PARAMS: {params}") # Use WARNING for visibility
            cursor.execute(sql, params)
            self._logminer_started = True
            logger.info("[LogMiner] Session started successfully.")

        except cx_Oracle.DatabaseError as db_err:
            logger.error(f"[LogMiner] Error starting LogMiner session: {db_err}", exc_info=True) # Log full traceback
            # Log the added logs if available (requires fetching from V$LOGMNR_LOGS)
            try:
                 cursor.execute("SELECT filename, low_scn, high_scn, status FROM V$LOGMNR_LOGS")
                 added_logs = cursor.fetchall()
                 logger.error(f"[LogMiner] Logs currently added to failed session: {added_logs}")
            except Exception as log_query_err:
                 logger.error(f"[LogMiner] Could not query V$LOGMNR_LOGS after failure: {log_query_err}")
            raise # Re-raise the original error
        finally:
            cursor.close()

###########################################################################################
#    def _start_logminer_session(self, start_scn: int) -> None:
#        """Starts a LogMiner session if not already started."""
#        if not self.conn:
#            raise ConnectionError("Not connected to Oracle.")
#        if self._logminer_started:
#            # Potentially end and restart if SCN changes significantly? For now, assume it's okay.
#            return
#
#        try:
#            with self.conn.cursor() as cursor:
#                print(f"Starting LogMiner session from SCN: {start_scn}") # Replace with logging
#                # Options:
#                # - DICT_FROM_ONLINE_CATALOG: Easiest, but requires supplemental logging.
#                # - COMMITTED_DATA_ONLY: Shows only committed transactions.
#                # - DDL_DICT_TRACKING: Needed if you want to capture DDL via LogMiner (complex).
#                options = "DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG"
##                options = "DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG + DBMS_LOGMNR.COMMITTED_DATA_ONLY"
#                sql = f"""
#                    BEGIN
#                        DBMS_LOGMNR.START_LOGMNR(
#                            STARTSCN => :start_scn,
#                            OPTIONS  => {options}
#                        );
#                    END;
#                """
#                cursor.execute(sql, {'start_scn': start_scn})
#                self._logminer_started = True
#                print("LogMiner session started successfully.") # Replace with logging
#        except cx_Oracle.Error as e:
#            print(f"Error starting LogMiner session: {e}") # Replace with logging
#            raise


    def _is_pdb(self) -> bool:
        """Check if connected to a PDB"""
        with self.conn.cursor() as cursor:
            cursor.execute("SELECT CDB FROM V$DATABASE")
            return cursor.fetchone()[0] == 'YES'


    def get_changes(self, last_position: Optional[Dict[str, Any]]) -> tuple[List[Dict[str, Any]], Optional[Dict[str, Any]]]:
        """Fetch structured changes from Oracle LogMiner since the last SCN."""
        if not self.conn:
            raise ConnectionError("Not connected to Oracle.")

        logger.debug(f"[LogMiner] get_changes called. Last position: {last_position}")
        start_scn = last_position.get('scn', 0) if last_position else 0
        if start_scn == 0:
             current_pos = self.get_current_position()
             start_scn = current_pos.get('scn', 0) if current_pos else 0
             if start_scn == 0:
                  logger.warning("[LogMiner] Could not determine current SCN, attempting LogMiner from SCN 0 (may be inefficient).")

        changes = []
        max_scn_in_batch = start_scn # Initialize with start SCN

        try:
            # Attempt to start the session (this will now handle adding logs)
            self._start_logminer_session(start_scn)

            # *** If session didn't start (no logs added), return no changes ***
            if not self._logminer_started:
                logger.info("[LogMiner] get_changes: LogMiner session not started (no relevant logs found/added). Returning no changes for this cycle.")
                # Return empty list and the *original* position, as no progress was made
                return [], last_position
            # *** End Check ***

            # --- If session started, proceed to query V$LOGMNR_CONTENTS ---
            logger.info("[LogMiner] Querying V$LOGMNR_CONTENTS...")
            with self.conn.cursor() as cursor:
                # Example Query (adjust columns based on supplemental logging)
                #query = """
                #    SELECT SCN, TIMESTAMP, OPERATION_CODE, OPERATION, SEG_OWNER, TABLE_NAME, ROW_ID, CSF, SQL_REDO
                #    FROM V$LOGMNR_CONTENTS
                #    WHERE OPERATION_CODE IN (1, 2, 3) -- INSERT, DELETE, UPDATE
                #      -- Optional filtering (if replicating specific tables):
                #      -- AND (SEG_OWNER = 'YOUR_SCHEMA' AND TABLE_NAME = 'YOUR_TABLE')
                #      -- OR (SEG_OWNER = 'OTHER_SCHEMA' AND TABLE_NAME = 'OTHER_TABLE')
                #    ORDER BY SCN
                #"""
                query = f"""
                    SELECT OPERATION_CODE, SCN, SQL_REDO, TIMESTAMP,
                           SEG_OWNER, TABLE_NAME, ROW_ID
                    FROM V$LOGMNR_CONTENTS
                    WHERE SCN > :start_scn
                      AND OPERATION_CODE IN (1,2,3)
                      AND SEG_OWNER NOT LIKE 'SYS%'
                    ORDER BY SCN
                """

                cursor.execute(query) # No start_scn needed here, START_LOGMNR defined the range

                # Process rows (using simplified structure as before)
                for row in cursor:
                    scn, ts, op_code, op_name_raw, schema, table_name, row_id, csf, sql_redo = row

                    # Basic Operation Mapping
                    if op_code == 1: op_name = 'insert'
                    elif op_code == 2: op_name = 'delete'
                    elif op_code == 3: op_name = 'update'
                    else: op_name = op_name_raw.lower()

                    # *** TODO: Enhance data extraction based on actual supplemental logging ***
                    # You MUST configure supplemental logging on source tables for PKs (and ideally changed columns)
                    # Then, modify the query above to select those columns directly from V$LOGMNR_CONTENTS
                    # Example (assuming PK column 'ID' and data column 'VALUE' are logged):
                    # SELECT SCN, ..., ID, VALUE, ... FROM V$LOGMNR_CONTENTS WHERE ...
                    # Then extract:
                    # primary_keys = {'ID': row[index_of_id]}
                    # after_data = {'ID': row[index_of_id], 'VALUE': row[index_of_value]}
                    # before_data = ... (requires logging before image or parsing SQL_REDO/UNDO)

                    # --- Placeholder Data (REMOVE THIS IN PRODUCTION) ---
                    primary_keys = {'placeholder_pk': f'pk_at_scn_{scn}'}
                    before_data = {} if op_name == 'insert' else {'pk': f'pk_at_scn_{scn}'}
                    after_data = {} if op_name == 'delete' else {'pk': f'pk_at_scn_{scn}', 'data': 'data_at_scn_{scn}'}
                    # --- End Placeholder ---

                    change_event = {
                        'source': 'oracle_logminer', 'position': {'scn': scn}, 'timestamp': ts,
                        'operation': op_name, 'schema': schema, 'table': table_name,
                        'primary_keys': primary_keys, 'before_data': before_data, 'after_data': after_data,
                        '_metadata': { 'row_id': row_id, 'csf': csf, 'raw_sql': sql_redo }
                    }
                    changes.append(change_event)

                    if scn > max_scn_in_batch: max_scn_in_batch = scn

                logger.info(f"[LogMiner] Fetched {len(changes)} change events from V$LOGMNR_CONTENTS.")

        except Exception as e:
            logger.error(f"[LogMiner] Error during get_changes (querying V$LOGMNR_CONTENTS or processing): {e}", exc_info=True)
            # Ensure LogMiner session is stopped if an error occurs during processing
            self._end_logminer_session()
            raise # Re-raise the exception to fail the task

        # Determine the new position
        new_position = {'scn': max_scn_in_batch} if max_scn_in_batch > start_scn else last_position
        logger.debug(f"[LogMiner] get_changes finished. Returning {len(changes)} changes. New position: {new_position}")
        return changes, new_position

    def _end_logminer_session(self):
        """Stops the current LogMiner session."""
        if self.conn and self._logminer_started:
            try:
                logger.info("[LogMiner] Ending LogMiner session...")
                with self.conn.cursor() as cursor:
                    cursor.execute("BEGIN DBMS_LOGMNR.END_LOGMNR(); END;")
                self._logminer_started = False
                logger.info("[LogMiner] Session ended successfully.")
            except cx_Oracle.Error as e:
                 # ORA-01307: no LogMiner session is active - Ignore safely
                 error_obj, = e.args
                 if error_obj.code == 1307:
                     logger.warning("[LogMiner] Attempted to end session, but none was active (ORA-01307).")
                     self._logminer_started = False # Ensure state is correct
                 else:
                     logger.error(f"[LogMiner] Error ending LogMiner session: {e}", exc_info=True)
                     # Don't prevent disconnect, just log error
        else:
             logger.debug("[LogMiner] No active session or connection to end.")


    def disconnect(self) -> None:
        # ... (Ensure disconnect calls _end_logminer_session before closing connection) ...
        if self._logminer_started:
            self._end_logminer_session()
        if self.conn:
            try:
                self.conn.close()
                logger.warning("Disconnected from Oracle.") # Use warning for visibility
            except cx_Oracle.Error as e:
                 logger.error(f"Error closing Oracle connection: {e}", exc_info=True)
            finally:
                 self.conn = None
        else:
             logger.info("Oracle connection already closed.")

    # ... (keep get_current_position) ...
    def get_current_position(self) -> Optional[Dict[str, Any]]:
        """Gets the current SCN from the database."""
        if not self.conn: raise ConnectionError("Not connected to Oracle.")
        try:
            with self.conn.cursor() as cursor:
                cursor.execute("SELECT current_scn FROM v$database")
                result = cursor.fetchone()
                return {'scn': result[0]} if result else None
        except cx_Oracle.Error as e:
            logger.error(f"Error fetching current SCN: {e}", exc_info=True)
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