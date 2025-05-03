# logminer.py (Refactored)

import time  # Add this import at the top with other imports
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, Generator # Added Generator

# Assuming interfaces.py is in the same directory or adjust import path
from app.interfaces import SourceConnector

import oracledb as cx_Oracle
# Print the version being used
print(f"Using oracledb version: {cx_Oracle.__version__}")

# Placeholder - remove if psycopg2 is not needed here anymore
# import psycopg2
# *** Add logging import ***
import logging
logger = logging.getLogger(__name__) # Use standard Python logging

# LogMiner options compatibility layer
try:
    # New oracledb (>= 1.0) constant names
    LOGMINER_DICT_FROM_ONLINE_CATALOG = cx_Oracle.LOGMINER_DICT_FROM_ONLINE_CATALOG
    LOGMINER_COMMITTED_DATA_ONLY = cx_Oracle.LOGMINER_COMMITTED_DATA_ONLY
    LOGMINER_PRINT_PRETTY_SQL = cx_Oracle.LOGMINER_PRINT_PRETTY_SQL
    LOGMINER_CONTINUOUS_MINE = cx_Oracle.LOGMINER_CONTINUOUS_MINE
except AttributeError:
    try:
        # Old cx_Oracle constant names
        LOGMINER_DICT_FROM_ONLINE_CATALOG = cx_Oracle.STARTLOGMNR_DICT_FROM_ONLINE_CATALOG
        LOGMINER_COMMITTED_DATA_ONLY = cx_Oracle.STARTLOGMNR_COMMITTED_DATA_ONLY
        LOGMINER_PRINT_PRETTY_SQL = cx_Oracle.STARTLOGMNR_PRINT_PRETTY_SQL
        LOGMINER_CONTINUOUS_MINE = cx_Oracle.STARTLOGMNR_CONTINUOUS_MINE
    except AttributeError:
        # Fallback to raw values if neither works
        LOGMINER_DICT_FROM_ONLINE_CATALOG = 8
        LOGMINER_COMMITTED_DATA_ONLY = 1
        LOGMINER_PRINT_PRETTY_SQL = 4
        LOGMINER_CONTINUOUS_MINE = 2

class OracleLogMinerConnector(SourceConnector):
    """
    SourceConnector implementation for Oracle using LogMiner.
    """

    def __init__(self):
        """Initialize the connector."""
        self.conn: Optional[cx_Oracle.Connection] = None
        self.config: Dict[str, Any] = {}
        self._logminer_started: bool = False
        self.logger = logging.getLogger(__name__)  # Add this line

    def connect(self, config: Dict[str, Any]) -> None:
        """Establish connection to Oracle XE."""
        if self.conn:
            self.disconnect()

        self.config = config
        try:
            port = int(config.get('port', 1521))
            dsn = cx_Oracle.makedsn(
                config['host'],
                port,
                service_name=config.get('service_name', 'XEPDB1')
            )
            logger.info(f"Connect : user :{config['username']},service_name :{config['service_name']}  to Oracle XE database")
#            self.conn = cx_Oracle.connect(
#                user=config['username'],
#                password=config['password'],
#                dsn=dsn,
#                mode=cx_Oracle.SYSDBA
#            )
            self.conn = cx_Oracle.connect(
                user="C##REP_USER",
                password="rep_user",
                dsn=dsn,
                mode=cx_Oracle.SYSDBA
            )

            self.conn.autocommit = False
            logger.info("Connected to Oracle XE database")
            # Set container context immediately after connecting
#            with self.conn.cursor() as cursor:
#                try:
#                    cursor.execute("ALTER SESSION SET CONTAINER = CDB$ROOT")
#                    logger.info("Connected to Oracle XE in CDB mode")
#                except cx_Oracle.DatabaseError as e:
#                    logger.warning("Could not set CDB$ROOT container (non-CDB mode?)")
#                    # Continue with regular connection if not in CDB mode

        except cx_Oracle.Error as e:
            logger.error(f"Connection failed: {e}")
            raise

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
    def _add_logfiles(self, start_scn: int, end_scn: int) -> bool:
        """Add both archived and online logs containing the SCN range."""
        if not self.conn:
            return False

        try:
            with self.conn.cursor() as cursor:
                archived_added = self._add_archived_logs(cursor, start_scn, end_scn)
                online_added = self._add_online_logs(cursor, start_scn, end_scn)

                # If no logs found, try forcing archive log generation
                if archived_added + online_added == 0:
                    self.logger.warning("No logs found for SCN range, forcing archive log generation")
                    self.force_log_switch()
                    time.sleep(2)  # Give time for archiving to complete
                    archived_added = self._add_archived_logs(cursor, start_scn, end_scn)
                    online_added = self._add_online_logs(cursor, start_scn, end_scn)

                self.logger.info(
                    f"Total logs added: {archived_added + online_added} "
                    f"(archived: {archived_added}, online: {online_added})"
                )
                return (archived_added + online_added) > 0

        except cx_Oracle.Error as e:
            self.logger.error(f"Error adding log files: {e}")
            return False

    # Add new method for PDB log handling
    def _add_pdb_logs(self, cursor, start_scn) -> int:
        """Add logs in a PDB environment using CDB-level views."""
        logs_added = 0

        try:
            # Query CDB-level archived logs filtered by current PDB's CON_ID
            cursor.execute("""
                SELECT name, first_change#, next_change#
                FROM CDB_ARCHIVED_LOG
                WHERE :scn BETWEEN first_change# AND next_change#
                   OR next_change# > :scn
                   AND con_id = SYS_CONTEXT('USERENV', 'CON_ID')
                   AND name IS NOT NULL
                ORDER BY first_change#
            """, {'scn': start_scn})

            for log_file, first_scn, next_scn in cursor:
                try:
                    add_option = 2 if logs_added > 0 else 1
                    # Use CDB-level path mapping
                    cursor.execute(
                        "BEGIN DBMS_LOGMNR.ADD_LOGFILE(:log_name, :add_opt); END;",
                        {'log_name': log_file.replace('CDB$ROOT/', ''),  # Adjust path if needed
                         'add_opt': add_option}
                    )
                    logs_added += 1
                    self.logger.info(f"Added PDB archived log: {log_file}")
                except cx_Oracle.DatabaseError as e:
                    if e.args[0].code != 1289:  # Ignore "already added" errors
                        raise

            return logs_added

        except cx_Oracle.Error as e:
            self.logger.error(f"Error querying CDB_ARCHIVED_LOG: {e}")
            return 0

    def _add_archived_logs(self, cursor, start_scn: int, end_scn: int) -> int:
        """Add archived logs containing the SCN range."""
        logs_added = 0
        try:
            # First try with exact SCN range matching
            query = """
                SELECT name, first_change#, next_change#
                FROM v$archived_log
                WHERE ((:start_scn BETWEEN first_change# AND next_change#)
                   OR (:end_scn BETWEEN first_change# AND next_change#)
                   OR (first_change# BETWEEN :start_scn AND :end_scn)
                   OR (next_change# BETWEEN :start_scn AND :end_scn))
                   AND name IS NOT NULL
                   AND status = 'A'
                   AND deleted = 'NO'
                ORDER BY first_change#
            """

            # Execute with explicit output type handling
            cursor.execute(query, {'start_scn': start_scn, 'end_scn': end_scn})

            # Check if we got results
            if cursor.rowcount == 0:
                # Fallback to broader query
                query = """
                    SELECT name, first_change#, next_change#
                    FROM v$archived_log
                    WHERE next_change# >= :start_scn
                    AND name IS NOT NULL
                    AND status = 'A'
                    AND deleted = 'NO'
                    ORDER BY first_change#
                """
                cursor.execute(query, {'start_scn': start_scn})

            # Process results
            while True:
                row = cursor.fetchone()
                if not row:
                    break

                log_file, first_scn, next_scn = row
                try:
                    add_option = 2 if logs_added > 0 else 1
                    cursor.execute(
                        "BEGIN DBMS_LOGMNR.ADD_LOGFILE(:log_name, :add_opt); END;",
                        {'log_name': log_file, 'add_opt': add_option}
                    )
                    logs_added += 1
                    self.logger.info(f"Added archived log: {log_file} (SCN range: {first_scn}-{next_scn})")
                except cx_Oracle.DatabaseError as e:
                    if e.args[0].code != 1289:  # Ignore "already added" errors
                        raise

            return logs_added

        except cx_Oracle.Error as e:
            self.logger.error(f"Error querying V$ARCHIVED_LOG: {e}", exc_info=True)
            return 0

    def _add_online_logs(self, cursor, start_scn: int, end_scn: int) -> int:
        """Add online redo logs that may contain the SCN range."""
        logs_added = 0
        max_valid_scn = 2 ** 48  # Reasonable upper bound for SCN values

        try:
            # Get all online logs that might contain our SCN range
            cursor.execute("""
                SELECT group#, sequence#, status, first_change#, next_change#
                FROM v$log 
                WHERE status IN ('CURRENT','ACTIVE','UNUSED')
                AND first_change# < next_change#
                AND ((:start_scn BETWEEN first_change# AND next_change#)
                    OR (:end_scn BETWEEN first_change# AND next_change#)
                    OR (first_change# BETWEEN :start_scn AND :end_scn)
                    OR (next_change# BETWEEN :start_scn AND :end_scn))
                ORDER BY first_change#
            """, {'start_scn': start_scn, 'end_scn': end_scn})

            for group_id, seq, status, first_scn, next_scn in cursor:
                # Skip logs with invalid SCN ranges
                if next_scn >= max_valid_scn:
                    self.logger.warning(
                        f"Skipping log group {group_id} with invalid SCN range: {first_scn}-{next_scn}"
                    )
                    continue

                try:
                    # Get log file members for this group
                    cursor.execute("""
                        SELECT member FROM v$logfile 
                        WHERE group# = :group_id
                        AND type = 'ONLINE'
                        AND status = 'VALID'
                    """, {'group_id': group_id})

                    for (log_file,) in cursor:
                        try:
                            add_option = 2 if logs_added > 0 else 1
                            cursor.execute(
                                "BEGIN DBMS_LOGMNR.ADD_LOGFILE(:log_name, :add_opt); END;",
                                {'log_name': log_file, 'add_opt': add_option}
                            )
                            logs_added += 1
                            self.logger.info(
                                f"Added online log: {log_file} "
                                f"(group {group_id}, seq {seq}, status {status}, SCN range: {first_scn}-{next_scn})"
                            )
                        except cx_Oracle.DatabaseError as e:
                            if e.args[0].code != 1289:  # Ignore "already added" errors
                                raise
                except cx_Oracle.Error as e:
                    self.logger.error(f"Error processing log group {group_id}: {e}")
                    continue

            return logs_added

        except cx_Oracle.Error as e:
            self.logger.error(f"Error querying online logs: {e}")
            return 0

    def force_log_switch(self):
        """Force a log switch to ensure changes are archived."""
        if not self.conn:
            return False

        try:
            with self.conn.cursor() as cursor:
                # First try in current container context
                try:
                    cursor.execute("ALTER SYSTEM ARCHIVE LOG CURRENT")
                    self.logger.info("Forced archive log generation in current context")
                    return True
                except cx_Oracle.DatabaseError as e:
                    self.logger.warning(f"Could not force log switch in current context: {e}")

                # If that fails, try switching to CDB$ROOT
                try:
                    cursor.execute("ALTER SESSION SET CONTAINER = CDB$ROOT")
                    cursor.execute("ALTER SYSTEM ARCHIVE LOG CURRENT")
                    self.logger.info("Forced archive log generation in CDB$ROOT")
                    return True
                except cx_Oracle.DatabaseError as e:
                    self.logger.error(f"Could not force log switch in CDB$ROOT: {e}")
                    return False
        except cx_Oracle.Error as e:
            self.logger.error(f"Error forcing log switch: {e}")
            return False

    # Update _start_logminer_session to handle CDB/PDB context
    def _start_logminer_session(self, start_scn: int) -> bool:
        """Start LogMiner with CDB awareness"""
        if not self.conn:
            return False

        try:
            with self.conn.cursor() as cursor:
                # Get current SCN to use as end point
                cursor.execute("SELECT current_scn FROM v$database")
                current_scn = cursor.fetchone()[0]

                # Validate SCN range
                if not self._validate_scn_range(start_scn):
                    self.logger.warning(f"Using current SCN {current_scn} instead of {start_scn}")
                    start_scn = current_scn

                if not self.validate_logminer_privileges():
                    raise RuntimeError("Missing required LogMiner privileges")

                # Log current log status for debugging
                self._log_available_logs()

                # Try adding logs multiple times if needed
                max_attempts = 3
                for attempt in range(max_attempts):
                    if self._add_logfiles(start_scn, current_scn):
                        if self._verify_log_files(cursor, start_scn, current_scn):
                            break
                    if attempt < max_attempts - 1:
                        self.logger.info(f"Force log switch and retry (attempt {attempt + 1})")
                        self.force_log_switch()
                        time.sleep(5)  # Increased delay for archiving to complete
                else:
                    self.logger.error("Failed to add valid log files after multiple attempts")
                    return False

                options = (
                        LOGMINER_DICT_FROM_ONLINE_CATALOG |
                        LOGMINER_COMMITTED_DATA_ONLY |
                        LOGMINER_PRINT_PRETTY_SQL |
                        LOGMINER_CONTINUOUS_MINE
                )

                self.logger.info(f"Starting LogMiner with SCN range: {start_scn}-{current_scn}, options: {options}")
                cursor.callproc("DBMS_LOGMNR.START_LOGMNR", [
                    int(start_scn),
                    int(current_scn),  # end_scn
                    None,  # start_time
                    None,  # end_time
                    None,  # dict_filename
                    options
                ])

                self._logminer_started = True
                self.logger.info("LogMiner session started successfully")
                return True

        except cx_Oracle.Error as e:
            self.logger.error(f"Failed to start LogMiner session: {e}", exc_info=True)
            return False

    def _log_available_logs(self):
        """Log detailed information about available logs."""
        if not self.conn:
            return

        try:
            with self.conn.cursor() as cursor:
                # Current SCN
                cursor.execute("SELECT current_scn FROM v$database")
                current_scn = cursor.fetchone()[0]
                self.logger.info(f"Current database SCN: {current_scn}")

                # Archived logs
                cursor.execute("""
                    SELECT name, first_change#, next_change#, sequence#, status
                    FROM v$archived_log
                    WHERE name IS NOT NULL
                    AND status = 'A'
                    AND deleted = 'NO'
                    ORDER BY first_change#
                """)
                self.logger.info("Archived logs available:")
                for row in cursor:
                    self.logger.info(f"  {row[0]} (SCN: {row[1]}-{row[2]}, Seq: {row[3]}, Status: {row[4]})")

                # Online logs
                cursor.execute("""
                    SELECT group#, sequence#, status, first_change#, next_change#
                    FROM v$log
                    WHERE status IN ('CURRENT','ACTIVE','UNUSED')
                    ORDER BY group#
                """)
                self.logger.info("Online redo logs available:")
                for row in cursor:
                    self.logger.info(f"  Group {row[0]}, Seq {row[1]}, Status {row[2]}, SCN: {row[3]}-{row[4]}")

        except cx_Oracle.Error as e:
            self.logger.error(f"Error querying log information: {e}")

    def _validate_log_files(self, start_scn: int) -> bool:
        """Verify that logs containing the SCN are available."""
        with self.conn.cursor() as cursor:
            cursor.execute("""
                SELECT COUNT(*) FROM v$archived_log
                WHERE :scn BETWEEN first_change# AND next_change#
                   OR next_change# > :scn
            """, {'scn': start_scn})
            count = cursor.fetchone()[0]
            return count > 0

    def _check_scn_range(self, start_scn: int) -> bool:
        """Check if SCN is within valid range."""
        with self.conn.cursor() as cursor:
            cursor.execute("SELECT MIN(first_change#), MAX(next_change#) FROM v$archived_log")
            min_scn, max_scn = cursor.fetchone()
            return min_scn <= start_scn <= max_scn if min_scn and max_scn else False

    def old_start_logminer_session(self, start_scn: Optional[int] = None):
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

    def get_changes(self, last_position: Optional[Dict[str, Any]]) -> Tuple[
        List[Dict[str, Any]], Optional[Dict[str, Any]]]:
        if not self.conn:
            raise ConnectionError("Not connected to Oracle.")

        start_scn = last_position.get('scn', 0) if last_position else 0
        changes = []
        max_scn = start_scn

        try:
            self.logger.info(f"Starting LogMiner session from SCN: {start_scn}")
            if not self._start_logminer_session(start_scn):
                # If failed to start, try with current SCN
                current_scn = self.get_current_position().get('scn')
                if current_scn and current_scn != start_scn:
                    self.logger.info(f"Retrying with current SCN: {current_scn}")
                    if not self._start_logminer_session(current_scn):
                        raise RuntimeError("Failed to start LogMiner session even with current SCN")
                    start_scn = current_scn
                else:
                    raise RuntimeError("Failed to start LogMiner session")

            with self.conn.cursor() as cursor:
                logger.info("Querying V$LOGMNR_CONTENTS...")
                cursor.execute("""
                    SELECT OPERATION_CODE, SCN, SQL_REDO, TIMESTAMP,
                           SEG_OWNER, TABLE_NAME, ROW_ID
                    FROM V$LOGMNR_CONTENTS
                    WHERE SCN > :start_scn
                      AND OPERATION_CODE IN (1,2,3)  -- INSERT, UPDATE, DELETE
                      AND SEG_OWNER NOT LIKE 'SYS%'
                    ORDER BY SCN
                """, {'start_scn': start_scn})

                logger.info(f"Found {cursor.rowcount} changes")
                for op_code, scn, sql_redo, ts, schema, table, row_id in cursor:
                    changes.append({
                        'scn': scn,
                        'timestamp': ts,
                        'operation': {1: 'insert', 2: 'delete', 3: 'update'}.get(op_code),
                        'schema': schema,
                        'table': table,
                        'sql': sql_redo
                    })
                    max_scn = max(max_scn, scn)

            new_position = {'scn': max_scn} if max_scn > start_scn else last_position
            return changes, new_position

        except cx_Oracle.Error as e:
            logger.error(f"Error getting changes: {e}")
            raise

    def validate_logminer_privileges(self) -> bool:
        """Comprehensive privilege validation with proper error handling"""
        checks = [
            ("DBMS_LOGMNR package",
             "SELECT 1 FROM all_objects WHERE owner='SYS' AND object_name='DBMS_LOGMNR'"),

#            ("V$LOGMNR_CONTENTS access",
#             "SELECT 1 FROM v$logmnr_contents WHERE ROWNUM=1"),

            ("ARCHIVED_LOG access",
             "SELECT 1 FROM v$archived_log WHERE ROWNUM=1"),

            ("LOGMINING privilege",
             "SELECT 1 FROM session_privs WHERE privilege='LOGMINING'")
        ]

        try:
            with self.conn.cursor() as cursor:
                # Ensure CDB$ROOT context
                try:
                    logger.info(f"Switching DB Container to CDB$ROOT .....")
                    cursor.execute("ALTER SESSION SET CONTAINER = CDB$ROOT")
                except cx_Oracle.DatabaseError as e:
                    logger.error(f"Container switch failed: {e}")
                    return False

                logger.info(f"Looping on privilege requirement list .....")
                for check_name, test_query in checks:
                    try:
                        logger.info(f"checking privilege {check_name} .....")
                        cursor.execute(test_query)
                        if not cursor.fetchone():
                            logger.error(f"Validation failed: {check_name}")
                            return False
                        logger.info(f"privilege {check_name} check Ok")
                    except cx_Oracle.DatabaseError as e:
                        logger.error(f"Privilege check failed for {check_name}: {e}")
                        return False

            return True

        except Exception as e:
            logger.error(f"Unexpected error during privilege validation: {e}")
            return False


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
                # Try with original case first
                logger.info(
                    f"get_table_schema : calling _get_table_columns schema_name : {schema_name}, table_name : {table_name}")
                columns = self._get_table_columns(cursor, schema_name, table_name)
                logger.info(
                    f"get_table_schema : _get_table_columns columns : {columns}")

                # If no columns found, try uppercase
                if not columns:
                    columns = self._get_table_columns(cursor, schema_name.upper(), table_name.upper())

                    if not columns:
                       raise ValueError(f"Table {schema_name}.{table_name} not found or no columns accessible.")

                table_def['columns'] = columns
                table_def['primary_key'] = self._get_primary_key_columns(cursor, schema_name, table_name)

            return table_def
        except cx_Oracle.Error as e:
            logger.error(f"Error fetching table schema for {schema_name}.{table_name}: {e}")  # Fixed logging here
            raise ValueError(f"Error fetching table schema for {schema_name}.{table_name}: {e}")

    def _get_table_columns(self, cursor, schema_name: str, table_name: str) -> List[Dict[str, Any]]:
        """Helper method to get columns with specific case handling"""
        query = """
            SELECT column_name, data_type, data_length, data_precision, data_scale, nullable
            FROM all_tab_columns
            WHERE owner = :owner AND table_name = :tbl
            ORDER BY column_id
        """
        logger.info(f"_get_table_columns query : for {query}")
        logger.info(f"_get_table_columns: query for {schema_name}.{table_name}")
        try:
            # Convert to uppercase to match Oracle's default case-insensitive behavior
            cursor.execute(query, {'owner': schema_name.upper(), 'tbl': table_name.upper()})
#            cursor.execute(query)
#            cols = [row for row in cursor]
#            logger.info(f"_get_table_columns: cols :  {cols}")
            return [{
                'name': col[0],
                'type': col[1],
                'length': col[2],
                'precision': col[3],
                'scale': col[4],
                'nullable': col[5] == 'Y'
            } for col in cursor]
        except cx_Oracle.Error as e:
            logger.error(f"Error fetching columns: {e}")
            return []

    def _get_primary_key_columns(self, cursor, schema_name: str, table_name: str) -> List[str]:
        """Helper method to get PK columns with case handling"""
        query = """
            SELECT cols.column_name
            FROM all_constraints cons
            JOIN all_cons_columns cols ON cons.constraint_name = cols.constraint_name
            WHERE cons.constraint_type = 'P'
            AND cons.owner = :owner
            AND cons.table_name = :tbl
            ORDER BY cols.position
        """
#        query = """
#            SELECT cols.column_name FROM all_constraints cons
#            JOIN all_cons_columns cols ON cons.constraint_name = cols.constraint_name
#            WHERE  cons.owner = 'AEF' AND cons.constraint_type = 'P' AND cons.table_name = 'AEF_TEST'
#            ORDER BY cols.position
#        """

        # Try original case first
        logger.info(f"_get_primary_key_columns Query Statement:  {query} for {schema_name}.{table_name} ")
        cursor.execute(query, {'owner': schema_name.upper(), 'tbl': table_name.upper()})
#        cursor.execute(query)
        logger.info(f"_get_primary_key_columns after execute Query Statement")
        pks = [row[0] for row in cursor]
        logger.info(f"_get_primary_key_columns:  {pks}")

        # If no PKs found, try uppercase
        if not pks:
            cursor.execute(query, {'schema': schema_name.upper(), 'table': table_name.upper()})
            pks = [row[0] for row in cursor]
            logger.info(f"_get_primary_key_columns second attempt:  {pks}")

        return pks

    def oldget_table_schema(self, schema_name: str, table_name: str) -> Dict[str, Any]:
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

    def _validate_scn_range(self, start_scn: int) -> bool:
        """Validate that the SCN is within available log range"""
        if not self.conn:
            return False

        try:
            with self.conn.cursor() as cursor:
                # Get current SCN
                cursor.execute("SELECT current_scn FROM v$database")
                current_scn = cursor.fetchone()[0]

                # Get oldest SCN in archive logs
                cursor.execute("""
                    SELECT MIN(first_change#), MAX(next_change#)
                    FROM v$archived_log 
                    WHERE name IS NOT NULL
                    AND first_change# > 0
                    AND status = 'A'
                    AND deleted = 'NO'
                """)
                min_scn, max_scn = cursor.fetchone()

                # Get online log range
                cursor.execute("""
                    SELECT MIN(first_change#), MAX(next_change#)
                    FROM v$log
                    WHERE status IN ('CURRENT','ACTIVE')
                    AND first_change# < next_change#
                    AND next_change# < POWER(2, 48)  -- Filter out invalid SCNs
                """)
                online_min, online_max = cursor.fetchone()

                # Determine overall min/max SCN
                min_scn = min(s for s in [min_scn, online_min] if s is not None)
                max_scn = max(s for s in [max_scn, online_max, current_scn] if s is not None)

                if start_scn < min_scn:
                    self.logger.warning(
                        f"Start SCN {start_scn} is older than oldest available SCN {min_scn}. "
                        f"Using current SCN {current_scn} instead."
                    )
                    return False

                if start_scn > max_scn:
                    self.logger.warning(
                        f"Start SCN {start_scn} is newer than newest available SCN {max_scn}. "
                        f"Using current SCN {current_scn} instead."
                    )
                    return False

                return True

        except cx_Oracle.Error as e:
            self.logger.error(f"Error validating SCN range: {e}")
            return False

    def _verify_log_files(self, cursor, start_scn: int, end_scn: int) -> bool:
        """Verify that the log files we added actually cover our SCN range."""
        try:
            # Check what logs are registered with LogMiner
            cursor.execute("""
                SELECT MIN(first_change#), MAX(next_change#)
                FROM v$logmnr_logs
                WHERE name IS NOT NULL
            """)
            result = cursor.fetchone()

            if not result or result[0] is None:
                self.logger.error("No log files registered with LogMiner")
                return False

            min_scn, max_scn = result

            if start_scn < min_scn or end_scn > max_scn:
                self.logger.error(
                    f"Log files don't cover required SCN range. "
                    f"Available: {min_scn}-{max_scn}, Needed: {start_scn}-{end_scn}"
                )
                return False

            return True

        except cx_Oracle.Error as e:
            self.logger.error(f"Error verifying log files: {e}")
            return False

# --- Remove the old PostgresCDCHandler unless you plan to refactor it immediately ---
# class PostgresCDCHandler:
#    ...