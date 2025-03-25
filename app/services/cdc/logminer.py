import cx_Oracle
from datetime import datetime


class OracleLogMiner:
    """Handles Oracle LogMiner-based CDC."""

    def __init__(self, connection_config):
        self.conn = cx_Oracle.connect(
            user=connection_config['user'],
            password=connection_config['password'],
            dsn=f"{connection_config['host']}:{connection_config['port']}/{connection_config['service']}"
        )

    def get_changes(self, start_scn, tables,app):
        """Fetch changes from Oracle LogMiner."""
        with self.conn.cursor() as cursor:
            cursor.execute(f"""
                BEGIN DBMS_LOGMNR.START_LOGMNR(
                    STARTSCN => {start_scn},
                    OPTIONS => DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG
                );
                END;""")
            app.logger.info(f"LogMiner started SCN: {start_scn}")

            query = f"""
                SELECT SCN, SQL_REDO, OPERATION, TABLE_NAME
                FROM V$LOGMNR_CONTENTS
                WHERE {' OR '.join([
                f"(SEG_OWNER='{t['schema']}' AND TABLE_NAME='{t['table']}')"
                for t in tables
            ])}
                AND OPERATION IN ('INSERT', 'UPDATE', 'DELETE')
            """
            changes = []
            cursor.execute(query)
            for row in cursor:
                changes.append({
                    'scn': row[0],
                    'sql': row[1],
                    'operation': row[2].lower(),
                    'table': row[3]
                })
                app.logger.info(f"LogMiner change detected: {row[1]}")  # <-- Add this line

#            return self._parse_results(cursor)
            return changes

    def _parse_results(self, cursor):
        """Parse LogMiner results into a structured format."""
        return [{
            'scn': row[0],
            'sql': row[1],
            'operation': row[2].lower(),
            'table': row[3],
            'timestamp': datetime.now()
        } for row in cursor]
class PostgresCDCHandler:
    def __init__(self, connection_config):
        self.conn = psycopg2.connect(
            dbname=connection_config['database'],
            user=connection_config['user'],
            password=connection_config['password'],
            host=connection_config['host'],
            port=connection_config['port']
        )

    def get_changes(self, start_lsn, tables):
        # PostgreSQL-specific CDC logic
        pass