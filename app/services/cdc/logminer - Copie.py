import cx_Oracle
from datetime import datetime

import psycopg2


import cx_Oracle
import psycopg2
from datetime import datetime


class OracleLogMiner:
    """Handles Oracle LogMiner-based CDC."""

    def __init__(self, connection_config):
        self.conn = cx_Oracle.connect(
            user=connection_config['user'],
            password=connection_config['password'],
            dsn=f"{connection_config['host']}:{connection_config['port']}/{connection_config['service']}"
        )

    def get_changes(self, start_scn, tables, app):
        """Fetch changes from Oracle LogMiner."""
        try:
            if not tables:
                raise ValueError("At least one table must be selected for replication")

            with self.conn.cursor() as cursor:
                # Start LogMiner session
                cursor.execute("""
                    BEGIN 
                        DBMS_LOGMNR.START_LOGMNR(
                            STARTSCN => :start_scn,
                            OPTIONS  => DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG
                        );
                    END;
                """, {'start_scn': start_scn})

                app.logger.info(f"LogMiner started SCN: {start_scn}")

                # Fetch primary key mappings
                pk_columns_map = self.get_primary_keys(tables)

                changes = []

                for table in tables:
                    schema, table_name = table['schema'], table['table']
                    pk_columns = pk_columns_map.get(table_name, [])

                    if not pk_columns:
                        app.logger.warning(f"No primary key found for {schema}.{table_name}, skipping PK extraction.")

                    # Build SQL query to include PK values dynamically
                    pk_select = ", ".join(pk_columns) if pk_columns else "NULL AS pk_value"
                    query = f"""
                        SELECT SCN, SQL_REDO, OPERATION, TABLE_NAME, {pk_select}
                        FROM V$LOGMNR_CONTENTS
                        WHERE SEG_OWNER = :schema AND TABLE_NAME = :table
                        AND OPERATION IN ('INSERT', 'UPDATE', 'DELETE')
                    """

                    app.logger.debug(f"Executing LogMiner query: {query}")

                    cursor.execute(query, {'schema': schema, 'table': table_name})

                    for row in cursor:
                        change_record = {
                            'scn': row[0],
                            'sql': row[1],
                            'operation': row[2].lower(),
                            'table': row[3],
                            'pk_values': {col: row[idx + 4] for idx, col in enumerate(pk_columns)},
                            'timestamp': datetime.now()
                        }
                        changes.append(change_record)

                # Stop LogMiner session to free resources
                cursor.execute("BEGIN DBMS_LOGMNR.END_LOGMNR(); END;")

                return changes

        except cx_Oracle.DatabaseError as e:
            app.logger.error(f"Oracle Database Error: {str(e)}")
            return []

    def get_primary_keys(self, tables):
        """Fetch primary key columns dynamically."""
        with self.conn.cursor() as cursor:
            pk_columns_map = {}

            for table in tables:
                schema, table_name = table['schema'], table['table']
                cursor.execute("""
                    SELECT cols.column_name
                    FROM all_cons_columns cols
                    JOIN all_constraints cons 
                    ON cols.constraint_name = cons.constraint_name
                    WHERE cons.constraint_type = 'P'
                    AND cols.owner = :schema
                    AND cols.table_name = :table
                """, {'schema': schema, 'table': table_name})

                pk_columns_map[table_name] = [row[0] for row in cursor]

        return pk_columns_map

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