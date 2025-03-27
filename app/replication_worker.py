import hashlib
from datetime import datetime
import time
from app import db, create_app
from app.models import ReplicationTask
from app.services.metadata_service import MetadataService,perform_initial_load
from datetime import datetime, timezone
import time
from app.services.cdc.logminer import OracleLogMiner  # Import LogMiner


def generate_hash(data):
    return hashlib.sha256(str(data).encode()).hexdigest()


def run_replication(task_id, initial_load=False, reload=False, start_datetime=None):
    app = create_app()
    with app.app_context():
        task = ReplicationTask.query.get(task_id)
        if not task:
            app.logger.error(f"Task {task_id} not found.")
            return

        source = task.source
        destination = task.destination

        # Validate source and destination endpoints
        if source.endpoint_type != 'source':
            raise ValueError("Source endpoint must be of type 'source'.")
        if destination.endpoint_type != 'target':
            raise ValueError("Destination endpoint must be of type 'target'.")

        # Use target_schema in the replication logic
        target_schema = destination.target_schema
        if not target_schema:
            raise ValueError("Target schema is not specified for the destination endpoint.")

        # Initialize LogMiner
        logminer_config = {
            'user': source.username,
            'password': source.password,
            'host': source.host,
            'port': source.port,
            'service': source.service_name
        }
        logminer = OracleLogMiner(logminer_config)

        # Perform initial load if required
        if initial_load or reload:
            app.logger.info(f"Performing initial load for task {task_id}.")
            tables_to_replicate = task.tables
            if not tables_to_replicate:
                raise ValueError("No tables selected for replication.")

            for full_table_name in tables_to_replicate:
                source_schema, table_name = full_table_name.split('.', 1)
                if not MetadataService.is_table_exists(destination, target_schema, table_name):
                    success = MetadataService.create_tables_if_not_exists(
                        source_endpoint=source,
                        target_endpoint=destination,
                        source_schema=source_schema,
                        target_schema=target_schema,
                        table_name=table_name
                    )
                    if success:
                        perform_initial_load(
                            source_endpoint=source,
                            target_endpoint=destination,
                            source_schema=source_schema,
                            target_schema=target_schema,
                            table_name=table_name
                        )

        # Start incremental replication
        task.status = 'running'
        db.session.commit()

        try:
            while task.status == 'running':
                start_time = time.time()

                # Get the last SCN from the task
                last_scn = task.metrics.get('last_scn', 0)

                # Fetch changes from LogMiner
                # Fetch changes from LogMiner
                # In replication_worker.py, add debug logs:

                changes = logminer.get_changes(
                    start_scn=last_scn,
                    tables=[{'schema': table.split('.')[0], 'table': table.split('.')[1]} for table in task.tables],
                    app=app
                )
                app.logger.info(f"Last SCN: {last_scn}")
                app.logger.info(f"New changes SCN range: {[change['scn'] for change in changes]}")
                app.logger.info(f"New changes : {[change for change in changes]}")

                if changes:
                    # Apply changes to target
                    apply_changes_to_target(changes, destination, target_schema,task,app)

                    # Update metrics
                    task.metrics['last_scn'] = changes[-1]['scn']
                    task.metrics['last_updated'] = datetime.now(timezone.utc).isoformat()

                    # Count operations
                    for change in changes:
                        op = change['operation']
                        if op == 'insert':
                            task.metrics['inserts'] += 1
                        elif op == 'update':
                            task.metrics['updates'] += 1
                        elif op == 'delete':
                            task.metrics['deletes'] += 1

                    db.session.commit()

                # Simulate a delay to mimic real-world replication
                time.sleep(1)

        except Exception as e:
            app.logger.error(f"Task {task_id} failed: {str(e)}")
            task.status = 'failed'
            db.session.commit()
        finally:
            if task.status != 'failed':
                task.status = 'stopped'
                db.session.commit()
            app.logger.info(f"Task {task_id} has been stopped.")

def oldrun_replication(task_id, initial_load=False, reload=False, tables_to_reload=None):
    app = create_app()
    with app.app_context():
        task = ReplicationTask.query.get(task_id)
        if not task:
            app.logger.error(f"Task {task_id} not found.")
            return

        source = task.source
        destination = task.destination

        # Validate source and destination endpoints
        if source.endpoint_type != 'source':
            raise ValueError("Source endpoint must be of type 'source'.")
        if destination.endpoint_type != 'target':
            raise ValueError("Destination endpoint must be of type 'target'.")

        # Use target_schema in the replication logic
        target_schema = destination.target_schema
        if not target_schema:
            raise ValueError("Target schema is not specified for the destination endpoint.")

        # Create schema if it doesn't exist
        MetadataService.create_schema_if_not_exists(destination, target_schema)
        # Add table creation logic for initial load
        if initial_load:
            app.logger.info(f"initial_load flag set to {initial_load}.")

            # Get the list of tables selected for replication
            tables_to_replicate = task.tables  # Ensure this field contains the selected tables
            if not tables_to_replicate:
                raise ValueError("No tables selected for replication.")
            # Get tables from the task (format: "schema.table")
            tables_to_replicate = task.tables  # e.g., ["AEF.AEF_TEST", "AEF.ANOTHER_TABLE"]

            for full_table_name in tables_to_replicate:
                # Split into schema and table name
                if '.' not in full_table_name:
                    raise ValueError(f"Invalid table name format: {full_table_name}. Expected 'schema.table'.")
                source_schema, table_name = full_table_name.split('.', 1)  # Split on the first dot
                if not MetadataService.is_table_exists(destination,target_schema,table_name):
                    # Call create table
                    app.logger.info(f" table {target_schema}.{table_name} doesn't exist....")

                    success = MetadataService.create_tables_if_not_exists(
                       source_endpoint=source,
                       target_endpoint=destination,
                       source_schema=source_schema,  # Extracted schema (e.g., "AEF")
                       target_schema=target_schema,  # Target schema (e.g., "AEF_TRGT")
                       table_name=table_name  # Extracted table name (e.g., "AEF_TEST")
                              )
                    if success:
                        app.logger.info(f"table {target_schema}.{table_name} created successfully.")
                        # perform an initial load
                        success_init_flag = perform_initial_load(
                              source_endpoint=source,
                              target_endpoint=destination,
                              source_schema=source_schema,
                              target_schema=target_schema,
                              table_name=table_name
                              )
                        if success_init_flag:
                            app.logger.info(
                                  f"Initial load for table {target_schema}.{table_name} completed successfully.")
                        else:
                              app.logger.info(
                                  f"Initial load for table {target_schema}.{table_name} Failed.")
                    else:
                          raise Exception(f"Failed to create table {table_name} in target schema.")
        else:
            # Add new tables and perform an initial load of new tables
            if task.tables:
                for full_table_name in task.tables:
                    # Split into schema and table name
                    if '.' not in full_table_name:
                        raise ValueError(f"Invalid table name format: {full_table_name}. Expected 'schema.table'.")
                    source_schema, table_name = full_table_name.split('.', 1)  # Split on the first dot
                    if not MetadataService.is_table_exists(destination, target_schema, table_name):
                        # Call table creation for each table
                        success = MetadataService.create_tables_if_not_exists(
                            source_endpoint=source,
                            target_endpoint=destination,
                            source_schema=source_schema,  # Extracted schema (e.g., "AEF")
                            target_schema=target_schema,  # Target schema (e.g., "AEF_TRGT")
                            table_name=table_name  # Extracted table name (e.g., "AEF_TEST")
                        )
                        # perform an initial load
                        if success:
                            app.logger.info(
                                f"Initial load for table {target_schema}.{table_name} completed successfully.")
                            success_init_flag = perform_initial_load(
                                source_endpoint=source,
                                target_endpoint=destination,
                                source_schema=source_schema,
                                target_schema=target_schema,
                                table_name=table_name
                            )
                            if success_init_flag:
                                app.logger.info(
                                    f"Initial load for table {target_schema}.{table_name} completed successfully.")
                            else:
                                app.logger.info(
                                    f"Initial load for table {target_schema}.{table_name} Failed.")

                        else:
                            raise Exception(f"Failed to create new table {table_name} in target schema.")

        # Initialize task status and metrics if not already set
        if task.metrics is None:
            task.metrics = {
                'inserts': 0,
                'updates': 0,
                'deletes': 0,
                'bytes_processed': 0,
                'latency': 0,
                'last_updated': datetime.now(timezone.utc).isoformat(),
                'last_position': 0
            }

        task.status = 'running'
        db.session.commit()

        try:
            while task.status == 'running':
                start_time = time.time()

                if reload:
                    # Perform a full reload of all selected tables or specific tables
                    if tables_to_reload:
                        app.logger.info(f"Reloading specific tables for task {task_id}: {tables_to_reload}.")
                        # Simulate reloading specific tables
                        for table in tables_to_reload:
                            app.logger.info(f"Reloading table {table}.")
                            task.metrics['inserts'] += 500  # Simulate 500 inserts per table
                            task.metrics['bytes_processed'] += 5242880  # Simulate 5 MB processed per table
                    else:
                        app.logger.info(f"Performing full reload for task {task_id}.")
                        # Simulate full reload of all tables
                        task.metrics['inserts'] += 1000  # Simulate 1000 inserts
                        task.metrics['bytes_processed'] += 10485760  # Simulate 10 MB processed
                    reload = False  # Mark reload as complete
                elif initial_load:
                    # Perform initial load of all selected tables
                    app.logger.info(f"Performing initial load for task {task_id}.")
                    # Simulate initial load by processing a large batch of data
                    task.metrics['inserts'] += 1000  # Simulate 1000 inserts
                    task.metrics['bytes_processed'] += 10485760  # Simulate 10 MB processed
                    initial_load = False  # Mark initial load as complete
                else:
                    # Incremental replication (resume from last position)
                    app.logger.info(f"Performing incremental replication for task {task_id}.")
                    # Simulate incremental replication by processing a small batch of data
                    task.metrics['inserts'] += 10  # Simulate 10 inserts
                    task.metrics['updates'] += 5  # Simulate 5 updates
                    task.metrics['deletes'] += 2  # Simulate 2 deletes
                    task.metrics['bytes_processed'] += 102400  # Simulate 100 KB processed
                    task.metrics['last_position'] += 10  # Update last position

                # Update latency and last updated timestamp
                task.metrics['latency'] = int((time.time() - start_time) * 1000)
                task.metrics['last_updated'] = datetime.now(timezone.utc).isoformat()

                # Commit changes to the database
                db.session.commit()

                # Simulate a delay to mimic real-world replication
                time.sleep(1)

        except Exception as e:
            app.logger.error(f"Task {task_id} failed: {str(e)}")
            task.status = 'failed'
            task.metrics['last_updated'] = datetime.now(timezone.utc).isoformat()
            db.session.commit()
        finally:
            if task.status != 'failed':
                task.status = 'stopped'
                task.metrics['last_updated'] = datetime.now(timezone.utc).isoformat()
                db.session.commit()
            app.logger.info(f"Task {task_id} has been stopped.")


# Add these imports
from sqlalchemy import create_engine, text, inspect


def _get_primary_key_columns(engine, schema, table_name):
    """
    Fetch the primary key columns for a given table using SQLAlchemy's inspection.
    """
    inspector = inspect(engine)
    pk_constraint = inspector.get_pk_constraint(table_name, schema=schema)
    return pk_constraint.get("constrained_columns", [])


def _parse_table_name(sql):
    """
    Parse the table name from the SQL statement, removing the schema prefix.
    """
    try:
        # Extract the table name from the SQL statement
        if "into" in sql.lower():
            table_part = sql.lower().split("into")[1].split("(")[0].strip()
        elif "from" in sql.lower():
            table_part = sql.lower().split("from")[1].split()[0].strip()
        else:
            raise ValueError(f"Unable to parse table name from SQL: {sql}")

        # Remove schema prefix and quotes
        table_name = table_part.split(".")[-1].replace('"', '').replace("'", "")
        return table_name
    except Exception as e:
        raise ValueError(f"Failed to parse table name from SQL: {sql}. Error: {str(e)}")


def _parse_columns_and_values(sql):
    """
    Parse columns and values from an INSERT SQL statement.
    """
    try:
        columns_part = sql.split("(")[1].split(")")[0].strip().replace('"', '')
        values_part = sql.split("values")[1].strip().replace(";", "").strip()
        return columns_part, values_part
    except Exception as e:
        raise ValueError(f"Failed to parse columns and values from SQL: {sql}. Error: {str(e)}")


def _build_where_clause_from_primary_keys(primary_key_columns, sql):
    """Extract only primary key conditions from DELETE statement"""
    try:
        where_part = sql.lower().split("where")[1].strip().replace(";", "")
        conditions = []

        for col in primary_key_columns:
            col_lower = col.lower()
            if f"{col_lower} =" in where_part:
                # Extract the condition using case-sensitive original column name
                start = where_part.find(col_lower)
                end = where_part.find(" and ", start) if " and " in where_part[start:] else None
                condition = where_part[start:end].strip()
                conditions.append(f"{col} {condition.split('=', 1)[1].strip()}")

        return " AND ".join(conditions)
    except Exception as e:
        raise ValueError(f"Failed to build WHERE clause: {str(e)}")

def apply_changes_to_target(changes, target_endpoint, target_schema, task, app):
    try:
        engine = create_engine(_get_target_connection_string(target_endpoint, target_schema))
        with engine.connect() as conn:
            # Cache primary keys for all tables involved in the task
            primary_key_cache = {}
            for change in changes:
                sql = change["sql"]
                operation = change["operation"]
                if operation in ("insert", "delete", "update"):
                    try:
                        table_name = _parse_table_name(sql)
                        if table_name not in primary_key_cache:
                            primary_key_cache[table_name] = _get_primary_key_columns(engine, target_schema, table_name)
                            if not primary_key_cache[table_name]:
                                raise ValueError(f"No primary key found for table {target_schema}.{table_name}")
                            app.logger.info(
                                f"Cached primary keys for table {target_schema}.{table_name}: {primary_key_cache[table_name]}")
                    except Exception as e:
                        app.logger.error(f"Failed to cache primary keys for SQL: {sql}. Error: {str(e)}")
                        continue

            for change in changes:
                sql = change["sql"]
                operation = change["operation"]
                if operation in ("insert", "delete", "update"):
                    try:
                        table_name = _parse_table_name(sql)
                        primary_key_columns = primary_key_cache.get(table_name)
                        if not primary_key_columns:
                            raise ValueError(f"No primary key found for table {target_schema}.{table_name}")

                        if operation == "insert":
                            columns, values = _parse_columns_and_values(sql)

                            # FIX 1: Clean up values formatting
                            # Remove surrounding parentheses and split values
                            clean_values = values.strip()[1:-1]  # Remove ()
                            individual_values = [v.strip() for v in clean_values.split(",")]

                            # FIX 2: Create proper column-value pairs
                            value_assignments = ", ".join([
                                f"{val} AS {col.strip()}"
                                for col, val in zip(columns.split(","), individual_values)
                            ])

                            if hasattr(task, "merge_enabled") and task.merge_enabled:
                                # Exclude primary keys from update
                                all_columns = [col.strip() for col in columns.split(",")]
                                primary_key_columns_upper = []
                                all_columns_upper = []

                                for s in all_columns:
                                    all_columns_upper.append(s.upper())

                                for s in primary_key_columns:
                                    primary_key_columns_upper.append(s.upper())

                                non_pk_columns = [col for col in all_columns_upper if col not in primary_key_columns_upper]
                                app.logger.info(f"primary_key_columns : {primary_key_columns_upper}")
                                app.logger.info(f"non_pk_columns : {non_pk_columns}")
                                on_clause = " AND ".join([f"tgt.{col} = src.{col}" for col in primary_key_columns])
                                update_set = ", ".join([f"tgt.{col} = src.{col}" for col in non_pk_columns])
                                app.logger.info(f"update_set : {update_set}")

                                merge_sql = f"""
                                    MERGE INTO {target_schema}.{table_name} tgt
                                USING (
                                    SELECT {','.join([
                                        f"{v} AS {c}" 
                                        for c, v in zip(columns.split(","), individual_values)
                                    ])} 
                                    FROM dual
                                ) src
                                ON ({on_clause})
                                    WHEN MATCHED THEN
                                      UPDATE SET {update_set},
                                                 tgt.meta_update_timestamp = SYSTIMESTAMP
                                    WHEN NOT MATCHED THEN
                                      INSERT ({columns}, meta_create_timestamp, meta_update_timestamp)
                                      VALUES ({clean_values}, SYSTIMESTAMP, SYSTIMESTAMP)
                                    """
                                app.logger.info(f"Executing MERGE: {merge_sql}")
                                conn.execute(text(merge_sql))
                            else:
                                # Build the INSERT statement for the target table
                                insert_sql = f"""
                                INSERT INTO {target_schema}.{table_name} ({columns})
                                VALUES ({clean_values});
                                """
                                app.logger.info(f"Executing INSERT: {insert_sql}")
                                conn.execute(text(insert_sql))

                        elif operation == "delete":
                            # Existing delete logic remains the same
                            where_clause = _build_where_clause_from_primary_keys(
                                primary_key_cache.get(table_name, []),
                                sql
                            )
                            delete_sql = f"""
                            DELETE FROM {target_schema}.{table_name}
                            WHERE {where_clause};
                            """
                            app.logger.info(f"Executing DELETE: {delete_sql}")
                            conn.execute(text(delete_sql))

                        elif operation == "update":
                            # Existing update logic remains the same
                            set_clause = sql.split("set")[1].split("where")[0].strip()
                            where_clause = _build_where_clause_from_primary_keys(primary_key_columns, sql)
                            update_sql = f"""
                            UPDATE {target_schema}.{table_name}
                            SET {set_clause}
                            WHERE {where_clause};
                            """
                            app.logger.info(f"Executing UPDATE: {update_sql}")
                            conn.execute(text(update_sql))

                        conn.commit()
                        app.logger.info(f"Applied: {sql}")
                    except Exception as e:
                        app.logger.error(f"Failed to apply change: {sql}. Error: {str(e)}")
                else:
                    app.logger.warning(f"Unsupported operation: {operation}")
    except Exception as e:
        app.logger.error(f"Replication failed: {str(e)}")
        raise

def oldapply_changes_to_target(changes, target_endpoint, target_schema,app):
    """Apply changes to the target Oracle database."""
    try:
        # Connect to Oracle target
        engine = create_engine(_get_target_connection_string(target_endpoint, target_schema))

        with engine.connect() as conn:
            for change in changes:
                # Handle Oracle-specific DML syntax if needed
                sql = change["sql"].replace("/* <oracle> */", "")  # Cleanup LogMiner artifacts
                app.logger.info(f"Sql to Apply: {sql}")

                # Execute the SQL statement
                conn.execute(text(sql))
                conn.commit()  # Explicit commit for Oracle

                # Log the operation
                app.logger.info(f"Applied: {sql}")

    except Exception as e:
        app.logger.error(f"Failed to apply changes: {str(e)}")
        raise

def _get_target_connection_string(endpoint, schema):
    """Generate connection string for the target database."""
    if endpoint.type == 'oracle':
        # Oracle connection string format: oracle+cx_oracle://user:password@host:port/?service_name=service
        return (
            f"oracle+cx_oracle://{endpoint.username}:{endpoint.password}"
            f"@{endpoint.host}:{endpoint.port}/?service_name={endpoint.service_name}"
        )
    elif endpoint.type == 'mysql':
        return f"mysql+pymysql://{endpoint.username}:{endpoint.password}@{endpoint.host}:{endpoint.port}/{schema}"
    elif endpoint.type == 'bigquery':
        return f"bigquery://{schema}"
    else:
        raise ValueError(f"Unsupported target type: {endpoint.type}")