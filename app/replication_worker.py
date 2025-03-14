import hashlib
from datetime import datetime
import time
from app import db, create_app
from app.models import ReplicationTask
from app.services.metadata_service import MetadataService

def generate_hash(data):
    return hashlib.sha256(str(data).encode()).hexdigest()

def run_replication(task_id, initial_load=False, reload=False, tables_to_reload=None):
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

                # Call table creation for each table
                success = MetadataService.create_tables_if_not_exists(
                    source_endpoint=source,
                    target_endpoint=destination,
                    source_schema=source_schema,  # Extracted schema (e.g., "AEF")
                    target_schema=target_schema,  # Target schema (e.g., "AEF_TRGT")
                    table_name=table_name  # Extracted table name (e.g., "AEF_TEST")
                )
                if not success:
                    raise Exception(f"Failed to create table {table_name} in target schema.")

        # Initialize task status and metrics if not already set
        if task.metrics is None:
            task.metrics = {
                'inserts': 0,
                'updates': 0,
                'deletes': 0,
                'bytes_processed': 0,
                'latency': 0,
                'last_updated': datetime.utcnow().isoformat(),
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
                task.metrics['last_updated'] = datetime.utcnow().isoformat()

                # Commit changes to the database
                db.session.commit()

                # Simulate a delay to mimic real-world replication
                time.sleep(1)

        except Exception as e:
            app.logger.error(f"Task {task_id} failed: {str(e)}")
            task.status = 'failed'
            task.metrics['last_updated'] = datetime.utcnow().isoformat()
            db.session.commit()
        finally:
            if task.status != 'failed':
                task.status = 'stopped'
                task.metrics['last_updated'] = datetime.utcnow().isoformat()
                db.session.commit()
            app.logger.info(f"Task {task_id} has been stopped.")