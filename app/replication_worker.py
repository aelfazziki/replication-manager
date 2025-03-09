import hashlib
from datetime import datetime
import time
from app import db, create_app
from app.models import ReplicationTask
from app.services.metadata_service import MetadataService

def generate_hash(data):
    return hashlib.sha256(str(data).encode()).hexdigest()

def run_replication(task_id, initial_load=False, resume=False):
    app = create_app()
    with app.app_context():
        task = ReplicationTask.query.get(task_id)
        if not task:
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

        # Example: Create schema if it doesn't exist
        MetadataService.create_schema_if_not_exists(destination, target_schema)
        task.status = 'running'
        task.metrics = {
            'inserts': 0,
            'updates': 0,
            'deletes': 0,
            'bytes_processed': 0,
            'latency': 0,
            'last_updated': datetime.utcnow().isoformat()
        }
        db.session.commit()

        try:
            while task.status == 'running':
                start_time = time.time()

                # Simulate replication work
                if initial_load:
                    # Perform initial load
                    pass
                elif resume:
                    # Resume from last position
                    pass
                else:
                    # Incremental replication
                    pass

                # Update metrics
                task.metrics.update({
                    'inserts': task.metrics.get('inserts', 0) + 10,
                    'updates': task.metrics.get('updates', 0) + 5,
                    'deletes': task.metrics.get('deletes', 0) + 2,
                    'bytes_processed': task.metrics.get('bytes_processed', 0) + 102400,
                    'latency': int((time.time() - start_time) * 1000),
                    'last_updated': datetime.utcnow().isoformat()
                })

                db.session.commit()
        except Exception as e:
            app.logger.error(f"Task {task_id} failed: {str(e)}")
        finally:
            task.status = 'stopped'
            db.session.commit()