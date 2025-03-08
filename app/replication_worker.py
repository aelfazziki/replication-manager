from app import db
from app.models import ReplicationTask
from datetime import datetime
import time


def run_replication(task_id):
    # Create a new app instance for this thread
    from app import create_app
    app = create_app()

    with app.app_context():
        task = ReplicationTask.query.get(task_id)
        if not task:
            return

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
                time.sleep(2)

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