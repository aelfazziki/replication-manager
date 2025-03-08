document.addEventListener('DOMContentLoaded', function() {
    // Update metrics every 2 seconds
    const updateInterval = 2000;
    let updateTimer = setInterval(updateTaskMetrics, updateInterval);

    // Stop button handlers
    document.querySelectorAll('.stop-task-btn').forEach(btn => {
        btn.addEventListener('click', function() {
            const taskId = this.dataset.taskId;
            confirmStopTask(taskId);
        });
    });

    // Function to check if all tasks are stopped
    function areAllTasksStopped() {
        const taskElements = document.querySelectorAll('[data-task-id]');
        return Array.from(taskElements).every(taskElement => {
            const statusBadge = taskElement.querySelector('.task-status-badge');
            return statusBadge.textContent.trim().toLowerCase() === 'stopped';
        });
    }

    // Stop the interval timer if all tasks are stopped
    function updateTaskMetrics() {
        if (!document.getElementById('task-monitor')) return;

        // Stop the timer if all tasks are stopped
        if (areAllTasksStopped()) {
            console.log('All tasks are stopped. Stopping metrics updates.'); // Debugging
            clearInterval(updateTimer);
            return;
        }

        document.querySelectorAll('[data-task-id]').forEach(async taskElement => {
            const taskId = taskElement.dataset.taskId;
            const statusBadge = taskElement.querySelector('.task-status-badge');

            // Skip fetching metrics if the task is stopped
            if (statusBadge.textContent.trim().toLowerCase() === 'stopped') {
                console.log(`Task ${taskId} is stopped. Skipping metrics update.`); // Debugging
                return;
            }

            try {
                const response = await fetch(`/task/${taskId}/metrics`);
                const metrics = await response.json();

                // Update metrics display
                taskElement.querySelector('[data-metric="latency"]').textContent =
                    metrics.latency ? `${metrics.latency}ms` : '--';
                taskElement.querySelector('[data-metric="volume"]').textContent =
                    formatBytes(metrics.bytes_processed);
                taskElement.querySelector('[data-metric="inserts"]').textContent =
                    metrics.inserts || 0;
                taskElement.querySelector('[data-metric="updates"]').textContent =
                    metrics.updates || 0;
                taskElement.querySelector('[data-metric="deletes"]').textContent =
                    metrics.deletes || 0;

                // Update status badge
                if (metrics.status) {
                    statusBadge.className = `task-status-badge badge bg-${getStatusColor(metrics.status)}`;
                    statusBadge.textContent = metrics.status.toUpperCase();
                }
            } catch (error) {
                console.error(`Error updating metrics for task ${taskId}:`, error);
            }
        });
    }
});

function formatBytes(bytes) {
        if (!bytes) return '0 B';
        const k = 1024;
        const sizes = ['B', 'KB', 'MB', 'GB'];
        const i = Math.floor(Math.log(bytes) / Math.log(k));
        return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
    }

    function getStatusColor(status) {
        switch(status.toLowerCase()) {
            case 'running': return 'success';
            case 'stopped': return 'danger';
            case 'paused': return 'warning';
            default: return 'secondary';
        }
    }

    async function confirmStopTask(taskId) {
        if(confirm('Are you sure you want to stop this task?')) {
            try {
                const response = await fetch(`/task/${taskId}/stop`, {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                    }
                });

                if(response.ok) {
                    console.log(`Task ${taskId} stopped successfully`); // Debugging

                    const taskElement = document.querySelector(`[data-task-id="${taskId}"]`);
                    taskElement.querySelector('.stop-task-btn').disabled = true;
                }
            } catch(error) {
                console.error('Error stopping task:', error);
            }
        }
    }
});