document.addEventListener('DOMContentLoaded', function() {
     console.log('DOM fully loaded'); // Debugging
    // Use event delegation for the "Stop" button
    document.addEventListener('click', function(event) {
        if (event.target.classList.contains('stop-task-btn')) {
            const taskId = event.target.dataset.taskId;
            confirmStopTask(taskId);
        }
    });
    // Wait for the task cards to be fully rendered
    const taskMonitor = document.getElementById('task-monitor');
    if (taskMonitor) {
        const observer = new MutationObserver(() => {
            // Initialize the script after the DOM is updated
            initializeTaskControl();
        });

        observer.observe(taskMonitor, { childList: true, subtree: true });
    } else {
        console.error('Task monitor element not found');
    }
});

function initializeTaskControl() {
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
}

function areAllTasksStopped() {
    const taskElements = document.querySelectorAll('[data-task-id]');
    return Array.from(taskElements).every(taskElement => {
        const statusBadge = taskElement.querySelector('.task-status-badge');
        // Add debugging
        console.log(`Task ${taskElement.dataset.taskId} status badge:`, statusBadge);
        if (!statusBadge) {
            console.error(`Status badge not found for task ${taskElement.dataset.taskId}`);
            return false; // Skip this task
        }
        return statusBadge.textContent.trim().toLowerCase() === 'stopped';
    });
}

function updateTaskMetrics(taskId) {
    fetch(`/task/${taskId}/metrics`)
        .then(response => response.json())
        .then(data => {
            // Update numeric values
            ['inserts', 'updates', 'deletes'].forEach(metric => {
                document.querySelector(`[data-metric="${metric}"]`).textContent =
                    data.metrics[metric] || 0;
            });

            // Format bytes
            const bytes = data.metrics['bytes_processed'] || 0;
            document.querySelector('[data-metric="bytes_processed"]').textContent =
                formatBytes(bytes);
        });
}

// Helper to format bytes
function formatBytes(bytes) {
    if (bytes === 0) return '0B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + sizes[i];
}

// Update every 5 seconds
setInterval(() => updateTaskMetrics(taskId), 5000);
async function oldupdateTaskMetrics() {
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

        // Add debugging
        console.log(`Task ${taskId} status badge:`, statusBadge);

        // Skip fetching metrics if the task is stopped or status badge is missing
        if (!statusBadge) {
            console.error(`Status badge not found for task ${taskId}`);
            return;
        }
        if (statusBadge.textContent.trim().toLowerCase() === 'stopped') {
            console.log(`Task ${taskId} is stopped. Skipping metrics update.`); // Debugging
            return;
        }

        try {
            const response = await fetch(`/task/${taskId}/metrics`);
            const metrics = await response.json();

            // Update metrics display
            const latencyElement = taskElement.querySelector('[data-metric="latency"]');
            const volumeElement = taskElement.querySelector('[data-metric="volume"]');
            const insertsElement = taskElement.querySelector('[data-metric="inserts"]');
            const updatesElement = taskElement.querySelector('[data-metric="updates"]');
            const deletesElement = taskElement.querySelector('[data-metric="deletes"]');

            if (latencyElement) latencyElement.textContent = metrics.latency ? `${metrics.latency}ms` : '--';
            if (volumeElement) volumeElement.textContent = formatBytes(metrics.bytes_processed);
            if (insertsElement) insertsElement.textContent = metrics.inserts || 0;
            if (updatesElement) updatesElement.textContent = metrics.updates || 0;
            if (deletesElement) deletesElement.textContent = metrics.deletes || 0;

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

// Helper function to format bytes
function oldformatBytes(bytes) {
    if (!bytes) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}

// Helper function to get status color
function getStatusColor(status) {
    switch (status.toLowerCase()) {
        case 'running': return 'success';
        case 'stopped': return 'danger';
        case 'paused': return 'warning';
        default: return 'secondary';
    }
}

// Function to confirm and stop a task
async function confirmStopTask(taskId) {
    console.log(`Confirming stop for task ${taskId}`); // Debugging

    if (confirm('Are you sure you want to stop this task?')) {
        try {
            console.log(`Stopping task ${taskId}...`); // Debugging
            const response = await fetch(`/task/${taskId}/stop`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                }
            });

            if (response.ok) {
                const result = await response.json();
                console.log(`Response from backend:`, result); // Debugging
                if (result.success) {
                    const taskElement = document.querySelector(`[data-task-id="${taskId}"]`);
                    const statusBadge = taskElement.querySelector('.task-status-badge');

                    // Update the status badge
                    if (statusBadge) {
                        statusBadge.className = `task-status-badge badge bg-${getStatusColor('stopped')}`;
                        statusBadge.textContent = 'STOPPED';
                    }

                    // Disable the stop button
                    const stopButton = taskElement.querySelector('.stop-task-btn');
                    if (stopButton) {
                        stopButton.disabled = true;
                    }

                    console.log(`Task ${taskId} stopped successfully.`);
                } else {
                    console.error(`Failed to stop task ${taskId}:`, result.message);
                }
            } else {
                console.error(`Failed to stop task ${taskId}: HTTP error ${response.status}`);
            }
        } catch (error) {
            console.error('Error stopping task:', error);
        }
    }
}
function confirmDeleteTask(taskId) {
    if (confirm("Are you sure you want to delete this task?")) {
        fetch(`/task/delete/${taskId}`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
        })
        .then(response => response.json())
        .then(data => {
            if (data.success) {
                // Reload the page to reflect the changes
                window.location.reload();
            } else {
                alert("Failed to delete task: " + data.message);
            }
        })
        .catch(error => {
            console.error("Error deleting task:", error);
            alert("An error occurred while deleting the task.");
        });
    }
}
setInterval(() => updateTaskMetrics(taskId), 5000);
