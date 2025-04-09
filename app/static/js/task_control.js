// app/static/js/task_control.js (Revised)

/**
 * Updates the UI elements within a specific task card based on status data.
 * @param {HTMLElement} cardElement - The task card element (div with data-task-id).
 * @param {object} statusData - The status data object fetched from the API.
 */
function updateTaskCardUI(cardElement, statusData) {
    if (!cardElement || !statusData) return;

    const taskId = cardElement.dataset.taskId;
    const status = statusData.status || 'unknown';
    const metrics = statusData.metrics || {};

    // --- Update Status Badge ---
    const statusBadge = cardElement.querySelector('.task-status-badge');
    if (statusBadge) {
        statusBadge.textContent = status.toUpperCase();
        // Remove existing background classes
        statusBadge.classList.remove('bg-success', 'bg-secondary', 'bg-danger', 'bg-warning', 'bg-info', 'bg-primary', 'bg-light', 'text-dark');
        // Add appropriate class based on status
        let badgeClass = 'bg-light';
        let textClass = 'text-dark'; // Default text for light backgrounds
        switch (status) {
            case 'running':    badgeClass = 'bg-success'; textClass = ''; break;
            case 'stopped':    badgeClass = 'bg-secondary'; textClass = ''; break;
            case 'failed':     badgeClass = 'bg-danger'; textClass = ''; break;
            case 'pending':    badgeClass = 'bg-warning'; break; // Keep text dark for yellow
            case 'stopping':   badgeClass = 'bg-info'; break;    // Keep text dark for cyan
            case 'completed':  badgeClass = 'bg-primary'; textClass = ''; break;
            default:           badgeClass = 'bg-light'; // Keep text dark for light grey
        }
        statusBadge.classList.add(badgeClass);
        if (textClass) {
            statusBadge.classList.add(textClass);
        }
    }

    // --- Update Metric Elements ---
    cardElement.querySelectorAll('[data-metric]').forEach(el => {
        const metricName = el.dataset.metric;
        let value = metrics[metricName];

        if (metricName === 'last_updated' && value) {
            // Format timestamp nicely
            try {
                value = new Date(value).toLocaleString();
            } catch (e) { /* Ignore formatting errors */ }
        }

        el.textContent = (value !== null && value !== undefined && value !== '') ? value : '--';

        // Handle error display specifically
        if (metricName === 'error') {
             if (value) {
                 el.innerHTML = `<i class="bi bi-exclamation-triangle"></i> Error: ${value}`;
                 el.classList.remove('d-none'); // Ensure error is visible
                 el.classList.add('text-danger', 'small', 'mt-1');
             } else {
                 el.textContent = ''; // Clear error message
                 el.classList.add('d-none'); // Hide element if no error
                 el.classList.remove('text-danger', 'small', 'mt-1');
             }
         }
    });

    // --- Update Button States ---
    const runButton = cardElement.querySelector('.run-btn');
    const reloadButton = cardElement.querySelector('.reload-btn');
    const stopButton = cardElement.querySelector('.stop-btn');
    const deleteButton = cardElement.querySelector('.delete-btn'); // Assuming delete is a button in dropdown

    const isRunning = status === 'running';
    const isPending = status === 'pending';
    const isStopping = status === 'stopping';
    const isActive = isRunning || isPending || isStopping;

    if (runButton) runButton.disabled = isActive;
    if (reloadButton) reloadButton.disabled = isActive;
    if (stopButton) stopButton.disabled = !isRunning && !isPending; // Can only stop if running/pending
    if (deleteButton) deleteButton.disabled = isActive; // Prevent delete if active
}

/**
 * Disables all action buttons on a specific card, optionally showing a spinner.
 * @param {HTMLElement} cardElement - The task card element.
 * @param {boolean} showSpinner - Whether to show a spinner on the clicked button.
 * @param {HTMLElement} clickedButton - The button that was clicked.
 */
function disableCardActions(cardElement, showSpinner = false, clickedButton = null) {
    cardElement.querySelectorAll('.run-btn, .reload-btn, .stop-btn, .delete-btn, .dropdown-toggle').forEach(btn => {
        btn.disabled = true;
        if (showSpinner && btn === clickedButton) {
             btn.dataset.originalHtml = btn.innerHTML; // Store original HTML
             btn.innerHTML = `<span class="spinner-border spinner-border-sm" role="status" aria-hidden="true"></span> Working...`;
        }
    });
}

/**
 * Re-enables buttons based on current status after an action completes.
 * @param {HTMLElement} cardElement - The task card element.
 * @param {object} statusData - The latest status data.
 */
function enableCardActions(cardElement, statusData) {
     cardElement.querySelectorAll('.run-btn, .reload-btn, .stop-btn, .delete-btn, .dropdown-toggle').forEach(btn => {
         if (btn.dataset.originalHtml) {
             btn.innerHTML = btn.dataset.originalHtml; // Restore original HTML
             delete btn.dataset.originalHtml;
         }
         // Re-enable based on latest status (updateTaskCardUI handles disabling correctly)
         btn.disabled = false; // Initially enable all
     });
     // Then let updateTaskCardUI set the correct disabled states
     updateTaskCardUI(cardElement, statusData);
}


/**
 * Handles API calls for task actions (Run, Reload, Stop).
 * @param {string} action - 'run', 'reload', or 'stop'.
 * @param {number} taskId - The ID of the task.
 * @param {object|null} bodyData - Optional JSON data for the request body (for 'run').
 * @param {HTMLElement} clickedButton - The button element that was clicked.
 */
async function handleTaskAction(action, taskId, bodyData = null, clickedButton) {
    const cardElement = clickedButton.closest('[data-task-id]');
    if (!cardElement) return;

    // Disable buttons on this card immediately
    disableCardActions(cardElement, true, clickedButton);

    const url = `/task/${taskId}/${action}`;
    const options = {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            'Accept': 'application/json'
            // Add CSRF token header if needed
        },
    };
    if (bodyData) {
        options.body = JSON.stringify(bodyData);
    }

    let result = null;
    try {
        const response = await fetch(url, options);
        result = await response.json(); // Always try to parse JSON

        if (!response.ok) {
            console.error(`Failed to ${action} task ${taskId}: HTTP ${response.status}`, result);
            alert(`Failed to ${action} task: ${result?.message || 'Server error'}`);
        } else {
            console.log(`Task ${taskId} ${action} request successful:`, result);
            // Optional: Show flash message via JS? Or rely on Flask flash + poll
            // Immediately fetch status and update UI
            await fetchAndUpdateStatus(taskId, cardElement);
        }

    } catch (error) {
        console.error(`Error during ${action} task ${taskId}:`, error);
        alert(`An error occurred while trying to ${action} the task.`);
    } finally {
        // Fetch final status after a short delay to allow backend processing, then re-enable buttons
        // Use the result from the initial fetchAndUpdateStatus if available
        const latestStatus = await fetchTaskStatus(taskId);
        if (cardElement) { // Check if card still exists (might be deleted)
             enableCardActions(cardElement, latestStatus);
        }
    }
}

/**
 * Prompts for confirmation and handles the delete task action.
 * @param {number} taskId - The ID of the task.
 * @param {string} taskName - The name of the task for the confirmation dialog.
 */
async function confirmDeleteTask(taskId, taskName) {
    if (!confirm(`Are you sure you want to delete task "${taskName}" (ID: ${taskId})?`)) {
        return;
    }

    const cardElement = document.querySelector(`[data-task-id="${taskId}"]`);
    const deleteButton = cardElement?.querySelector('.delete-btn'); // Might be null if called differently

    // Disable actions on the card
    if (cardElement) {
        disableCardActions(cardElement, true, deleteButton);
    }

    const url = `/task/${taskId}/delete`; // CORRECTED URL
    try {
        const response = await fetch(url, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Accept': 'application/json'
                // Add CSRF token header if needed
            }
        });
        const data = await response.json();

        if (response.ok && data.success) {
            console.log(`Task ${taskId} deleted successfully.`);
            // Remove the card from the DOM instead of reloading page
            if (cardElement) {
                cardElement.remove();
                // Optionally show a temporary success message where the card was
            }
             // You might want a more persistent message using Flask flash + maybe redirect/refresh
             // or a dedicated notification system. For now, just log it.
             alert(`Task "${taskName}" deleted successfully.`); // Simple feedback

        } else {
            console.error(`Failed to delete task ${taskId}:`, data.message);
            alert(`Failed to delete task: ${data?.message || 'Server error'}`);
             // Re-enable buttons on failure
             const latestStatus = await fetchTaskStatus(taskId); // Fetch status again
             if (cardElement) {
                 enableCardActions(cardElement, latestStatus);
             }
        }
    } catch (error) {
        console.error(`Error deleting task ${taskId}:`, error);
        alert("An error occurred while deleting the task.");
         // Re-enable buttons on failure
         const latestStatus = await fetchTaskStatus(taskId); // Fetch status again
         if (cardElement) {
             enableCardActions(cardElement, latestStatus);
         }
    }
}


/**
 * Fetches the status for a single task.
 * @param {string} taskId
 * @returns {Promise<object|null>} - The status data object or null on error.
 */
async function fetchTaskStatus(taskId) {
     try {
        const response = await fetch(`/task/${taskId}/status`);
        if (!response.ok) {
            console.error(`Failed to fetch status for task ${taskId}: HTTP ${response.status}`);
            return null;
        }
        const statusData = await response.json();
        if (!statusData || !statusData.success) {
            console.error(`Error in status response for task ${taskId}:`, statusData?.message);
            return null;
        }
        return statusData;
    } catch (error) {
        console.error(`Error fetching status for task ${taskId}:`, error);
        return null;
    }
}

/**
 * Fetches status for a task and updates its card UI.
 * @param {string} taskId
 * @param {HTMLElement} cardElement - The card element for the task.
 */
async function fetchAndUpdateStatus(taskId, cardElement) {
    const statusData = await fetchTaskStatus(taskId);
    if (statusData && cardElement) { // Ensure card still exists
        updateTaskCardUI(cardElement, statusData);
    }
}


/**
 * Polls the status of all visible tasks on the page.
 */
function pollAllTaskStatuses() {
    const taskCards = document.querySelectorAll('[data-task-id]');
    if (taskCards.length === 0) {
        // console.log("No task cards found on page to poll.");
        return; // No tasks to poll
    }

    // console.log(`Polling status for ${taskCards.length} tasks...`);
    taskCards.forEach(card => {
        const taskId = card.dataset.taskId;
        // Fetch status and update UI *without* waiting for all promises
        // This prevents one failed request from blocking updates for others
        fetchAndUpdateStatus(taskId, card);
    });
}

// --- Event Listener Setup ---
document.addEventListener('DOMContentLoaded', function() {

    // Add listeners to the container for event delegation (more efficient)
    const taskMonitor = document.getElementById('task-monitor'); // Assuming the container has this ID

    if (taskMonitor) {
        taskMonitor.addEventListener('click', function(event) {
            const target = event.target;
            const button = target.closest('button'); // Find the closest button clicked
            if (!button) return; // Exit if click wasn't on or inside a button

            const taskId = button.dataset.taskId || button.closest('[data-task-id]')?.dataset.taskId;
            if (!taskId) return; // Exit if no task ID found

            if (button.classList.contains('run-btn')) {
                // Prompt for start datetime - Consider using a modal instead of prompt
                const startDatetimeInput = prompt("Enter optional start datetime (YYYY-MM-DD HH:MM:SS) or leave blank:");
                // Basic validation (optional) - allow empty string
                // const isValidFormat = startDatetimeInput === '' || /^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/.test(startDatetimeInput);
                // if (!isValidFormat) {
                //     alert("Invalid datetime format. Please use YYYY-MM-DD HH:MM:SS or leave blank.");
                //     return;
                // }
                 handleTaskAction('run', taskId, { start_datetime: startDatetimeInput || null }, button);

            } else if (button.classList.contains('reload-btn')) {
                if (confirm("Are you sure you want to reload this task? This will reset its position and trigger an initial load.")) {
                     handleTaskAction('reload', taskId, null, button);
                }
            } else if (button.classList.contains('stop-btn')) {
                if (confirm("Are you sure you want to request this task to stop?")) {
                     handleTaskAction('stop', taskId, null, button);
                }
             }
             // Note: Delete is handled by onclick attribute calling confirmDeleteTask directly
        });
    }

    // --- Initial Status Poll and Periodic Polling ---
    pollAllTaskStatuses(); // Poll immediately on load
    setInterval(pollAllTaskStatuses, 10000); // Poll every 10 seconds (adjust interval as needed)

});

// Note: confirmDeleteTask is defined globally because it's called via onclick attribute in dashboard.html