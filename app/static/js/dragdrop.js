document.addEventListener('DOMContentLoaded', function() {
    // Initialize drag-and-drop functionality
    const cards = document.querySelectorAll('.endpoint-card');
    cards.forEach(card => {
        card.addEventListener('dragstart', handleDragStart);
    });

    const dropzones = document.querySelectorAll('.pipeline-area');
    dropzones.forEach(zone => {
        zone.addEventListener('drop', handleDrop);
        zone.addEventListener('dragover', handleDragOver);
    });
});

function handleDragStart(e) {
    e.dataTransfer.setData('text/plain', e.target.id);
}

function handleDrop(e) {
    e.preventDefault();
    const id = e.dataTransfer.getData('text');
    const draggable = document.getElementById(id);
    e.target.appendChild(draggable);
}

function handleDragOver(e) {
    e.preventDefault();
}