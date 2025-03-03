class DragDropManager {
  constructor() {
    this.endpoints = [];
    this.initDnD();
  }

  initDnD() {
    interact('.endpoint-card').draggable({
      inertia: true,
      autoScroll: true,
      listeners: {
        start: (event) => this.onDragStart(event),
        move: (event) => this.onDragMove(event),
        end: (event) => this.onDragEnd(event)
      }
    });

    interact('.pipeline-area').dropzone({
      accept: '.endpoint-card',
      listeners: {
        drop: (event) => this.onDrop(event)
      }
    });
  }

  onDrop(event) {
    const endpointId = event.relatedTarget.dataset.endpointId;
    const type = event.relatedTarget.dataset.type;
    this.addToPipeline(endpointId, type, event.pageX, event.pageY);
  }

  addToPipeline(endpointId, type, x, y) {
    const endpoint = this.endpoints.find(e => e.id === endpointId);
    const pipeline = document.getElementById('pipeline-container');
    
    const node = document.createElement('div');
    node.className = `pipeline-node ${type}`;
    node.style.left = `${x - pipeline.offsetLeft}px`;
    node.style.top = `${y - pipeline.offsetTop}px`;
    node.innerHTML = `
      <div class="node-header">${endpoint.name}</div>
      <div class="node-config"></div>
    `;
    
    pipeline.appendChild(node);
    this.initNodeConfig(node, endpoint);
  }

  initNodeConfig(node, endpoint) {
    const configArea = node.querySelector('.node-config');
    
    // Source-specific config
    if(endpoint.type === 'oracle') {
      configArea.innerHTML = `
        <div class="table-selector">
          <h4>Select Tables</h4>
          <div class="table-list"></div>
          <button onclick="fetchTables('${endpoint.id}')">Refresh Tables</button>
        </div>
      `;
    }
    
    // Target-specific config
    if(endpoint.type === 'bigquery') {
      configArea.innerHTML = `
        <div class="target-config">
          <label>Partitioning:
            <select class="partition-type">
              <option value="none">None</option>
              <option value="time">Time-based</option>
              <option value="ingestion">Ingestion time</option>
            </select>
          </label>
          <label>Clustering:
            <input type="text" class="cluster-columns" placeholder="comma-separated columns">
          </label>
        </div>
      `;
    }
  }
}