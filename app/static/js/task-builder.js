function fetchTables(endpointId) {
  fetch(`/api/endpoints/${endpointId}/tables`)
    .then(response => response.json())
    .then(tables => {
      const container = document.querySelector('.table-list');
      container.innerHTML = tables.map(table => `
        <div class="table-item">
          <label>
            <input type="checkbox" name="tables" value="${table.full_name}">
            ${table.name}
            <span class="columns-toggle" onclick="toggleColumns(this)">▶</span>
            <div class="column-list hidden"></div>
          </label>
        </div>
      `).join('');
    });
}

function toggleColumns(element) {
  const columnList = element.nextElementSibling;
  columnList.classList.toggle('hidden');
  
  if(!columnList.innerHTML) {
    fetch(`/api/tables/${element.parentNode.dataset.tableId}/columns`)
      .then(response => response.json())
      .then(columns => {
        columnList.innerHTML = columns.map(col => `
          <div class="column-item">
            <label>
              <input type="checkbox" name="columns" 
                     checked disabled>
              ${col.name} (${col.type})
            </label>
          </div>
        `).join('');
      });
  }
}