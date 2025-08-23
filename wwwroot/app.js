const uploadForm = document.getElementById('uploadForm');
const csvFile = document.getElementById('csvFile');
const previewChk = document.getElementById('previewChk');
const uploadResult = document.getElementById('uploadResult');
const exportBtn = document.getElementById('exportBtn');
const weekInput = document.getElementById('week');
const monthInput = document.getElementById('month');
const exportResult = document.getElementById('exportResult');
const viewDbBtn = document.getElementById('viewDbBtn');
const dbView = document.getElementById('dbView');
const analysisDiv = document.getElementById('analysis');
const analysisRefresh = document.getElementById('analysisRefresh');

function renderTable(container, columns, rows, useUploadColor = false) {
    if (!columns || !rows || rows.length === 0) {
        container.innerHTML = `<div class="table-wrap"><table><thead><tr>${columns.map(c => `<th>${c}</th>`).join('')}</tr></thead><tbody><tr><td colspan="${columns.length}">No data</td></tr></tbody></table></div>`;
        return;
    }
    const thead = `<thead><tr>${columns.map(c => `<th>${c}</th>`).join('')}</tr></thead>`;
    const tbody = '<tbody>' +
        rows.map(row => {
            // Highlight error row if _errors is present and non-empty
            const isError = row._errors && row._errors.trim() !== "";
            // All other existing color logic stays the same!
            let rowClass = "";
            if (useUploadColor && typeof row.Uploaded === "string") {
                rowClass = row.Uploaded.trim().toLowerCase() === 'yes' ? 'uploaded-yes' : 'uploaded-no';
            }
            if (isError) rowClass += (rowClass ? ' ' : '') + 'error-row';
            return `<tr${rowClass ? ` class="${rowClass}"` : ''}>` +
                columns.map(c => `<td>${row[c] || ''}</td>`).join('') + 
                '</tr>';
        }).join('') + '</tbody>';
    container.innerHTML = `<div class="table-wrap"><table>${thead}${tbody}</table></div>`;
}





// --- CSV upload preview handler (existing, unchanged) ---
if (uploadForm) {
    uploadForm.addEventListener('submit', async (e) => {
        e.preventDefault();
        uploadResult.textContent = '';
        if (!csvFile.files || csvFile.files.length === 0) {
            uploadResult.textContent = 'Please select a CSV file';
            return;
        }
        const file = csvFile.files[0];
        const formData = new FormData();
        formData.append('file', file);
        formData.append('preview', previewChk.checked ? 'true' : 'false');
        try {
            const res = await fetch('/api/process-csv', {
                method: 'POST',
                body: formData
            });
            const data = await res.json();
            if (data.preview) {
                // Show preview table
                if (data.preview.length > 0) {
                    const previewColumns = Object.keys(data.preview[0]);
                    renderTable(document.getElementById('previewTable'), previewColumns, data.preview);
                } else {
                    document.getElementById('previewTable').innerHTML = '<div>No preview data.</div>';
                }
            } else {
                document.getElementById('previewTable').innerHTML = '';
            }
            // Show result message/errors
            const parts = [];
            if (data.ok) {
                parts.push(`<div class="success">${data.message}</div>`);
            } else {
                const errs = (data.errors || []).map(e => `<li>${e}</li>`).join('');
                if (errs) parts.push(`<div class="error-list">Error(s):<ul>${errs}</ul></div>`);
                else parts.push(`<div class="error">${data.message || 'Failed'}</div>`);
            }
            uploadResult.innerHTML = parts.join('');
        } catch (err) {
            uploadResult.innerHTML = `<div class="error">Error: ${err.message}</div>`;
            document.getElementById('previewTable').innerHTML = '';
        }
    });
}

// --- CSV Export button handler (existing, unchanged) ---
if (exportBtn) {
    exportBtn.addEventListener('click', async () => {
        exportResult.textContent = 'Exporting...';
        const w = weekInput.value.trim();
        const m = monthInput.value.trim();
        const weeks = (document.getElementById('weeks')?.value || '').trim();
        const months = (document.getElementById('months')?.value || '').trim();
        const years = (document.getElementById('years')?.value || '').trim();
        const all = document.getElementById('exportAll')?.checked;
        // If not all, require at least one filter
        if (!all && !w && !m && !weeks && !months && !years) {
            exportResult.textContent = 'Please provide filters (week+month) or weeks, months, years, or check All.';
            return;
        }
        const params = new URLSearchParams();
        if (all) params.set('all', 'true');
        if (w) params.set('week', w);
        if (m) params.set('month', m);
        if (weeks) params.set('weeks', weeks);
        if (months) params.set('months', months);
        if (years) params.set('years', years);
        try {
            const res = await fetch(`/api/export?${params.toString()}`);
            if (res.ok) {
                const blob = await res.blob();
                const url = URL.createObjectURL(blob);
                const a = document.createElement('a');
                // filename from params (safe fallback)
                const fileName = `export_${Date.now()}.csv`;
                a.href = url;
                a.download = fileName;
                document.body.appendChild(a);
                a.click();
                a.remove();
                URL.revokeObjectURL(url);
                exportResult.textContent = 'Export completed.';
            } else {
                const data = await res.json().catch(() => ({ message: 'Export failed' }));
                exportResult.textContent = data.message || 'Export failed.';
            }
        } catch (err) {
            exportResult.textContent = `Error: ${err.message}`;
        }
    });
}

// --- DB View handler (existing, unchanged) ---
viewDbBtn.addEventListener('click', async () => {
    dbView.innerHTML = 'Loading...';
    try {
        const res = await fetch('/api/view-db');
        const data = await res.json();
        if (data.ok && data.columns && data.rows) {
            renderTable(dbView, data.columns, data.rows, false);
// OR just omit the fourth arg (default is false)
        } else {
            dbView.innerHTML = '<div>No data.</div>';
        }
    } catch (err) {
        dbView.innerHTML = `<div class="error">Error: ${err.message}</div>`;
    }
});
// Auto-load DB on page open
viewDbBtn.click();

// --- KPI Analysis (the only area with changed row logic) ---
async function loadAnalysis() {
    if (!analysisDiv) return;
    analysisDiv.innerHTML = 'Loading...';
    try {
        const res = await fetch('/api/analysis');
        const data = await res.json();
        if (!data.ok) {
            analysisDiv.innerHTML = `<div class="result">${data.message || 'No analysis available'}</div>`;
            return;
        }
        // Only these columns
        const cols = ['Region', 'Circle', 'Count', 'Workdone Week', 'Booking Month', 'Uploaded'];
        const rows = (data.analysis || []).map(a => ({
            Region: a["Region"],
            Circle: a["Circle"],
            Count: a["Count"],
            "Workdone Week": a["Workdone Week"],
            "Booking Month": a["Booking Month"],
            Uploaded: a["Uploaded"]
        }));

        // Info banner with current week/month
        const info = document.createElement('div');
        info.className = 'result';
        info.textContent = `Current Workdone Week: ${data.week || '-'} | Booking Month: ${data.month || '-'}${data.fallback ? ' (fallback used)' : ''}`;
        analysisDiv.innerHTML = '';
        analysisDiv.appendChild(info);

        renderTable(analysisDiv, cols, rows, true);

    } catch (err) {
        analysisDiv.innerHTML = `<div class="error">Error loading analysis: ${err.message}</div>`;
    }
}

if (analysisRefresh) analysisRefresh.addEventListener('click', loadAnalysis);
loadAnalysis();

const searchForm = document.getElementById('searchForm');
const adminTable = document.getElementById('adminTable');

if (searchForm) {
  searchForm.addEventListener('submit', async (e) => {
    e.preventDefault();
    const data = {};
    for (const input of searchForm.querySelectorAll('input[name]')) {
      if (input.value.trim()) data[input.name] = input.value.trim();
    }
    const res = await fetch('/api/search-db', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(data)
    });
    const obj = await res.json();
    if (obj.ok) {
  renderAdminTable(obj.columns, obj.rows);
} else {
  adminTable.innerHTML = `<div class="error">${obj.message || 'Search failed'}</div>`;
}
  });
}

function renderAdminTable(columns, rows) {
  let html = '<table><thead><tr>';
  columns.forEach(col => { html += `<th>${col}</th>`; });
  html += '<th>Actions</th></tr></thead><tbody>';

  rows.forEach(row => {
    html += '<tr>';
    columns.forEach(col => {
      // Disable editing for Id, Business Category, and Category
      if (col === "Id" || col === "Business Category" || col === "Category") {
        html += `<td><input data-id="${row.Id}" data-col="${col}" value="${row[col] || ''}" readonly style="background:#f0f0f0;" /></td>`;
      } else {
        html += `<td><input data-id="${row.Id}" data-col="${col}" value="${row[col] || ''}" /></td>`;
      }
    });
    html += `<td><button onclick="updateRow('${row.Id}')">Save</button></td>`;
    html += '</tr>';
  });

  html += '</tbody></table>';
  document.getElementById('adminTable').innerHTML = html;
}


// Update record
window.updateRow = async function (id) {
  const inputs = adminTable.querySelectorAll(`input[data-id="${id}"]`);
  const data = {Id: id};
  inputs.forEach(input => {
    if (input.getAttribute("data-col") !== "Id")
      data[input.getAttribute("data-col")] = input.value;
  });

  // Grab or create error display area in Data Management section
  let errorDiv = document.getElementById('dataMgmtError');
  if (!errorDiv) {
    errorDiv = document.createElement('div');
    errorDiv.id = 'dataMgmtError';
    adminTable.parentNode.insertBefore(errorDiv, adminTable);
  }
  errorDiv.innerHTML = ""; // clear previous errors

  try {
    const res = await fetch('/api/update-row', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(data)
    });
    const obj = await res.json();

    if (obj.ok) {
      errorDiv.innerHTML = '<div class="success">Updated!</div>';
    } else {
      // Compose error HTML
      let html = `<div class="error"><b>${obj.message || 'Update failed.'}</b>`;
      if (obj.errors && Array.isArray(obj.errors) && obj.errors.length > 0) {
        html += '<ul>';
        for (const err of obj.errors) html += `<li>${err}</li>`;
        html += '</ul>';
      }
      html += '</div>';
      errorDiv.innerHTML = html;
    }
  } catch (e) {
    errorDiv.innerHTML = `<div class="error">Error: ${e.message}</div>`;
  }
};


// Delete record
window.deleteRow = async function(id) {
  if (!confirm('Are you sure you want to delete this row?')) return;
  const res = await fetch('/api/delete-row', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({Id: id})
  });
  const obj = await res.json();
  alert(obj.ok ? 'Deleted!' : 'Delete failed.');
  if (obj.ok) {
    // Remove the row from table UI
    adminTable.querySelectorAll(`input[data-id="${id}"]`).forEach(input => {
      input.closest('tr').remove();
    });
  }
};

const logoutBtn = document.getElementById('logoutBtn');
if (logoutBtn) {
  logoutBtn.addEventListener('click', () => {
    sessionStorage.removeItem('isLoggedIn');
    window.location.href = 'login.html'; // redirect to login page
  });
}

const form = document.getElementById('login-form');
const errorDiv = document.getElementById('login-error');

form.addEventListener('submit', async (e) => {
  e.preventDefault();

  errorDiv.textContent = '';
  const username = document.getElementById('username').value.trim();
  const password = document.getElementById('password').value.trim();

  try {
    const res = await fetch('/api/login', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ username, password })
    });

    if (!res.ok) {
      errorDiv.textContent = 'Invalid username or password.';
      return;
    }

    const data = await res.json();
    if (data.ok) {
      sessionStorage.setItem('isLoggedIn', 'true');
      sessionStorage.setItem('isAdmin', data.isAdmin ? 'true' : 'false');
      window.location.href = 'index.html';
    } else {
      errorDiv.textContent = 'Invalid username or password.';
    }

  } catch (err) {
    errorDiv.textContent = 'Login failed, please try again later.';
  }
});

const addUserBtn = document.getElementById('addUserBtn');
const addUserModal = document.getElementById('addUserModal');
const cancelAddUserBtn = document.getElementById('cancelAddUserBtn');
const addUserForm = document.getElementById('addUserForm');
const addUserMessage = document.getElementById('addUserMessage');

addUserBtn.addEventListener('click', () => {
  addUserModal.style.display = 'block';
  addUserMessage.textContent = '';
  addUserForm.reset();
});

cancelAddUserBtn.addEventListener('click', () => {
  addUserModal.style.display = 'none';
  addUserMessage.textContent = '';
});

addUserForm.addEventListener('submit', async (e) => {
  e.preventDefault();
  addUserMessage.style.color = 'red'; // reset to error color
  addUserMessage.textContent = '';

  const username = addUserForm.Username.value.trim();
  const password = addUserForm.Password.value;
  const isAdmin = addUserForm.IsAdmin.checked;

  if (!username || !password) {
    addUserMessage.textContent = 'Username and password are required.';
    return;
  }

  try {
    const res = await fetch('/api/users/create', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ username, password, isAdmin })
    });

    const data = await res.json();

    if (data.ok) {
      addUserMessage.style.color = 'green';
      addUserMessage.innerHTML = `
        User created successfully!<br>
        <b>Username:</b> ${username}<br>
        <b>Is Admin:</b> ${isAdmin}<br>
        (Password hidden for security)
      `;
      addUserForm.reset();
      // Optionally refresh your user list here if you have a function for that
      // e.g. loadUsers();
    } else {
      addUserMessage.textContent = data.message || 'Failed to create user.';
    }
  } catch (err) {
    addUserMessage.textContent = 'Error creating user: ' + err.message;
  }
});




