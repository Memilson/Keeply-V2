const apiBase = "/api/keeply";

async function readJson(url, options) {
  const response = await fetch(url, options);
  const data = await response.json().catch(() => ({}));
  if (!response.ok) {
    const message = data.error || `HTTP ${response.status}`;
    throw new Error(message);
  }
  return data;
}

export async function getHealth() {
  return readJson(`${apiBase}/health`);
}

export async function getHistory(limit = 20) {
  return readJson(`${apiBase}/history?limit=${limit}`);
}

export async function startScan(payload) {
  return readJson(`${apiBase}/scan`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json"
    },
    body: JSON.stringify(payload)
  });
}
