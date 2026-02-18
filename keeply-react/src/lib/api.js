export const API_BASE = import.meta.env.VITE_API_BASE || "/api/keeply-ws";
export const AGENT_API_BASE = import.meta.env.VITE_AGENT_API_BASE || "/api/keeply";

async function doRequest(base, label, path, options = {}) {
  let response;
  try {
    response = await fetch(`${base}${path}`, {
      method: options.method || "GET",
      credentials: options.credentials ?? "same-origin",
      headers: {
        "Content-Type": "application/json",
        ...(options.headers || {}),
      },
      ...options,
    });
  } catch (err) {
    const message =
      `Falha de conexao com a API ${label} (${base}). ` +
      "Verifique se o backend esta no ar e se a URL da API esta correta.";
    throw new Error(message, { cause: err });
  }

  const text = await response.text();
  let data = {};
  if (text) {
    try {
      data = JSON.parse(text);
    } catch {
      data = {
        message: "Resposta nao-JSON do servidor",
        raw: text.slice(0, 300),
        url: response.url,
        contentType: response.headers.get("content-type") || "",
      };
    }
  }

  return { ok: response.ok, status: response.status, data };
}

export async function request(path, options = {}) {
  return doRequest(API_BASE, "web", path, options);
}

export async function agentRequest(path, options = {}) {
  return doRequest(AGENT_API_BASE, "do agente", path, options);
}
