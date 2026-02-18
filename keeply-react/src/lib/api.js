export const API_BASE = import.meta.env.VITE_API_BASE || "/api/keeply-ws";

export async function request(path, options = {}) {
  let response;
  try {
    response = await fetch(`${API_BASE}${path}`, {
      method: options.method || "GET",
      // Keeply ws/auth flow is token-based, so cross-origin requests should not force cookies.
      credentials: options.credentials ?? "same-origin",
      headers: {
        "Content-Type": "application/json",
        ...(options.headers || {}),
      },
      ...options,
    });
  } catch (err) {
    const message =
      `Falha de conexao com a API (${API_BASE}). ` +
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
      };
    }
  }

  return { ok: response.ok, status: response.status, data };
}