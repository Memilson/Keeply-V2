import { useState } from "react";

const API_BASE = "/api/auth/api/auth";
const MIN_PASSWORD_LEN = 6;

async function request(path, options = {}) {
  const response = await fetch(`${API_BASE}${path}`, {
    headers: {
      "Content-Type": "application/json",
      ...(options.headers || {}),
    },
    ...options,
  });

  const text = await response.text();
  const data = text ? JSON.parse(text) : {};
  return { ok: response.ok, status: response.status, data };
}

export default function App() {
  const [registerForm, setRegisterForm] = useState({
    name: "",
    email: "",
    password: "",
  });
  const [loginForm, setLoginForm] = useState({ email: "", password: "" });
  const [token, setToken] = useState("");
  const [result, setResult] = useState(null);
  const [formError, setFormError] = useState("");
  const [loading, setLoading] = useState(false);

  function normalizeEmail(email) {
    return email.trim().toLowerCase();
  }

  function validateRegisterForm() {
    if (!registerForm.name.trim()) return "Nome completo e obrigatorio.";
    if (!normalizeEmail(registerForm.email)) return "Email e obrigatorio.";
    if (registerForm.password.length < MIN_PASSWORD_LEN) {
      return `Senha deve ter no minimo ${MIN_PASSWORD_LEN} caracteres.`;
    }
    return "";
  }

  function validateLoginForm() {
    if (!normalizeEmail(loginForm.email)) return "Email e obrigatorio.";
    if (loginForm.password.length < MIN_PASSWORD_LEN) {
      return `Senha deve ter no minimo ${MIN_PASSWORD_LEN} caracteres.`;
    }
    return "";
  }

  async function handleStatus() {
    setFormError("");
    setLoading(true);
    try {
      const res = await request("/status", { method: "GET" });
      setResult(res);
    } catch (error) {
      setResult({ ok: false, status: 0, data: { message: error.message } });
    } finally {
      setLoading(false);
    }
  }

  async function handleRegister(event) {
    event.preventDefault();
    setFormError("");
    const validationError = validateRegisterForm();
    if (validationError) {
      setFormError(validationError);
      return;
    }

    setLoading(true);
    try {
      const res = await request("/register", {
        method: "POST",
        body: JSON.stringify({
          name: registerForm.name.trim(),
          email: normalizeEmail(registerForm.email),
          password: registerForm.password,
        }),
      });
      if (res.ok && res.data?.token) setToken(res.data.token);
      setResult(res);
    } catch (error) {
      setResult({ ok: false, status: 0, data: { message: error.message } });
    } finally {
      setLoading(false);
    }
  }

  async function handleLogin(event) {
    event.preventDefault();
    setFormError("");
    const validationError = validateLoginForm();
    if (validationError) {
      setFormError(validationError);
      return;
    }

    setLoading(true);
    try {
      const res = await request("/login", {
        method: "POST",
        body: JSON.stringify({
          email: normalizeEmail(loginForm.email),
          password: loginForm.password,
        }),
      });
      if (res.ok && res.data?.token) setToken(res.data.token);
      setResult(res);
    } catch (error) {
      setResult({ ok: false, status: 0, data: { message: error.message } });
    } finally {
      setLoading(false);
    }
  }

  return (
    <main className="page">
      <h1>Keeply Auth Test (React)</h1>
      <p>Proxy atual usa base: {API_BASE}</p>
      <p className="hint">Schema: full_name, email unico (citext), password_hash.</p>
      <p className="hint">Regras aplicadas: nome obrigatorio, email obrigatorio, senha minima de 6 caracteres.</p>

      {formError ? <p className="error">{formError}</p> : null}

      <section className="panel">
        <button onClick={handleStatus} disabled={loading}>
          {loading ? "Aguarde..." : "Checar status auth"}
        </button>
      </section>

      <section className="grid">
        <form className="panel" onSubmit={handleRegister}>
          <h2>Register</h2>
          <label>
            Nome
            <input
              value={registerForm.name}
              onChange={(event) =>
                setRegisterForm((prev) => ({ ...prev, name: event.target.value }))
              }
              placeholder="Nome completo"
              required
            />
          </label>
          <label>
            Email
            <input
              type="email"
              value={registerForm.email}
              onChange={(event) =>
                setRegisterForm((prev) => ({ ...prev, email: event.target.value }))
              }
              placeholder="voce@dominio.com"
              required
            />
          </label>
          <label>
            Senha
            <input
              type="password"
              value={registerForm.password}
              onChange={(event) =>
                setRegisterForm((prev) => ({ ...prev, password: event.target.value }))
              }
              minLength={MIN_PASSWORD_LEN}
              placeholder={`Minimo ${MIN_PASSWORD_LEN} caracteres`}
              required
            />
          </label>
          <button type="submit" disabled={loading}>
            Registrar
          </button>
        </form>

        <form className="panel" onSubmit={handleLogin}>
          <h2>Login</h2>
          <label>
            Email
            <input
              type="email"
              value={loginForm.email}
              onChange={(event) =>
                setLoginForm((prev) => ({ ...prev, email: event.target.value }))
              }
              placeholder="voce@dominio.com"
              required
            />
          </label>
          <label>
            Senha
            <input
              type="password"
              value={loginForm.password}
              onChange={(event) =>
                setLoginForm((prev) => ({ ...prev, password: event.target.value }))
              }
              minLength={MIN_PASSWORD_LEN}
              placeholder={`Minimo ${MIN_PASSWORD_LEN} caracteres`}
              required
            />
          </label>
          <button type="submit" disabled={loading}>
            Entrar
          </button>
        </form>
      </section>

      <section className="panel">
        <h2>Token</h2>
        <textarea readOnly rows={3} value={token} placeholder="Token aparece aqui" />
      </section>

      <section className="panel">
        <h2>Resposta</h2>
        <pre>{result ? JSON.stringify(result, null, 2) : "Sem resposta ainda."}</pre>
      </section>
    </main>
  );
}
