import { useCallback, useEffect, useRef, useState } from "react";
import AuthModal from "./components/AuthModal";
import LandingPage from "./components/LandingPage";
import Dashboard from "./components/Dashboard";
import { MIN_PASSWORD_LEN } from "./lib/constants";
import { normalizeEmail } from "./lib/format";
import { request } from "./lib/api";

const emptyRegister = { name: "", email: "", password: "" };
const emptyLogin = { email: "", password: "" };

function getMsg(err, fallback) {
  return err?.message || fallback;
}

function isAuthFail(res) {
  return res?.status === 401 || res?.status === 403;
}

function validateLogin(form) {
  if (!normalizeEmail(form.email)) return "Email e obrigatorio.";
  if ((form.password || "").length < MIN_PASSWORD_LEN)
    return `Senha deve ter no minimo ${MIN_PASSWORD_LEN} caracteres.`;
  return "";
}

function validateRegister(form) {
  if (!form.name?.trim()) return "Nome e obrigatorio.";
  if (!normalizeEmail(form.email)) return "Email e obrigatorio.";
  if ((form.password || "").length < MIN_PASSWORD_LEN)
    return `Senha deve ter no minimo ${MIN_PASSWORD_LEN} caracteres.`;
  return "";
}

export default function App() {
  const didInit = useRef(false); // evita double-bootstrap do StrictMode (dev)

  // forms
  const [registerForm, setRegisterForm] = useState(emptyRegister);
  const [loginForm, setLoginForm] = useState(emptyLogin);

  // data
  const [session, setSession] = useState(null);
  const [devices, setDevices] = useState([]);
  const [pairingState, setPairingState] = useState(null);
  const [pairingInput, setPairingInput] = useState("");
  const [pairingAlias, setPairingAlias] = useState("");
  const [wsConnected, setWsConnected] = useState(false);
  const [wsEvents, setWsEvents] = useState([]);
  const [wsMetrics, setWsMetrics] = useState({
    created: 0,
    running: 0,
    success: 0,
    failed: 0,
    cancelled: 0,
  });

  // ui
  const [authMode, setAuthMode] = useState(null); // "login" | "register" | null
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(true);

  const resetSignedOutState = useCallback(() => {
    setSession(null);
    setDevices([]);
    setPairingState(null);
    setPairingInput("");
    setPairingAlias("");
    setWsConnected(false);
    setWsEvents([]);
    setWsMetrics({ created: 0, running: 0, success: 0, failed: 0, cancelled: 0 });
  }, []);

  const loadDevices = useCallback(async () => {
    const res = await request("/devices", { method: "GET" });

    if (!res.ok) {
      if (isAuthFail(res)) {
        resetSignedOutState();
        return;
      }
      setDevices([]);
      setError(res.data?.message || "Nao foi possivel listar dispositivos");
      return;
    }

    setDevices(Array.isArray(res.data?.items) ? res.data.items : []);
    if (res.data?.session) setSession(res.data.session);
  }, [resetSignedOutState]);

  const loadPairingState = useCallback(async () => {
    const res = await request("/pairing/code", { method: "GET" });

    if (!res.ok) {
      // pairing pode não existir ainda, sem ser erro fatal
      setPairingState({ paired: false, requiresPairing: true });
      return;
    }

    setPairingState({
      paired: Boolean(res.data?.paired),
      requiresPairing: Boolean(res.data?.requiresPairing),
      code: res.data?.code || "",
      linkedAt: res.data?.linkedAt || null,
      userEmail: res.data?.userEmail || null,
    });
  }, []);

  const bootstrap = useCallback(async () => {
    setLoading(true);
    setError("");

    try {
      const sessionRes = await request("/session", { method: "GET" });

      if (!sessionRes.ok) {
        resetSignedOutState();
        return;
      }

      setSession(sessionRes.data?.session || null);

      // não deixe um endpoint derrubar os outros
      await Promise.allSettled([loadDevices(), loadPairingState()]);
    } catch (err) {
      setError(getMsg(err, "Falha ao carregar sessao"));
    } finally {
      setLoading(false);
    }
  }, [loadDevices, loadPairingState, resetSignedOutState]);

  useEffect(() => {
    if (didInit.current) return;
    didInit.current = true;
    bootstrap();
  }, [bootstrap]);

  const handleLogin = useCallback(
    async (event) => {
      event.preventDefault();
      const v = validateLogin(loginForm);
      if (v) return setError(v);

      setLoading(true);
      setError("");

      try {
        const res = await request("/login", {
          method: "POST",
          body: JSON.stringify({
            email: normalizeEmail(loginForm.email),
            password: loginForm.password,
          }),
        });

        if (!res.ok) {
          setError(res.data?.message || "Falha no login");
          return;
        }

        setAuthMode(null);
        await bootstrap();
      } catch (err) {
        setError(getMsg(err, "Erro ao logar"));
      } finally {
        setLoading(false);
      }
    },
    [loginForm, bootstrap]
  );

  const handleRegister = useCallback(
    async (event) => {
      event.preventDefault();
      const v = validateRegister(registerForm);
      if (v) return setError(v);

      setLoading(true);
      setError("");

      try {
        const res = await request("/register", {
          method: "POST",
          body: JSON.stringify({
            name: registerForm.name.trim(),
            email: normalizeEmail(registerForm.email),
            password: registerForm.password,
          }),
        });

        if (!res.ok) {
          setError(res.data?.message || "Falha no cadastro");
          return;
        }

        setAuthMode(null);
        await bootstrap();
      } catch (err) {
        setError(getMsg(err, "Erro ao cadastrar"));
      } finally {
        setLoading(false);
      }
    },
    [registerForm, bootstrap]
  );

  const handleLogout = useCallback(async () => {
    setLoading(true);
    setError("");

    try {
      await request("/logout", { method: "POST" });
      resetSignedOutState();
    } catch (err) {
      setError(getMsg(err, "Erro ao fazer logout"));
    } finally {
      setLoading(false);
    }
  }, [resetSignedOutState]);

  const handleActivatePairing = useCallback(async () => {
    const code = (pairingInput || "").trim().toUpperCase();
    if (!code) return setError("Informe o codigo do agente para ativacao.");

    setLoading(true);
    setError("");

    try {
      const res = await request("/pairing/activate", {
        method: "POST",
        body: JSON.stringify({
          code,
          alias: (pairingAlias || "").trim() || null,
        }),
      });

      if (!res.ok) {
        setError(res.data?.message || "Falha ao ativar agente");
        return;
      }

      setPairingInput("");
      setPairingAlias("");

      await Promise.allSettled([loadDevices(), loadPairingState()]);
    } catch (err) {
      setError(getMsg(err, "Erro ao ativar agente"));
    } finally {
      setLoading(false);
    }
  }, [pairingInput, pairingAlias, loadDevices, loadPairingState]);

  useEffect(() => {
    if (!session) return;
    const envUrl = import.meta.env.VITE_AGENT_WS_URL;
    const defaultUrl = `${window.location.protocol === "https:" ? "wss" : "ws"}://${window.location.hostname}:8091/ws/keeply`;
    const wsUrl = envUrl || defaultUrl;
    let ws;
    try {
      ws = new WebSocket(wsUrl);
    } catch {
      setWsConnected(false);
      return;
    }

    ws.onopen = () => setWsConnected(true);
    ws.onclose = () => setWsConnected(false);
    ws.onerror = () => setWsConnected(false);
    ws.onmessage = (ev) => {
      try {
        const msg = JSON.parse(ev.data);
        const type = msg?.type || "unknown";
        const payload = msg?.payload || {};
        const ts = payload?.ts || new Date().toISOString();
        const item = { type, ts, payload };
        setWsEvents((prev) => [item, ...prev].slice(0, 30));

        setWsMetrics((prev) => {
          const next = { ...prev };
          if (type === "scan.created") next.created += 1;
          if (type === "scan.running") next.running += 1;
          if (type === "scan.success") next.success += 1;
          if (type === "scan.failed") next.failed += 1;
          if (type === "scan.cancelled") next.cancelled += 1;
          return next;
        });
      } catch {
        // ignore malformed frame
      }
    };

    return () => {
      try {
        ws.close();
      } catch {}
    };
  }, [session]);

  const handleDeleteDevice = useCallback(
    async (deviceId) => {
      if (!deviceId) return;

      const ok = window.confirm("Excluir/desvincular este dispositivo?");
      if (!ok) return;

      setLoading(true);
      setError("");

      try {
        const res = await request(`/devices/${deviceId}`, { method: "DELETE" });

        if (!res.ok) {
          if (isAuthFail(res)) resetSignedOutState();
          else setError(res.data?.message || "Falha ao excluir dispositivo");
          return;
        }

        await loadDevices();
      } catch (err) {
        setError(getMsg(err, "Erro ao excluir dispositivo"));
      } finally {
        setLoading(false);
      }
    },
    [loadDevices, resetSignedOutState]
  );

  // UI states
  if (loading && !session) {
    return (
      <main className="page">
        <h1 className="h1">Keeply</h1>
        <p className="mutedBlock">Carregando...</p>
      </main>
    );
  }

  if (!session) {
    return (
      <>
        <LandingPage
          loading={loading}
          onOpenLogin={() => {
            setError("");
            setAuthMode("login");
          }}
          onOpenRegister={() => {
            setError("");
            setAuthMode("register");
          }}
        />

        <AuthModal
          mode={authMode}
          onClose={() => setAuthMode(null)}
          loading={loading}
          error={error}
          registerForm={registerForm}
          setRegisterForm={setRegisterForm}
          loginForm={loginForm}
          setLoginForm={setLoginForm}
          onLogin={handleLogin}
          onRegister={handleRegister}
        />
      </>
    );
  }

  return (
    <Dashboard
      session={session}
      devices={devices}
      pairingState={pairingState}
      pairingInput={pairingInput}
      setPairingInput={setPairingInput}
      pairingAlias={pairingAlias}
      setPairingAlias={setPairingAlias}
      wsConnected={wsConnected}
      wsEvents={wsEvents}
      wsMetrics={wsMetrics}
      loading={loading}
      error={error}
      onActivatePairing={handleActivatePairing}
      onDeleteDevice={handleDeleteDevice}
      onRefresh={bootstrap}
      onLogout={handleLogout}
    />
  );
}
