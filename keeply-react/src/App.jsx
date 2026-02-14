import { useMemo, useState } from "react";
import { getHealth, getHistory, startScan } from "./api";

export default function App() {
  const [health, setHealth] = useState(null);
  const [history, setHistory] = useState([]);
  const [loadingHealth, setLoadingHealth] = useState(false);
  const [loadingHistory, setLoadingHistory] = useState(false);
  const [scanLoading, setScanLoading] = useState(false);
  const [error, setError] = useState("");
  const [scanForm, setScanForm] = useState({
    root: "",
    dest: "",
    password: ""
  });

  const statusClass = useMemo(() => {
    if (!health) return "status status-neutral";
    return health.ok ? "status status-ok" : "status status-bad";
  }, [health]);

  async function loadHealth() {
    setLoadingHealth(true);
    setError("");
    try {
      const data = await getHealth();
      setHealth(data);
    } catch (e) {
      setError(e.message);
    } finally {
      setLoadingHealth(false);
    }
  }

  async function loadHistory() {
    setLoadingHistory(true);
    setError("");
    try {
      const data = await getHistory(15);
      setHistory(data.items || []);
    } catch (e) {
      setError(e.message);
    } finally {
      setLoadingHistory(false);
    }
  }

  async function handleScan(event) {
    event.preventDefault();
    setScanLoading(true);
    setError("");
    try {
      await startScan(scanForm);
      await loadHistory();
    } catch (e) {
      setError(e.message);
    } finally {
      setScanLoading(false);
    }
  }

  return (
    <main className="shell">
      <section className="hero">
        <h1>Keeply Control Panel</h1>
        <p>Frontend React para operar o serviço Keeply em tempo real.</p>
      </section>

      <section className="grid">
        <article className="card">
          <header className="card-head">
            <h2>API Health</h2>
            <button onClick={loadHealth} disabled={loadingHealth}>
              {loadingHealth ? "Consultando..." : "Atualizar"}
            </button>
          </header>

          <div className={statusClass}>
            {health ? (health.ok ? "ONLINE" : "OFFLINE") : "SEM DADOS"}
          </div>

          {health && <pre>{JSON.stringify(health, null, 2)}</pre>}
        </article>

        <article className="card">
          <header className="card-head">
            <h2>Executar Scan</h2>
          </header>
          <form onSubmit={handleScan} className="scan-form">
            <label>
              Root
              <input
                value={scanForm.root}
                onChange={(e) => setScanForm((prev) => ({ ...prev, root: e.target.value }))}
                placeholder="C:/origem"
                required
              />
            </label>
            <label>
              Dest
              <input
                value={scanForm.dest}
                onChange={(e) => setScanForm((prev) => ({ ...prev, dest: e.target.value }))}
                placeholder="D:/destino"
                required
              />
            </label>
            <label>
              Password (opcional)
              <input
                value={scanForm.password}
                onChange={(e) => setScanForm((prev) => ({ ...prev, password: e.target.value }))}
                type="password"
              />
            </label>
            <button type="submit" disabled={scanLoading}>
              {scanLoading ? "Rodando scan..." : "Iniciar Scan"}
            </button>
          </form>
        </article>
      </section>

      <section className="card">
        <header className="card-head">
          <h2>Historico</h2>
          <button onClick={loadHistory} disabled={loadingHistory}>
            {loadingHistory ? "Carregando..." : "Recarregar"}
          </button>
        </header>

        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                <th>ID</th>
                <th>Status</th>
                <th>Tipo</th>
                <th>Inicio</th>
                <th>Fim</th>
                <th>Arquivos</th>
                <th>Erros</th>
              </tr>
            </thead>
            <tbody>
              {history.length === 0 ? (
                <tr>
                  <td colSpan="7">Sem registros ainda.</td>
                </tr>
              ) : (
                history.map((row) => (
                  <tr key={row.id}>
                    <td>{row.id}</td>
                    <td>{row.status || "-"}</td>
                    <td>{row.backupType || "-"}</td>
                    <td>{row.startedAt || "-"}</td>
                    <td>{row.finishedAt || "-"}</td>
                    <td>{row.filesProcessed ?? 0}</td>
                    <td>{row.errors ?? 0}</td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      </section>

      {error && <div className="error">Erro: {error}</div>}
    </main>
  );
}
