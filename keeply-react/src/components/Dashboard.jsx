import { memo, useCallback, useEffect, useMemo, useRef, useState } from "react";
import { formatLastSeen } from "../lib/format";

const EMPTY_ARR = [];
const EMPTY_OBJ = Object.freeze({});
const TABS = Object.freeze([
  { id: "inicio", label: "Início" },
  { id: "registro", label: "Registro" },
]);

function minutesAgo(iso) {
  if (!iso) return null;
  const t = Date.parse(iso);
  if (!Number.isFinite(t)) return null;
  return (Date.now() - t) / 60000;
}

function statusFromLastSeen(iso) {
  const m = minutesAgo(iso);
  if (m == null) return { label: "sem heartbeat", tone: "muted" };
  if (m <= 10) return { label: "ativo (estimado)", tone: "ok" };
  if (m <= 60) return { label: "recente (estimado)", tone: "warn" };
  return { label: "sem sinal (estimado)", tone: "danger" };
}

const Surface = memo(function Surface({ title, chip, right, children }) {
  const showHead = title || chip || right;
  return (
    <section className="surface">
      {showHead ? (
        <header className="surfaceHead">
          <div className="surfaceHeadLeft">
            {title ? <div className="h3">{title}</div> : null}
            {chip ? <span className="chip">{chip}</span> : null}
          </div>
          {right ? <div className="surfaceHeadRight">{right}</div> : null}
        </header>
      ) : null}
      {children}
    </section>
  );
});

const IconBtn = memo(function IconBtn({ title, onClick, disabled, children }) {
  return (
    <button
      className="iconBtn"
      type="button"
      title={title}
      aria-label={title}
      onClick={onClick}
      disabled={disabled}
    >
      {children}
    </button>
  );
});

const StatCard = memo(function StatCard({ label, value, tone }) {
  return (
    <div className="miniCard miniCard--tight" role="group" aria-label={label}>
      <div className="miniLabel">{label}</div>
      <div className={`miniValue${tone ? ` miniValue--${tone}` : ""}`}>{value}</div>
    </div>
  );
});

const NavTabs = memo(function NavTabs({ active, onChange }) {
  return (
    <nav className="navTabs" aria-label="Navegação">
      {TABS.map((t) => {
        const isActive = active === t.id;
        return (
          <button
            key={t.id}
            type="button"
            className={`navTab${isActive ? " navTab--active" : ""}`}
            onClick={() => onChange(t.id)}
            aria-current={isActive ? "page" : undefined}
          >
            {t.label}
          </button>
        );
      })}
    </nav>
  );
});

const StatusBadge = memo(function StatusBadge({ wsConnected, lastSeenAt }) {
  const s = !wsConnected ? { label: "ws offline", tone: "muted" } : statusFromLastSeen(lastSeenAt);
  return (
    <span className={`status status--${s.tone}`}>
      <span className="statusDot" aria-hidden="true" />
      {s.label}
    </span>
  );
});

const BarsChart = memo(function BarsChart({ bars, maxBar }) {
  return (
    <div className="wsChart" role="list" aria-label="Resumo de métricas">
      {bars.map((b) => {
        const pct = maxBar === 0 ? 0 : (b.value / maxBar) * 100;
        return (
          <div key={b.label} className="wsBarWrap" role="listitem">
            <div className="wsBarLabel">{b.label}</div>
            <div className="wsBarTrack" aria-hidden="true">
              <div className="wsBarFill" style={{ width: `${pct}%` }} />
            </div>
            <div className="wsBarValue">{b.value}</div>
          </div>
        );
      })}
    </div>
  );
});

function ProfileMenu({ email, loading, onViewProfile, onLogout }) {
  const [open, setOpen] = useState(false);
  const btnRef = useRef(null);
  const menuRef = useRef(null);

  const initial = (email || "P").trim()[0]?.toUpperCase() || "P";

  const close = useCallback(() => setOpen(false), []);
  const toggle = useCallback(() => setOpen((v) => !v), []);

  useEffect(() => {
    if (!open) return;

    const onDown = (e) => {
      const b = btnRef.current;
      const m = menuRef.current;
      if (!b || !m) return;
      if (b.contains(e.target) || m.contains(e.target)) return;
      setOpen(false);
    };

    document.addEventListener("pointerdown", onDown);
    return () => document.removeEventListener("pointerdown", onDown);
  }, [open]);

  return (
    <div className="profileMenu">
      <button
        ref={btnRef}
        type="button"
        className="profileBtn"
        onClick={toggle}
        aria-haspopup="menu"
        aria-expanded={open}
        disabled={loading}
        title="Abrir menu do perfil"
      >
        <span className="profileAvatar" aria-hidden="true">{initial}</span>
        <span className="profileBtnLabel">Conta</span>
        <span className="profileChevron" aria-hidden="true">▾</span>
      </button>

      {open ? (
        <div ref={menuRef} className="menu" role="menu" aria-label="Menu do perfil">
          <div className="menuHeader">
            <div className="menuTitle">Logado como</div>
            <div className="menuSub">{email || "-"}</div>
          </div>

          <button
            type="button"
            className="menuItem"
            role="menuitem"
            onClick={() => {
              close();
              onViewProfile?.();
            }}
            disabled={loading}
          >
            Ver perfil
          </button>

          <div className="menuSep" role="separator" />

          <button
            type="button"
            className="menuItem menuItem--danger"
            role="menuitem"
            onClick={() => {
              close();
              onLogout?.();
            }}
            disabled={loading}
          >
            Sair
          </button>
        </div>
      ) : null}
    </div>
  );
}

export default function Dashboard({
  session,
  devices = EMPTY_ARR,
  pairingState,
  pairingInput,
  setPairingInput,
  pairingAlias,
  setPairingAlias,
  wsConnected,
  wsEvents = EMPTY_ARR,
  wsMetrics,
  loading,
  error,
  onActivatePairing,
  onDeleteDevice,
  onRefresh,
  onLogout,
  onViewProfile, // <-- novo (opcional)
}) {
  const [tab, setTab] = useState("inicio");

  const metrics = wsMetrics ?? EMPTY_OBJ;
  const hasDevices = devices.length > 0;

  const onCopy = useCallback(async (text) => {
    if (!text) return;
    try {
      await navigator.clipboard?.writeText?.(text);
    } catch {
      // sem hardwork: se falhar, só ignora
    }
  }, []);

  const cols = useMemo(() => {
    return [
      { h: "Nome", v: (d) => d.machineName || "-" },
      { h: "Alias", v: (d) => d.machineAlias || "-" },
      { h: "OS", v: (d) => d.osName || "-" },
      { h: "Hostname", v: (d) => d.hostname || "-" },
      {
        h: "Status",
        v: (d) => <StatusBadge wsConnected={wsConnected} lastSeenAt={d.lastSeenAt} />,
      },
      { h: "Último acesso", v: (d) => formatLastSeen(d.lastSeenAt) },
    ];
  }, [wsConnected]);

  const { kpis, bars, maxBar } = useMemo(() => {
    const created = Math.max(0, Number(metrics.created ?? 0));
    const running = Math.max(0, Number(metrics.running ?? 0));
    const success = Math.max(0, Number(metrics.success ?? 0));
    const failed = Math.max(0, Number(metrics.failed ?? 0));
    const cancelled = Math.max(0, Number(metrics.cancelled ?? 0));

    const barsLocal = [
      { label: "Criados", value: created },
      { label: "Run", value: running },
      { label: "OK", value: success },
      { label: "Fail", value: failed },
      { label: "Cancel", value: cancelled },
    ];
    const maxLocal = barsLocal.reduce((acc, b) => (b.value > acc ? b.value : acc), 0);

    const kpisLocal = [
      { label: "Criados", value: created },
      { label: "Executando", value: running },
      { label: "Sucesso", value: success, tone: "blue" },
      { label: "Falha", value: failed, tone: "purple" },
      { label: "Cancelados", value: cancelled },
      { label: "Frames", value: wsEvents.length },
    ];

    return { kpis: kpisLocal, bars: barsLocal, maxBar: maxLocal };
  }, [metrics, wsEvents.length]);

  const recentEvents = useMemo(() => wsEvents.slice(0, 10), [wsEvents]);

  const onChangeTab = useCallback((t) => setTab(t), []);
  const onPairCodeChange = useCallback(
    (e) => setPairingInput(e.target.value.toUpperCase().trimStart()),
    [setPairingInput]
  );
  const onPairAliasChange = useCallback(
    (e) => setPairingAlias(e.target.value),
    [setPairingAlias]
  );

  const onPaste = useCallback(async () => {
    try {
      const t = await navigator.clipboard?.readText?.();
      if (t) setPairingInput(String(t).toUpperCase().trim());
    } catch {}
  }, [setPairingInput]);

  return (
    <div className="shell">
      <header className="appNav">
        <div className="appNavInner">
          <div className="navBrand" role="banner">
            <div className="brandName">
              <strong>Keeply</strong> <span className="brandTag">AGENT CONSOLE</span>
            </div>
          </div>

          <NavTabs active={tab} onChange={onChangeTab} />

          <div className="navRight">
            <button className="btn btn--ghost" onClick={onRefresh} disabled={loading}>
              Atualizar
            </button>

            <ProfileMenu
              email={session?.email}
              loading={loading}
              onViewProfile={onViewProfile}
              onLogout={onLogout}
            />
          </div>
        </div>
      </header>

      <main className="page">
        {error ? (
          <div className="alert alert--danger" role="alert">
            {error}
          </div>
        ) : null}

        {tab === "inicio" ? (
          <>
            <section className="section">
              <div className="row row--between row--wrap">
                <div className="h2">Seus dispositivos</div>
                <span className="pillSmall">{devices.length} dispositivo(s)</span>
              </div>

              <Surface>
                {devices.length === 0 ? (
                  <div className="mutedBlock" role="note">
                    Nenhum dispositivo encontrado.
                  </div>
                ) : (
                  <div className="tableWrap">
                    <table className="table">
                      <thead>
                        <tr>
                          {cols.map((c) => (
                            <th key={c.h} scope="col">
                              {c.h}
                            </th>
                          ))}
                          <th className="thActions" scope="col">
                            Ações
                          </th>
                        </tr>
                      </thead>

                      <tbody>
                        {devices.map((d, index) => {
                          const key = d.id || d.machineAlias || index;
                          return (
                            <tr key={key} className="rowHover">
                              {cols.map((c) => (
                                <td key={c.h}>{c.v(d)}</td>
                              ))}

                              <td className="tdActions">
                                <div className="row row--end">
                                  <IconBtn
                                    title="Copiar alias"
                                    onClick={() => onCopy(d.machineAlias || "")}
                                    disabled={!d.machineAlias}
                                  >
                                    ⧉
                                  </IconBtn>

                                  <button
                                    className="btn btn--dangerOutline"
                                    type="button"
                                    onClick={() => {
                                      if (!d.id) return;
                                      const name = d.machineName || d.machineAlias || "dispositivo";
                                      const ok = window.confirm(`Excluir/desvincular?\n\n${name}`);
                                      if (ok) onDeleteDevice?.(d.id);
                                    }}
                                    disabled={loading || !d.id}
                                  >
                                    Excluir
                                  </button>
                                </div>
                              </td>
                            </tr>
                          );
                        })}
                      </tbody>
                    </table>
                  </div>
                )}
              </Surface>
            </section>

            <section className="section">
              <Surface title="Métricas por WebSocket" chip={wsConnected ? "ws online" : "ws offline"}>
                {!hasDevices ? (
                  <div className="mutedBlock" role="note">
                    Vincule pelo menos um agente para exibir métricas.
                  </div>
                ) : (
                  <>
                    <div className="miniGrid miniGrid--top">
                      {kpis.map((k) => (
                        <StatCard key={k.label} label={k.label} value={k.value} tone={k.tone} />
                      ))}
                    </div>

                    <BarsChart bars={bars} maxBar={maxBar} />

                    <div className="eventsBlock">
                      <div className="h3">Eventos recebidos</div>
                      {wsEvents.length === 0 ? (
                        <div className="mutedBlock" role="note">
                          Aguardando frames no WebSocket...
                        </div>
                      ) : (
                        <ul className="pathList" aria-label="Últimos eventos">
                          {recentEvents.map((ev, i) => (
                            <li key={`${ev.type}-${ev.ts}-${i}`}>
                              <span className="pathDot" aria-hidden="true" />
                              <span className="pathText">
                                {ev.type} · {formatLastSeen(ev.ts)}
                              </span>
                            </li>
                          ))}
                        </ul>
                      )}
                    </div>
                  </>
                )}
              </Surface>
            </section>
          </>
        ) : null}

        {tab === "registro" ? (
          <section className="section">
            <Surface
              title="Ativar agente por código"
              chip="pairing"
              right={
                pairingState?.paired ? (
                  <span className="status status--ok">
                    <span className="statusDot" aria-hidden="true" />
                    agente local cadastrado
                  </span>
                ) : pairingState?.code ? (
                  <span className="pillSmall">
                    Código: <strong>{pairingState.code}</strong>
                  </span>
                ) : null
              }
            >
              <div className="mutedBlock" role="note">
                Cole o código exibido no agente.
              </div>

              <div className="pairRow">
                <input
                  className="fieldInput"
                  placeholder="Código do agente (ex: ABCDE-234FG)"
                  value={pairingInput}
                  onChange={onPairCodeChange}
                  inputMode="text"
                  autoCapitalize="characters"
                  autoCorrect="off"
                  spellCheck={false}
                />

                <input
                  className="fieldInput"
                  placeholder="Alias do agente (opcional, ex: servidor-sala)"
                  value={pairingAlias || ""}
                  onChange={onPairAliasChange}
                />

                <div className="row">
                  <button className="btn btn--soft" type="button" disabled={loading} onClick={onPaste}>
                    Colar
                  </button>

                  <button
                    className="btn btn--primary"
                    type="button"
                    onClick={onActivatePairing}
                    disabled={loading || !pairingInput?.trim()}
                  >
                    Ativar agente
                  </button>
                </div>
              </div>
            </Surface>
          </section>
        ) : null}
      </main>
    </div>
  );
}
