import { memo, useCallback, useEffect, useMemo, useRef, useState } from "react";
import { agentRequest } from "../lib/api";
import { formatLastSeen } from "../lib/format";

const EMPTY_ARR = Object.freeze([]);
const EMPTY_OBJ = Object.freeze({});

const TABS = Object.freeze([
  { id: "inicio", label: "Início" },
  { id: "registro", label: "Registro" },
  { id: "atividades", label: "Atividades" },
]);

const WEEK_DAYS = Object.freeze([
  { id: "mon", label: "Seg" },
  { id: "tue", label: "Ter" },
  { id: "wed", label: "Qua" },
  { id: "thu", label: "Qui" },
  { id: "fri", label: "Sex" },
  { id: "sat", label: "Sáb" },
  { id: "sun", label: "Dom" },
]);
const CONFIG_STORAGE_PREFIX = "keeply_device_config_v1:";
const WINDOWS_LEGACY_DEST = "C:\\Temp\\keeply";
const WINDOWS_DEFAULT_DEST = "C:\\Users\\angel\\AppData\\Roaming\\Keeply\\backup-store";

function getConfigStorageKey(deviceKey) {
  return `${CONFIG_STORAGE_PREFIX}${deviceKey || "unknown"}`;
}

const clampCount = (v) => {
  const n = Number(v);
  return Number.isFinite(n) ? Math.max(0, n) : 0;
};

const clampInt = (v, min, max, fallback) => {
  const n = Number(v);
  if (!Number.isFinite(n)) return fallback;
  return Math.min(max, Math.max(min, Math.trunc(n)));
};

const deviceKeyOf = (d) =>
  String(d?.id || d?.machineAlias || d?.hostname || d?.machineName || "");

function minutesAgo(iso, nowMs) {
  if (!iso) return null;
  const t = Date.parse(iso);
  if (!Number.isFinite(t)) return null;
  return (nowMs - t) / 60000;
}

function statusFromLastSeen(iso, nowMs) {
  const m = minutesAgo(iso, nowMs);
  if (m == null) return { label: "sem heartbeat", tone: "muted" };
  if (m <= 10) return { label: "ativo (estimado)", tone: "ok" };
  if (m <= 60) return { label: "recente (estimado)", tone: "warn" };
  return { label: "sem sinal (estimado)", tone: "danger" };
}

// ---- presets “humanos” (label) + valor real (path) ----
function buildOriginOptions(isWindows, userName) {
  if (isWindows) {
    return [
      { label: "Backup do Usuário Local", value: `C:\\Users\\${userName}` },
      { label: "Backup de Todos Usuários (C:\\Users)", value: "C:\\Users" },
    ];
  }
  return [
    { label: "Backup do Usuário Local", value: `/home/${userName}` },
    { label: "Backup de Todos Usuários (/home)", value: "/home" },
  ];
}

function buildDestinationOptions(isWindows) {
  if (isWindows) {
    return [
      { label: "Destino Padrão (Roaming)", value: WINDOWS_DEFAULT_DEST },
      { label: "Destino Público (Compartilhado)", value: "C:\\Users\\Public\\keeply" },
    ];
  }
  return [
    { label: "Destino Padrão (Temp)", value: "/tmp/keeply" },
    { label: "Destino Público (Compartilhado)", value: "/var/tmp/keeply" },
  ];
}

function normalizeSavedDestination(path, isWindows) {
  const raw = String(path || "").trim();
  if (!raw) return raw;
  if (!isWindows) return raw;
  return raw.toLowerCase() === WINDOWS_LEGACY_DEST.toLowerCase() ? WINDOWS_DEFAULT_DEST : raw;
}

function durationLabel(startIso, endMs = Date.now()) {
  if (!startIso) return "-";
  const start = Date.parse(startIso);
  if (!Number.isFinite(start)) return "-";
  const sec = Math.max(0, Math.floor((endMs - start) / 1000));
  const mm = Math.floor(sec / 60).toString().padStart(2, "0");
  const ss = (sec % 60).toString().padStart(2, "0");
  return `${mm}:${ss}`;
}

function msLabel(ms) {
  if (!Number.isFinite(ms) || ms <= 0) return "00:00";
  const sec = Math.floor(ms / 1000);
  const mm = Math.floor(sec / 60).toString().padStart(2, "0");
  const ss = (sec % 60).toString().padStart(2, "0");
  return `${mm}:${ss}`;
}

// ---- UI bits ----
const Surface = memo(function Surface({ title, chip, right, children }) {
  const showHead = !!(title || chip || right);
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

const StatusBadge = memo(function StatusBadge({ wsConnected, lastSeenAt, nowMs }) {
  const s = !wsConnected
    ? { label: "Agente Offline", tone: "muted" }
    : statusFromLastSeen(lastSeenAt, nowMs);

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

const DevicePicker = memo(function DevicePicker({
  devices,
  selectedKey,
  onSelect,
  wsConnected,
  nowMs,
}) {
  if (devices.length === 0) {
    return (
      <div className="mutedBlock" role="note">
        Nenhum dispositivo encontrado.
      </div>
    );
  }

  return (
    <div className="devicePickerList" role="list" aria-label="Lista de dispositivos">
      {devices.map((d) => {
        const key = deviceKeyOf(d);
        const active = key === selectedKey;
        return (
          <button
            key={key}
            type="button"
            className={`devicePickBtn${active ? " devicePickBtn--active" : ""}`}
            onClick={() => onSelect(key)}
            role="listitem"
          >
            <div className="devicePickTop">
              <div className="devicePickTitle">
                {d.machineAlias || d.machineName || "Dispositivo"}
              </div>
              <StatusBadge wsConnected={wsConnected} lastSeenAt={d.lastSeenAt} nowMs={nowMs} />
            </div>
            <div className="devicePickMeta">{d.machineName || "-"}</div>
            <div className="devicePickMeta">
              {d.osName || "-"} · {d.hostname || "-"}
            </div>
          </button>
        );
      })}
    </div>
  );
});

function ProfileMenu({ email, loading, onViewProfile, onLogout }) {
  const [open, setOpen] = useState(false);
  const btnRef = useRef(null);
  const menuRef = useRef(null);

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

    const onKey = (e) => {
      if (e.key === "Escape") setOpen(false);
    };

    document.addEventListener("pointerdown", onDown);
    document.addEventListener("keydown", onKey);
    return () => {
      document.removeEventListener("pointerdown", onDown);
      document.removeEventListener("keydown", onKey);
    };
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
        <span className="profileAvatar" aria-hidden="true">
          <span className="profileAvatarIcon">👤</span>
        </span>
        <span className="profileBtnLabel">Conta</span>
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

// ---- editor de agendamento (sem startAt) ----
const ScheduleEditor = memo(function ScheduleEditor({
  scheduleMode,
  setScheduleMode,
  everyXHours,
  setEveryXHours,
  scheduleWeekDays,
  setScheduleWeekDays,
  dailyTime,
  setDailyTime,
}) {
  const toggleDay = useCallback((dayId) => {
    setScheduleWeekDays((prev) => {
      if (prev.includes(dayId)) return prev.filter((d) => d !== dayId);
      return [...prev, dayId];
    });
  }, [setScheduleWeekDays]);

  return (
    <div className="configRow">
      <label className="tiny">Agendamento</label>

      <div className="segmented" role="radiogroup" aria-label="Tipo de agendamento">
        <button
          type="button"
          className={`btn ${scheduleMode === "hourly" ? "btn--primary" : "btn--soft"}`}
          onClick={() => setScheduleMode("hourly")}
          aria-checked={scheduleMode === "hourly"}
          role="radio"
        >
          A cada X horas
        </button>

        <button
          type="button"
          className={`btn ${scheduleMode === "daily" ? "btn--primary" : "btn--soft"}`}
          onClick={() => setScheduleMode("daily")}
          aria-checked={scheduleMode === "daily"}
          role="radio"
        >
          Dias + hora
        </button>
      </div>

      {scheduleMode === "hourly" ? (
        <div className="configRow" style={{ marginTop: 8 }}>
          <label className="tiny">Executar a cada (horas)</label>
          <input
            className="fieldInput"
            type="number"
            min={1}
            max={24}
            value={everyXHours}
            onChange={(e) => setEveryXHours(clampInt(e.target.value, 1, 24, 6))}
          />
        </div>
      ) : (
        <div className="configRow" style={{ marginTop: 8 }}>
          <label className="tiny">Dias da semana</label>
          <div className="weekGrid" role="group" aria-label="Dias da semana">
            {WEEK_DAYS.map((d) => {
              const active = scheduleWeekDays.includes(d.id);
              return (
                <button
                  key={d.id}
                  type="button"
                  className={`weekBtn${active ? " weekBtn--active" : ""}`}
                  onClick={() => toggleDay(d.id)}
                  aria-pressed={active}
                >
                  {d.label}
                </button>
              );
            })}
          </div>

          <label className="tiny" style={{ marginTop: 8 }}>Hora</label>
          <input
            className="fieldInput"
            type="time"
            value={dailyTime}
            onChange={(e) => setDailyTime(e.target.value)}
          />
        </div>
      )}
    </div>
  );
});

// ---- restore tab ----
const RestoreTab = memo(function RestoreTab({
  selectedDevice,
  restoreCandidates,
  historyItems,
  restoreLoading,
  restoreError,
  onRefreshBackups,
  restoreBackupId,
  setRestoreBackupId,
  restoreTargetMode,
  setRestoreTargetMode,
  restoreTargetPath,
  setRestoreTargetPath,
  onListAgentFolders,
  onRunRestore,
  selectedDeviceIsWindows,
  selectedUserName,
  selectedDeviceKey,
  panelNote,
  setPanelNote,
  setRestoreLoading,
}) {
  const placeholderCustom = selectedDeviceIsWindows
    ? `Ex: C:\\Users\\${selectedUserName}\\Documentos`
    : `Ex: /home/${selectedUserName}/documentos`;
  const hasSelectedCandidate = restoreCandidates.some((b) => b.id === restoreBackupId);
  const [restoreFolderItems, setRestoreFolderItems] = useState(EMPTY_ARR);
  const [restoreFoldersLoading, setRestoreFoldersLoading] = useState(false);
  const [restoreBrowserCurrent, setRestoreBrowserCurrent] = useState("");
  const [restoreBrowserParent, setRestoreBrowserParent] = useState("");
  const [restoreBrowserError, setRestoreBrowserError] = useState("");

  useEffect(() => {
    if (restoreTargetMode !== "custom") {
      setRestoreFolderItems(EMPTY_ARR);
      setRestoreBrowserCurrent("");
      setRestoreBrowserParent("");
      setRestoreBrowserError("");
    }
  }, [restoreTargetMode]);

  const openRestoreFolder = useCallback(async (path) => {
    if (typeof onListAgentFolders !== "function") {
      setRestoreBrowserError("Listagem de pastas não configurada no App.");
      return;
    }
    setRestoreFoldersLoading(true);
    setRestoreBrowserError("");

    const result = await onListAgentFolders({ path });
    setRestoreFoldersLoading(false);

    if (!result?.ok) {
      const msg = result?.error || "Falha ao listar pastas.";
      setRestoreBrowserError(msg);
      return;
    }

    const data = result?.data || EMPTY_OBJ;
    const items = Array.isArray(data?.items) ? data.items : EMPTY_ARR;
    setRestoreFolderItems(items);
    setRestoreBrowserCurrent(String(data?.current || path || ""));
    setRestoreBrowserParent(String(data?.parent || ""));
    if (!restoreTargetPath && data?.current) setRestoreTargetPath(String(data.current));
  }, [onListAgentFolders, restoreTargetPath, setRestoreTargetPath]);

  useEffect(() => {
    if (restoreTargetMode !== "custom") return;
    openRestoreFolder(restoreTargetPath || "");
  }, [restoreTargetMode, selectedDeviceKey]); // eslint-disable-line react-hooks/exhaustive-deps

  return (
    <div className="devicePanel">
      <div className="configRow">
        <div className="row row--between row--wrap">
          <label className="tiny">Backup para restaurar</label>
          <button className="btn btn--soft" type="button" onClick={onRefreshBackups} disabled={restoreLoading}>
            {restoreLoading ? "Atualizando..." : "Atualizar backups"}
          </button>
        </div>

        {restoreCandidates.length > 0 ? (
          <select
            className="fieldInput"
            value={restoreCandidates.some((b) => b.id === restoreBackupId) ? restoreBackupId : ""}
            onChange={(e) => setRestoreBackupId(e.target.value)}
          >
            <option value="">Selecione um backup</option>
            {restoreCandidates.map((b) => (
              <option key={b.id} value={b.id}>
                {b.label}
              </option>
            ))}
          </select>
        ) : (
          <div className="mutedBlock" role="note">
            {restoreLoading ? "Carregando backups do agente..." : restoreError || "Nenhum backup concluído disponível para restore no momento."}
            {!restoreLoading && !restoreError && Array.isArray(historyItems) && historyItems.length > 0 ? (
              <div style={{ marginTop: 8 }}>
                Histórico encontrado, mas ainda sem `scanId` concluído para restore:
                <pre className="activityPayload" style={{ marginTop: 8 }}>
{JSON.stringify(
  historyItems.slice(0, 3).map((h) => ({
    id: h?.id,
    status: h?.status,
    scanId: h?.scanId,
    startedAt: h?.startedAt,
    finishedAt: h?.finishedAt,
    message: h?.message || null,
  })),
  null,
  2
)}
                </pre>
              </div>
            ) : null}
          </div>
        )}
      </div>

      <div className="configRow">
        <label className="tiny">Destino do restore</label>
        <div className="segmented" role="radiogroup" aria-label="Destino do restore">
          <button
            type="button"
            className={`btn ${restoreTargetMode === "original" ? "btn--primary" : "btn--soft"}`}
            onClick={() => setRestoreTargetMode("original")}
            role="radio"
            aria-checked={restoreTargetMode === "original"}
          >
            Local original
          </button>
          <button
            type="button"
            className={`btn ${restoreTargetMode === "custom" ? "btn--primary" : "btn--soft"}`}
            onClick={() => setRestoreTargetMode("custom")}
            role="radio"
            aria-checked={restoreTargetMode === "custom"}
          >
            Outro local
          </button>
        </div>

        {restoreTargetMode === "custom" ? (
          <div className="folderBrowser">
            <div className="folderBrowserHead">
              <button
                className="btn btn--soft"
                type="button"
                onClick={() => openRestoreFolder(restoreBrowserParent)}
                disabled={restoreFoldersLoading || !restoreBrowserParent}
                title="Voltar para pasta pai"
              >
                ↑ Voltar
              </button>
              <div className="folderBrowserPath" title={restoreBrowserCurrent || placeholderCustom}>
                {restoreBrowserCurrent || placeholderCustom}
              </div>
            </div>

            <div className="folderBrowserBody" role="listbox" aria-label="Pastas do computador">
              {restoreFoldersLoading ? (
                <div className="folderRow folderRow--info">Carregando pastas...</div>
              ) : restoreBrowserError ? (
                <div className="folderRow folderRow--info">{restoreBrowserError}</div>
              ) : restoreFolderItems.length === 0 ? (
                <div className="folderRow folderRow--info">Nenhuma subpasta encontrada.</div>
              ) : (
                restoreFolderItems.map((it) => {
                  const selected = restoreTargetPath === it.path;
                  return (
                    <button
                      key={it.path}
                      type="button"
                      className={`folderRow${selected ? " folderRow--selected" : ""}`}
                      onClick={() => setRestoreTargetPath(it.path)}
                      onDoubleClick={() => {
                        setRestoreTargetPath(it.path);
                        openRestoreFolder(it.path);
                      }}
                    >
                      <span className="folderIcon" aria-hidden="true">📁</span>
                      <span className="folderName">{it.name || it.path}</span>
                    </button>
                  );
                })
              )}
            </div>

            <div className="folderBrowserHint">
              Clique para selecionar · Duplo clique para abrir pasta
            </div>
          </div>
        ) : null}
      </div>

      <div className="devicePanelActions">
        <button
          className="btn btn--primary"
          type="button"
          onClick={async () => {
            if (!selectedDevice || !hasSelectedCandidate) return;
            if (typeof onRunRestore !== "function") {
              setPanelNote?.("Integração de restore não configurada no App.");
              return;
            }
            setRestoreLoading?.(true);
            const result = await onRunRestore({
              backupId: String(restoreBackupId || "").trim(),
              scanId: Number(restoreBackupId),
              targetMode: restoreTargetMode,
              targetPath: restoreTargetMode === "custom" ? restoreTargetPath : "",
              deviceId: selectedDeviceKey,
            });
            setRestoreLoading?.(false);
            if (!result?.ok) {
              setPanelNote?.(result?.error || "Falha no restore.");
              return;
            }
            setPanelNote?.(`Restore concluído. arquivos=${result?.data?.filesRestored ?? "-"} erros=${result?.data?.errors ?? "-"}`);
          }}
          disabled={!selectedDevice || !hasSelectedCandidate}
          title={!hasSelectedCandidate ? "Selecione um backup concluído" : undefined}
        >
          Restaurar
        </button>
      </div>

      {panelNote ? <div className="mutedBlock">{panelNote}</div> : null}
    </div>
  );
});

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
  onRunBackup,
  onRunRestore,
  onListAgentFolders,
  onRefresh,
  onLogout,
  onViewProfile,
}) {
  const [tab, setTab] = useState("inicio");

  const [selectedDeviceKey, setSelectedDeviceKey] = useState("");
  const [panelTab, setPanelTab] = useState("backup"); // "backup" | "restore" | "config"

  // config
  const [configOrigin, setConfigOrigin] = useState("");
  const [configDestination, setConfigDestination] = useState("");
  const [originMode, setOriginMode] = useState("preset"); // preset | custom
  const [destMode, setDestMode] = useState("preset"); // preset | custom
  const [scheduleMode, setScheduleMode] = useState("hourly"); // hourly | daily
  const [everyXHours, setEveryXHours] = useState(6);
  const [scheduleWeekDays, setScheduleWeekDays] = useState(["mon", "wed", "fri"]);
  const [dailyTime, setDailyTime] = useState("02:00");
  const [configEncryption, setConfigEncryption] = useState(false);

  // restore
  const [restoreBackupId, setRestoreBackupId] = useState("");
  const [restoreTargetMode, setRestoreTargetMode] = useState("original"); // original | custom
  const [restoreTargetPath, setRestoreTargetPath] = useState("");
  const [historyItems, setHistoryItems] = useState(EMPTY_ARR);
  const [restoreLoading, setRestoreLoading] = useState(false);
  const [restoreError, setRestoreError] = useState("");
  const [originFolderItems, setOriginFolderItems] = useState(EMPTY_ARR);
  const [destFolderItems, setDestFolderItems] = useState(EMPTY_ARR);
  const [originFoldersLoading, setOriginFoldersLoading] = useState(false);
  const [destFoldersLoading, setDestFoldersLoading] = useState(false);
  const [originBrowserCurrent, setOriginBrowserCurrent] = useState("");
  const [originBrowserParent, setOriginBrowserParent] = useState("");
  const [destBrowserCurrent, setDestBrowserCurrent] = useState("");
  const [destBrowserParent, setDestBrowserParent] = useState("");

  const [panelNote, setPanelNote] = useState("");
  const [scanProgress, setScanProgress] = useState(null);
  const [scanNowMs, setScanNowMs] = useState(Date.now());

  const nowMs = Date.now();

  useEffect(() => {
    if (!scanProgress || scanProgress.state !== "running") return undefined;
    const id = setInterval(() => setScanNowMs(Date.now()), 1000);
    return () => clearInterval(id);
  }, [scanProgress]);

  // ws metrics -> primitives (menos rerender por ref de objeto)
  const created = clampCount(wsMetrics?.created);
  const running = clampCount(wsMetrics?.running);
  const success = clampCount(wsMetrics?.success);
  const failed = clampCount(wsMetrics?.failed);
  const cancelled = clampCount(wsMetrics?.cancelled);

  const hasDevices = devices.length > 0;

  const onlineDevices = useMemo(() => {
    let online = 0;
    for (const d of devices) {
      const m = minutesAgo(d.lastSeenAt, nowMs);
      if (m != null && m <= 10) online++;
    }
    return online;
  }, [devices, nowMs]);

  const offlineDevices = Math.max(0, devices.length - onlineDevices);

  // mantém selectedDeviceKey válido
  useEffect(() => {
    if (!selectedDeviceKey) return;
    const exists = devices.some((d) => deviceKeyOf(d) === selectedDeviceKey);
    if (!exists) setSelectedDeviceKey("");
  }, [devices, selectedDeviceKey]);

  const selectedDevice = useMemo(() => {
    if (!selectedDeviceKey) return null;
    return devices.find((d) => deviceKeyOf(d) === selectedDeviceKey) || null;
  }, [devices, selectedDeviceKey]);

  const selectedDeviceIsWindows = /win/i.test(selectedDevice?.osName || "");
  const selectedUserName =
    (selectedDevice?.machineName || "").split("@")[0].trim() || "usuario";

  const originOptions = useMemo(
    () => buildOriginOptions(selectedDeviceIsWindows, selectedUserName),
    [selectedDeviceIsWindows, selectedUserName]
  );

  const destinationOptions = useMemo(
    () => buildDestinationOptions(selectedDeviceIsWindows),
    [selectedDeviceIsWindows]
  );

  // reset “padrão” somente quando troca de device (não quando muda array memo)
  useEffect(() => {
    if (!selectedDevice) return;

    setPanelTab("backup");

    let saved = null;
    try {
      const raw = localStorage.getItem(getConfigStorageKey(selectedDeviceKey));
      saved = raw ? JSON.parse(raw) : null;
    } catch {
      saved = null;
    }

    const defaultOrigin = originOptions[0]?.value || "";
    const defaultDest = destinationOptions[0]?.value || "";

    setOriginMode(saved?.originMode === "custom" ? "custom" : "preset");
    setDestMode(saved?.destMode === "custom" ? "custom" : "preset");
    setConfigOrigin(saved?.configOrigin || defaultOrigin);
    setConfigDestination(normalizeSavedDestination(saved?.configDestination, selectedDeviceIsWindows) || defaultDest);
    setScheduleMode(saved?.scheduleMode === "daily" ? "daily" : "hourly");
    setEveryXHours(clampInt(saved?.everyXHours, 1, 24, 6));
    setScheduleWeekDays(
      Array.isArray(saved?.scheduleWeekDays) && saved.scheduleWeekDays.length > 0
        ? saved.scheduleWeekDays
        : ["mon", "wed", "fri"]
    );
    setDailyTime(typeof saved?.dailyTime === "string" && saved.dailyTime ? saved.dailyTime : "02:00");
    setConfigEncryption(Boolean(saved?.configEncryption));

    setRestoreBackupId("");
    setRestoreTargetMode("original");
    setRestoreTargetPath("");
    setOriginFolderItems(EMPTY_ARR);
    setDestFolderItems(EMPTY_ARR);
    setOriginBrowserCurrent("");
    setOriginBrowserParent("");
    setDestBrowserCurrent("");
    setDestBrowserParent("");
    setPanelNote("");
  }, [selectedDeviceKey, selectedDevice, originOptions, destinationOptions]);

  const onRequestDelete = useCallback(
    (d) => {
      if (!d?.id) return;
      const name = d.machineName || d.machineAlias || "dispositivo";
      const ok = window.confirm(`Excluir/desvincular?\n\n${name}`);
      if (ok) onDeleteDevice?.(d.id);
    },
    [onDeleteDevice]
  );

  const { kpis, bars, maxBar } = useMemo(() => {
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
  }, [created, running, success, failed, cancelled, wsEvents.length]);

  const recentEvents = useMemo(() => wsEvents.slice(0, 20), [wsEvents]);

  const estimatedScanMs = useMemo(() => {
    const done = historyItems
      .filter((h) => String(h?.status || "").toUpperCase() === "SUCCESS")
      .map((h) => {
        const s = Date.parse(h?.startedAt || "");
        const f = Date.parse(h?.finishedAt || "");
        if (!Number.isFinite(s) || !Number.isFinite(f) || f <= s) return null;
        return f - s;
      })
      .filter((v) => Number.isFinite(v) && v > 0)
      .slice(0, 8);
    if (done.length === 0) return null;
    return done.reduce((acc, v) => acc + v, 0) / done.length;
  }, [historyItems]);

  const scanProgressUi = useMemo(() => {
    if (!scanProgress) return null;

    const state = String(scanProgress.state || "").toLowerCase();
    const startedRef = scanProgress.startedAt || scanProgress.createdAt;
    const startedMs = Date.parse(startedRef || "");
    const elapsedMs = Number.isFinite(startedMs) ? Math.max(0, scanNowMs - startedMs) : 0;

    let percent = null;
    let etaLabel = null;
    if (state === "success") {
      percent = 100;
      etaLabel = "concluído";
    } else if (state === "failed" || state === "cancelled") {
      percent = 100;
      etaLabel = state === "failed" ? "falhou" : "cancelado";
    } else if (state === "running") {
      if (estimatedScanMs && estimatedScanMs > 0) {
        percent = Math.max(4, Math.min(95, Math.round((elapsedMs / estimatedScanMs) * 100)));
        etaLabel = `ETA ${msLabel(Math.max(0, estimatedScanMs - elapsedMs))}`;
      } else {
        percent = Math.max(4, Math.min(90, Math.round((scanProgress.attempt || 1) * 3)));
        etaLabel = "estimando...";
      }
    } else {
      percent = 2;
      etaLabel = "iniciando...";
    }

    const statusLabel =
      state === "success" ? "Concluído" :
      state === "failed" ? "Falhou" :
      state === "cancelled" ? "Cancelado" :
      state === "running" ? "Em execução" : "Na fila";

    return {
      statusLabel,
      elapsedLabel: durationLabel(startedRef, scanNowMs),
      percent,
      etaLabel,
      message: scanProgress.message || null,
      state,
    };
  }, [scanProgress, scanNowMs, estimatedScanMs]);

  const loadBackupHistory = useCallback(async () => {
    setRestoreLoading(true);
    setRestoreError("");
    try {
      const res = await agentRequest("/history?limit=50", { method: "GET" });
      if (!res.ok) {
        setHistoryItems(EMPTY_ARR);
        setRestoreError(res.data?.message || "Falha ao carregar histórico de backups.");
        return;
      }
      const items = Array.isArray(res.data?.items) ? res.data.items : EMPTY_ARR;
      setHistoryItems(items);
    } catch {
      setHistoryItems(EMPTY_ARR);
      setRestoreError("Falha ao conectar na API do agente para listar backups.");
    } finally {
      setRestoreLoading(false);
    }
  }, []);

  const pollScanJobUntilDone = useCallback(async (jobId, onTick) => {
    const id = String(jobId || "").trim();
    if (!id) return { ok: false, error: "jobId inválido" };

    const maxAttempts = 900; // ~30 minutos (900 * 2s)
    for (let i = 0; i < maxAttempts; i += 1) {
      const res = await agentRequest(`/scan/${encodeURIComponent(id)}`, { method: "GET" });
      if (!res.ok) {
        return {
          ok: false,
          error: res?.data?.message || `Falha ao consultar status do backup (HTTP ${res.status || "?"})`,
        };
      }

      const job = res?.data?.job || EMPTY_OBJ;
      const state = String(job?.state || "").toLowerCase();
      onTick?.({
        jobId: id,
        state,
        attempt: i + 1,
        polledAt: new Date().toISOString(),
        createdAt: job?.createdAt || null,
        startedAt: job?.startedAt || null,
        finishedAt: job?.finishedAt || null,
        lastHeartbeatAt: job?.lastHeartbeatAt || null,
        message: job?.message || null,
      });
      if (state === "success" || state === "failed" || state === "cancelled") {
        return { ok: true, state, job };
      }

      await new Promise((r) => setTimeout(r, 2000));
    }

    return { ok: false, timeout: true, error: "Timeout aguardando conclusão do backup." };
  }, []);

  const loadAgentFolders = useCallback(
    async (kind, pathOverride = null) => {
      if (typeof onListAgentFolders !== "function") {
        setPanelNote("Listagem de pastas não configurada no App.");
        return null;
      }
      const isOrigin = kind === "origin";
      if (isOrigin) setOriginFoldersLoading(true);
      else setDestFoldersLoading(true);

      const path = pathOverride ?? (isOrigin ? configOrigin : configDestination);
      const result = await onListAgentFolders({ path });
      if (isOrigin) setOriginFoldersLoading(false);
      else setDestFoldersLoading(false);

      if (!result?.ok) {
        setPanelNote(result?.error || "Falha ao listar pastas.");
        return null;
      }

      const data = result?.data || EMPTY_OBJ;
      const items = Array.isArray(data?.items) ? data.items : EMPTY_ARR;
      const current = String(data?.current || path || "");
      const parent = String(data?.parent || "");

      if (isOrigin) {
        setOriginFolderItems(items);
        setOriginBrowserCurrent(current);
        setOriginBrowserParent(parent);
      } else {
        setDestFolderItems(items);
        setDestBrowserCurrent(current);
        setDestBrowserParent(parent);
      }
      return { items, current, parent };
    },
    [onListAgentFolders, configOrigin, configDestination]
  );

  useEffect(() => {
    if (panelTab !== "config" || !selectedDevice || originMode !== "custom") return;
    loadAgentFolders("origin", configOrigin || "");
  }, [panelTab, selectedDevice, originMode, selectedDeviceKey]); // eslint-disable-line react-hooks/exhaustive-deps

  useEffect(() => {
    if (panelTab !== "config" || !selectedDevice || destMode !== "custom") return;
    loadAgentFolders("dest", configDestination || "");
  }, [panelTab, selectedDevice, destMode, selectedDeviceKey]); // eslint-disable-line react-hooks/exhaustive-deps

  useEffect(() => {
    if (!session) return;
    loadBackupHistory();
  }, [session, loadBackupHistory]);

  useEffect(() => {
    if (panelTab !== "restore") return;
    if (!selectedDevice) return;
    loadBackupHistory();
  }, [panelTab, selectedDevice, loadBackupHistory]);

  // tentativa “safe” de extrair backups do wsEvents (se você mandar isso no payload)
  const restoreCandidates = useMemo(() => {
    const out = [];

    // histórico real do agente (/history)
    for (const h of historyItems) {
      const id = h?.scanId;
      if (!id) continue;
      const finished = h?.finishedAt || h?.startedAt || "";
      const label = `${String(id)} · ${h?.status || "status?"} · ${finished ? formatLastSeen(finished) : "-"}`;
      out.push({ id: String(id), label });
    }

    if (!selectedDevice) {
      const uniqNoDevice = new Map();
      for (const b of out) if (!uniqNoDevice.has(b.id)) uniqNoDevice.set(b.id, b);
      return Array.from(uniqNoDevice.values()).slice(0, 30);
    }

    const key = selectedDeviceKey;
    for (const ev of wsEvents) {
      // heurística: ev.payload.backupId ou ev.payload.backup_id
      const p = ev?.payload || EMPTY_OBJ;
      const backupId = p.backupId || p.backup_id || p.id;
      const deviceHint = p.deviceKey || p.device_id || p.machineAlias || p.hostname;

      if (!backupId) continue;
      if (deviceHint && String(deviceHint) !== key && String(deviceHint) !== selectedDevice.machineAlias && String(deviceHint) !== selectedDevice.hostname) {
        continue;
      }

      // rótulo humano
      const label = `${String(backupId)} · ${ev.type || "backup"} · ${formatLastSeen(ev.ts)}`;
      out.push({ id: String(backupId), label });
    }

    // remove duplicados por id
    const uniq = new Map();
    for (const b of out) if (!uniq.has(b.id)) uniq.set(b.id, b);
    return Array.from(uniq.values()).slice(0, 30);
  }, [historyItems, wsEvents, selectedDeviceKey, selectedDevice, selectedDevice?.machineAlias, selectedDevice?.hostname]);

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

  const handleRunBackupNow = useCallback(async () => {
    if (!selectedDevice) return;
    if (typeof onRunBackup !== "function") {
      setPanelNote("Integração de backup não configurada no App.");
      return;
    }

    const root = String(configOrigin || "").trim();
    const dest = normalizeSavedDestination(configDestination, selectedDeviceIsWindows);

    if (!root || !dest) {
      setPanelTab("config");
      setPanelNote("Configure origem e destino antes de executar backup.");
      return;
    }

    const result = await onRunBackup({
      device: selectedDevice,
      root,
      dest,
      password: configEncryption ? undefined : undefined,
    });

    if (!result?.ok) {
      setPanelNote(result?.error || "Falha ao iniciar backup.");
      return;
    }

    setPanelNote("Backup iniciado.");
    setScanProgress({
      jobId,
      state: "created",
      attempt: 0,
      polledAt: new Date().toISOString(),
      createdAt: null,
      startedAt: null,
      finishedAt: null,
      lastHeartbeatAt: null,
      message: null,
    });
    if (!result?.data?.jobId) return;

    try {
      const polled = await pollScanJobUntilDone(result.data.jobId, (tick) => {
        setScanProgress(tick);
      });
      if (!polled.ok) {
        setPanelNote((prev) => `${prev}\n${polled.error || "Não foi possível acompanhar o status do backup."}`);
        setScanProgress((prev) => ({
          ...(prev || {}),
          state: "failed",
          message: polled.error || "Não foi possível acompanhar o status do backup.",
          finishedAt: new Date().toISOString(),
        }));
        return;
      }

      if (polled.state === "success") {
        setPanelNote("Backup concluído com sucesso.");
      } else if (polled.state === "failed") {
        setPanelNote("Backup falhou.");
      } else if (polled.state === "cancelled") {
        setPanelNote("Backup cancelado.");
      }
    } catch {
      setPanelNote((prev) => `${prev}\nFalha ao acompanhar status do backup.`);
      setScanProgress((prev) => ({
        ...(prev || {}),
        state: "failed",
        message: "Falha ao acompanhar status do backup.",
        finishedAt: new Date().toISOString(),
      }));
    } finally {
      loadBackupHistory();
    }
  }, [
    selectedDevice,
    onRunBackup,
    configOrigin,
    configDestination,
    configEncryption,
    loadBackupHistory,
    pollScanJobUntilDone,
  ]);

  const saveDeviceConfig = useCallback(() => {
    if (!selectedDevice) return;

    const name = selectedDevice.machineAlias || selectedDevice.machineName || "dispositivo";

    const scheduleText =
      scheduleMode === "hourly"
        ? `a cada ${clampInt(everyXHours, 1, 24, 6)} hora(s)`
        : `dias ${scheduleWeekDays.join(", ") || "-"} às ${dailyTime || "--:--"}`;

    try {
      localStorage.setItem(
        getConfigStorageKey(selectedDeviceKey),
        JSON.stringify({
          originMode,
          destMode,
          configOrigin,
          configDestination,
          scheduleMode,
          everyXHours: clampInt(everyXHours, 1, 24, 6),
          scheduleWeekDays,
          dailyTime,
          configEncryption,
        })
      );
    } catch {}

    setPanelNote(
      `Configuração salva para ${name}. Origem: ${configOrigin || "-"} · Destino: ${configDestination || "-"} · Agendamento: ${scheduleText} · Criptografia: ${configEncryption ? "sim" : "não"}`
    );
  }, [
    selectedDevice,
    selectedDeviceKey,
    originMode,
    destMode,
    scheduleMode,
    everyXHours,
    scheduleWeekDays,
    dailyTime,
    configOrigin,
    configDestination,
    configEncryption,
  ]);

  const onSelectDevice = useCallback((key) => {
    setSelectedDeviceKey((prev) => (prev === key ? "" : key));
  }, []);

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
            <button
              className="refreshBtn"
              type="button"
              onClick={onRefresh}
              disabled={loading}
              title="Atualizar"
              aria-label="Atualizar"
            >
              ↻
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
          <section className="section dashboardGrid">
            <div className="dashboardMain">
              <Surface title="Visão geral">
                <div className="miniGrid miniGrid--top">
                  <StatCard label="Dispositivos" value={devices.length} />
                  <StatCard label="Online (estimado)" value={onlineDevices} tone="blue" />
                  <StatCard label="Offline (estimado)" value={offlineDevices} tone="purple" />
                  <StatCard label="Frames" value={wsEvents.length} />
                </div>
              </Surface>
            </div>

            <aside className="dashboardSide">
              <Surface title="Dispositivos" chip={`${devices.length} total`}>
                <DevicePicker
                  devices={devices}
                  selectedKey={selectedDeviceKey}
                  onSelect={onSelectDevice}
                  wsConnected={wsConnected}
                  nowMs={nowMs}
                />
              </Surface>

              {selectedDevice ? (
                <Surface
                  title={`Painel do dispositivo: ${selectedDevice.machineAlias || selectedDevice.machineName || "-"}`}
                  chip={formatLastSeen(selectedDevice.lastSeenAt)}
                >
                  {/* abas internas do painel (remove agendamento) */}
                  <div className="devicePanelActions">
                    <button
                      className={`btn ${panelTab === "backup" ? "btn--primary" : "btn--soft"}`}
                      type="button"
                      onClick={() => setPanelTab("backup")}
                    >
                      Backup
                    </button>
                    <button
                      className={`btn ${panelTab === "restore" ? "btn--primary" : "btn--soft"}`}
                      type="button"
                      onClick={() => setPanelTab("restore")}
                    >
                      Restore
                    </button>
                    <button
                      className={`btn ${panelTab === "config" ? "btn--primary" : "btn--soft"}`}
                      type="button"
                      onClick={() => setPanelTab("config")}
                    >
                      Configuração
                    </button>
                  </div>

                  {/* BACKUP TAB */}
                  {panelTab === "backup" ? (
                    <div className="devicePanel">
                      <div className="mutedBlock" role="note">
                        Backup usa a configuração atual do agente.
                      </div>

                      <div className="devicePanelActions">
                        <button className="btn btn--primary" type="button" onClick={handleRunBackupNow}>
                          Executar backup agora
                        </button>
                      </div>

                      {scanProgressUi ? (
                        <div className="mutedBlock" role="status" style={{ marginTop: 10 }}>
                          <div><strong>Progresso do backup</strong></div>
                          <div>{scanProgressUi.statusLabel} · {scanProgressUi.percent}% · {scanProgressUi.etaLabel}</div>
                          <div>decorrido: {scanProgressUi.elapsedLabel}</div>
                          <div style={{ marginTop: 8, height: 8, borderRadius: 999, background: "rgba(0,0,0,.08)", overflow: "hidden" }}>
                            <div
                              style={{
                                height: "100%",
                                width: `${scanProgressUi.percent}%`,
                                background:
                                  scanProgressUi.state === "failed"
                                    ? "#d64545"
                                    : scanProgressUi.state === "cancelled"
                                      ? "#8a8a8a"
                                      : "#2f7cf6",
                                opacity: scanProgressUi.state === "running" ? 0.9 : 1,
                              }}
                            />
                          </div>
                          {scanProgressUi.message ? <div style={{ marginTop: 8 }}>mensagem: {scanProgressUi.message}</div> : null}
                        </div>
                      ) : null}

                      {panelNote ? <div className="mutedBlock">{panelNote}</div> : null}
                    </div>
                  ) : null}

                  {/* RESTORE TAB */}
                  {panelTab === "restore" ? (
                    <RestoreTab
                      selectedDevice={selectedDevice}
                      restoreCandidates={restoreCandidates}
                      historyItems={historyItems}
                      restoreLoading={restoreLoading}
                      restoreError={restoreError}
                      onRefreshBackups={loadBackupHistory}
                      restoreBackupId={restoreBackupId}
                      setRestoreBackupId={setRestoreBackupId}
                      restoreTargetMode={restoreTargetMode}
                      setRestoreTargetMode={setRestoreTargetMode}
                      restoreTargetPath={restoreTargetPath}
                      setRestoreTargetPath={setRestoreTargetPath}
                      onListAgentFolders={onListAgentFolders}
                      onRunRestore={onRunRestore}
                      selectedDeviceIsWindows={selectedDeviceIsWindows}
                      selectedUserName={selectedUserName}
                      selectedDeviceKey={selectedDeviceKey}
                      panelNote={panelNote}
                      setPanelNote={setPanelNote}
                      setRestoreLoading={setRestoreLoading}
                    />
                  ) : null}

                  {/* CONFIG TAB */}
                  {panelTab === "config" ? (
                    <div className="devicePanel">
                      <div className="deviceConfigHeader">
                        <div className="h3">⚙ Configuração</div>
                      </div>

                      {/* ORIGEM */}
                      <div className="configRow">
                        <label className="tiny">Origem</label>
                        <select
                          className="fieldInput"
                          value={originMode === "preset" ? configOrigin : "__custom__"}
                          onChange={(e) => {
                            const val = e.target.value;
                            if (val === "__custom__") {
                              setOriginMode("custom");
                            } else {
                              setOriginMode("preset");
                              setConfigOrigin(val);
                            }
                          }}
                        >
                          {originOptions.map((o) => (
                            <option key={o.value} value={o.value}>
                              {o.label}
                            </option>
                          ))}
                          <option value="__custom__">Outro local…</option>
                        </select>

                        {originMode === "custom" ? (
                          <div className="folderBrowser">
                            <div className="folderBrowserHead">
                              <button
                                className="btn btn--soft"
                                type="button"
                                onClick={() => loadAgentFolders("origin", originBrowserParent)}
                                disabled={originFoldersLoading || !originBrowserParent}
                                title="Voltar para pasta pai"
                              >
                                ↑ Voltar
                              </button>
                              <div className="folderBrowserPath" title={originBrowserCurrent || configOrigin || ""}>
                                {originBrowserCurrent || configOrigin || (selectedDeviceIsWindows ? "C:\\" : "/")}
                              </div>
                            </div>
                            <div className="folderBrowserBody" role="listbox" aria-label="Pastas de origem">
                              {originFoldersLoading ? (
                                <div className="folderRow folderRow--info">Carregando pastas...</div>
                              ) : originFolderItems.length === 0 ? (
                                <div className="folderRow folderRow--info">Nenhuma subpasta encontrada.</div>
                              ) : (
                                originFolderItems.map((it) => {
                                  const selected = configOrigin === it.path;
                                  return (
                                    <button
                                      key={it.path}
                                      type="button"
                                      className={`folderRow${selected ? " folderRow--selected" : ""}`}
                                      onClick={() => setConfigOrigin(it.path)}
                                      onDoubleClick={() => {
                                        setConfigOrigin(it.path);
                                        loadAgentFolders("origin", it.path);
                                      }}
                                    >
                                      <span className="folderIcon" aria-hidden="true">📁</span>
                                      <span className="folderName">{it.name || it.path}</span>
                                    </button>
                                  );
                                })
                              )}
                            </div>
                            <div className="folderBrowserHint">Clique para selecionar · Duplo clique para abrir pasta</div>
                          </div>
                        ) : null}
                      </div>

                      {/* DESTINO */}
                      <div className="configRow">
                        <label className="tiny">Destino</label>
                        <select
                          className="fieldInput"
                          value={destMode === "preset" ? configDestination : "__custom__"}
                          onChange={(e) => {
                            const val = e.target.value;
                            if (val === "__custom__") {
                              setDestMode("custom");
                            } else {
                              setDestMode("preset");
                              setConfigDestination(val);
                            }
                          }}
                        >
                          {destinationOptions.map((o) => (
                            <option key={o.value} value={o.value}>
                              {o.label}
                            </option>
                          ))}
                          <option value="__custom__">Outro local…</option>
                        </select>

                        {destMode === "custom" ? (
                          <div className="folderBrowser">
                            <div className="folderBrowserHead">
                              <button
                                className="btn btn--soft"
                                type="button"
                                onClick={() => loadAgentFolders("dest", destBrowserParent)}
                                disabled={destFoldersLoading || !destBrowserParent}
                                title="Voltar para pasta pai"
                              >
                                ↑ Voltar
                              </button>
                              <div className="folderBrowserPath" title={destBrowserCurrent || configDestination || ""}>
                                {destBrowserCurrent || configDestination || (selectedDeviceIsWindows ? "C:\\" : "/")}
                              </div>
                            </div>
                            <div className="folderBrowserBody" role="listbox" aria-label="Pastas de destino">
                              {destFoldersLoading ? (
                                <div className="folderRow folderRow--info">Carregando pastas...</div>
                              ) : destFolderItems.length === 0 ? (
                                <div className="folderRow folderRow--info">Nenhuma subpasta encontrada.</div>
                              ) : (
                                destFolderItems.map((it) => {
                                  const selected = configDestination === it.path;
                                  return (
                                    <button
                                      key={it.path}
                                      type="button"
                                      className={`folderRow${selected ? " folderRow--selected" : ""}`}
                                      onClick={() => setConfigDestination(it.path)}
                                      onDoubleClick={() => {
                                        setConfigDestination(it.path);
                                        loadAgentFolders("dest", it.path);
                                      }}
                                    >
                                      <span className="folderIcon" aria-hidden="true">📁</span>
                                      <span className="folderName">{it.name || it.path}</span>
                                    </button>
                                  );
                                })
                              )}
                            </div>
                            <div className="folderBrowserHint">Clique para selecionar · Duplo clique para abrir pasta</div>
                          </div>
                        ) : null}
                      </div>

                      {/* AGENDAMENTO (sem startAt) */}
                      <ScheduleEditor
                        scheduleMode={scheduleMode}
                        setScheduleMode={setScheduleMode}
                        everyXHours={everyXHours}
                        setEveryXHours={setEveryXHours}
                        scheduleWeekDays={scheduleWeekDays}
                        setScheduleWeekDays={setScheduleWeekDays}
                        dailyTime={dailyTime}
                        setDailyTime={setDailyTime}
                      />

                      <label className="deviceToggle">
                        <input
                          type="checkbox"
                          checked={configEncryption}
                          onChange={(e) => setConfigEncryption(e.target.checked)}
                        />
                        <span>Criptografia</span>
                      </label>

                      <div className="devicePanelActions">
                        <button className="btn btn--primary" type="button" onClick={saveDeviceConfig}>
                          Salvar configuração
                        </button>

                        <button
                          className="btn btn--dangerOutline"
                          type="button"
                          disabled={loading || !selectedDevice.id}
                          onClick={() => onRequestDelete(selectedDevice)}
                        >
                          Excluir dispositivo
                        </button>
                      </div>

                      {panelNote ? <div className="mutedBlock">{panelNote}</div> : null}
                    </div>
                  ) : null}
                </Surface>
              ) : null}
            </aside>
          </section>
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
                {pairingState?.paired
                  ? "Este agente local ja esta vinculado no painel."
                  : "Cole o código exibido no agente."}
              </div>

              {!pairingState?.paired ? (
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
              ) : null}
            </Surface>
          </section>
        ) : null}

        {tab === "atividades" ? (
          <section className="section activitiesGrid">
            <Surface title="Alertas e Métricas" chip={wsConnected ? "Agente Online" : "Agente Offline"}>
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
                </>
              )}
            </Surface>

            <Surface title="Lista de Atividades" chip={`${recentEvents.length} itens`}>
              {recentEvents.length === 0 ? (
                <div className="mutedBlock" role="note">
                  Aguardando frames no WebSocket...
                </div>
              ) : (
                <div className="activityDropdownList">
                  {recentEvents.map((ev, i) => (
                    <details key={`${ev.type}-${ev.ts}-${i}`} className="activityItem">
                      <summary className="activitySummary">
                        <span className="activityType">{ev.type}</span>
                        <span className="activityTime">{formatLastSeen(ev.ts)}</span>
                      </summary>
                      <div className="activityBody">
                        <div className="activityMeta">Timestamp: {ev.ts || "-"}</div>
                        <pre className="activityPayload">{JSON.stringify(ev.payload || {}, null, 2)}</pre>
                      </div>
                    </details>
                  ))}
                </div>
              )}
            </Surface>
          </section>
        ) : null}
      </main>
    </div>
  );
}
