import { API_BASE } from "../lib/api";

const NAV = [
  { href: "#inicio", label: "Inicio", active: true },
  { href: "#como", label: "Como funciona" },
  { href: "#planos", label: "Planos" },
  { href: "#ajuda", label: "Ajuda ▾" },
];

const STATS = [
  { label: "BACKUPS FEITOS ESTA SEMANA", value: "240" },
  { label: "VERSOES POR ARQUIVO", value: "ate 30 dias" },
  { label: "RESTAURACAO MEDIA", value: "2 min" },
];

const MINI = [
  { label: "Backups realizados", value: "247", hint: "Desde que voce instalou" },
  { label: "Fotos e videos", value: "824 GB", hint: "Memorias protegidas", tone: "blue" },
  { label: "Taxa de sucesso", value: "99,8%", hint: "Tarefas concluidas", tone: "purple" },
];

const INFO = [
  {
    id: "como",
    title: "Como funciona",
    text:
      'O agente roda em segundo plano, detecta mudancas e cria versoes. Se algo falhar, voce recebe sinal claro - sem "painel caotico".',
    col: true,
  },
  {
    id: "como-2",
    title: "Foco em simplicidade",
    text:
      'Interface limpa, numeros que importam e acoes curtas. Estilo cloud sem virar "produto barulhento".',
    col: true,
  },
  {
    id: "planos",
    title: "Planos",
    text:
      'Aqui entra sua grade de planos depois (Free / Plus / Pro). A landing ja esta com o "shape" certo.',
  },
  {
    id: "ajuda",
    title: "Ajuda",
    text: "Link para docs/FAQ, ou suporte. O objetivo e manter consistencia visual.",
  },
];

export default function LandingPage({ loading, onOpenLogin, onOpenRegister }) {
  const bars = Array.from({ length: 14 }, (_, i) => 18 + ((i * 19) % 62));

  return (
    <div className="shell">
      <header className="topbar">
        <div className="brand">
          <div className="brandName">
            <strong>Keeply</strong> <span className="brandTag">PROTEJA O QUE IMPORTA</span>
          </div>
        </div>

        <nav className="nav">
          {NAV.map((n) => (
            <a
              key={n.href}
              className={`navLink${n.active ? " navLink--active" : ""}`}
              href={n.href}
            >
              {n.label}
            </a>
          ))}
        </nav>

        <div className="topActions">
          <button className="linkBtn" onClick={onOpenLogin} disabled={loading}>
            Entrar
          </button>
          <button className="btn btn--primary" onClick={onOpenRegister} disabled={loading}>
            Criar minha conta
          </button>
        </div>
      </header>

      <main className="page" id="inicio">
        <section className="hero">
          <div className="heroInner">
            <div className="pill">
              <span className="pillBar" />
              <span>Keeply Pessoal - Backup leve e tranquilo</span>
            </div>

            <h1 className="h1">Suas memorias protegidas enquanto voce vive a vida</h1>

            <p className="sub">
              Fotos de familia, trabalhos da faculdade, documentos importantes. O Keeply faz o
              backup em segundo plano e avisa se algo precisar da sua atencao.
            </p>

            <div className="ctaRow">
              <button
                className="btn btn--primary btn--lg"
                onClick={onOpenRegister}
                disabled={loading}
              >
                Comecar backup gratis
              </button>
              <a className="btn btn--soft btn--lg" href="#como">
                Ver como funciona
              </a>
            </div>

            <div className="divider" />

            <div className="stats">
              {STATS.map((s) => (
                <div key={s.label} className="stat">
                  <div className="statLabel">{s.label}</div>
                  <div className="statValue">{s.value}</div>
                </div>
              ))}
            </div>

            <div className="footnote">
              Roda em segundo plano - Windows, Mac e Linux - Sem precisar ser da area de TI
            </div>
          </div>
        </section>

        <section className="section">
          <div className="bigCard">
            <div className="bigCardHeader">
              <div>
                <div className="eyebrow">Seu computador, sempre em dia</div>
                <div className="h2">Resumo do backup</div>
              </div>
              <div className="chip chip--ok">Tudo sincronizado</div>
            </div>

            <div className="miniGrid">
              {MINI.map((c) => (
                <div key={c.label} className="miniCard">
                  <div className="miniLabel">{c.label}</div>
                  <div className={`miniValue${c.tone ? ` miniValue--${c.tone}` : ""}`}>
                    {c.value}
                  </div>
                  <div className="miniHint">{c.hint}</div>
                </div>
              ))}
            </div>

            <div className="activityHead">
              <div className="h3">Atividade dos ultimos 7 dias</div>
              <div className="muted">Sem falhas criticas</div>
            </div>

            <div className="bars">
              {bars.map((h, i) => (
                <div key={i} className="bar" style={{ height: `${h}px` }} />
              ))}
            </div>
          </div>
        </section>

        {/* Como funciona (duas colunas) */}
        <section className="section" id="como">
          <div className="twoCol">
            {INFO.filter((x) => x.col).map((c) => (
              <div key={c.id} className="infoCard">
                <div className="h2">{c.title}</div>
                <p className="mutedBlock">{c.text}</p>
              </div>
            ))}
          </div>
        </section>

        {/* Planos / Ajuda (1 card cada) */}
        {INFO.filter((x) => !x.col).map((c) => (
          <section key={c.id} className="section" id={c.id}>
            <div className="infoCard">
              <div className="h2">{c.title}</div>
              <p className="mutedBlock">{c.text}</p>
            </div>
          </section>
        ))}

        <footer className="footer">
          <div className="footerLeft">
            <div className="footerBrand">
              <div className="dotLogo" aria-hidden="true" />
              <span>Keeply</span>
            </div>
            <div className="footerMeta">Backup local - UI cloud - Carbon-ish</div>
          </div>
          <div className="footerRight">
            <span className="pillSmall">API: {API_BASE}</span>
          </div>
        </footer>
      </main>
    </div>
  );
}
