import { useEffect, useMemo } from "react";
import { MIN_PASSWORD_LEN } from "../lib/constants";

const MODES = {
  login: {
    title: "Entrar",
    sub: "Use sua conta local para vincular e gerenciar dispositivos.",
    submit: (loading) => (loading ? "Entrando..." : "Entrar"),
    fields: (form) => [
      {
        k: "email",
        label: "Email",
        type: "email",
        value: form.email,
        autoComplete: "email",
        placeholder: "voce@dominio.com",
      },
      {
        k: "password",
        label: "Senha",
        type: "password",
        value: form.password,
        autoComplete: "current-password",
        minLength: MIN_PASSWORD_LEN,
        placeholder: `Minimo ${MIN_PASSWORD_LEN} caracteres`,
      },
    ],
  },
  register: {
    title: "Criar minha conta",
    sub: "Crie um usuario local para administrar suas maquinas.",
    submit: (loading) => (loading ? "Registrando..." : "Criar minha conta"),
    fields: (form) => [
      {
        k: "name",
        label: "Nome",
        type: "text",
        value: form.name,
        autoComplete: "name",
        placeholder: "Seu nome",
      },
      {
        k: "email",
        label: "Email",
        type: "email",
        value: form.email,
        autoComplete: "email",
        placeholder: "voce@dominio.com",
      },
      {
        k: "password",
        label: "Senha",
        type: "password",
        value: form.password,
        autoComplete: "new-password",
        minLength: MIN_PASSWORD_LEN,
        placeholder: `Minimo ${MIN_PASSWORD_LEN} caracteres`,
      },
    ],
  },
};

function Field({ label, ...props }) {
  return (
    <label className="field">
      <span>{label}</span>
      <input {...props} required />
    </label>
  );
}

export default function AuthModal({
  mode, // "login" | "register" | null
  onClose,
  loading,
  error,
  registerForm,
  setRegisterForm,
  loginForm,
  setLoginForm,
  onLogin,
  onRegister,
}) {
  const isOpen = Boolean(mode);
  const cfg = mode ? MODES[mode] : null;

  const { form, setForm, onSubmit } = useMemo(() => {
    if (mode === "login") {
      return { form: loginForm, setForm: setLoginForm, onSubmit: onLogin };
    }
    if (mode === "register") {
      return { form: registerForm, setForm: setRegisterForm, onSubmit: onRegister };
    }
    return { form: null, setForm: null, onSubmit: null };
  }, [mode, loginForm, setLoginForm, onLogin, registerForm, setRegisterForm, onRegister]);

  useEffect(() => {
    if (!isOpen) return;
    const onKeyDown = (e) => e.key === "Escape" && onClose();
    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, [isOpen, onClose]);

  if (!isOpen || !cfg || !form) return null;

  const fields = cfg.fields(form);
  const change = (k) => (e) => setForm((p) => ({ ...p, [k]: e.target.value }));

  return (
    <div className="modalOverlay" role="dialog" aria-modal="true">
      <div className="modal">
        <div className="modalHeader">
          <div>
            <div className="modalTitle">{cfg.title}</div>
            <div className="modalSub">{cfg.sub}</div>
          </div>

          <button className="iconBtn" onClick={onClose} aria-label="Fechar">
            ✕
          </button>
        </div>

        {error ? <div className="alert alert--danger">{error}</div> : null}

        <form className="form" onSubmit={onSubmit}>
          {fields.map((f) => (
            <Field
              key={f.k}
              label={f.label}
              type={f.type}
              value={f.value || ""}
              onChange={change(f.k)}
              placeholder={f.placeholder}
              autoComplete={f.autoComplete}
              minLength={f.minLength}
            />
          ))}

          <div className="formActions">
            <button className="btn btn--primary" type="submit" disabled={loading}>
              {cfg.submit(loading)}
            </button>
            <button className="btn btn--ghost" type="button" onClick={onClose} disabled={loading}>
              Cancelar
            </button>
          </div>
        </form>
      </div>

      <button className="modalBackdrop" onClick={onClose} aria-label="Fechar modal" />
    </div>
  );
}
