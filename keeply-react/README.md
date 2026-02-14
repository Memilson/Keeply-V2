# Keeply React Frontend

Painel React para consumir a API Keeply (`/api/keeply/*`).

## Requisitos

- Node.js 20+
- npm 10+

## Rodar

```bash
cd keeply-react
npm install
npm run dev
```

Abre em `http://localhost:5173`.

## API alvo

O `vite.config.js` ja faz proxy de `/api` para `http://localhost:8082`.

Entao mantenha o backend Keeply rodando em `8082`:

```bash
cd ..
mvn exec:java "-Dexec.args=api --port 8082"
```
