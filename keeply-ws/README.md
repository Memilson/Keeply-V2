# Keeply WS (Spring)

Microservico local em Spring Boot para o agente Keeply:
- login no auth-service
- persistencia de sessao local em arquivo JSON
- registro/listagem de dispositivos
- consulta de backups no Keeply API local do agente

## Rodar local

```bash
cd keeply-ws
mvn spring-boot:run
```

## Variaveis de ambiente

- `KEEPLY_WS_PORT` (default `8092`)
- `KEEPLY_WS_BIND` (default `0.0.0.0`)
- `KEEPLY_AUTH_BASE_URL` (default `http://localhost:18081/api/auth`)
- `KEEPLY_AGENT_API_BASE_URL` (default `http://localhost:25420/api/keeply`)
- `KEEPLY_WS_SESSION_FILE` (default `${user.dir}/.keeply-ws-session.json`)
- `KEEPLY_WS_CLIENT_VERSION` (default `keeply-ws/1.0.0`)

## Endpoints

Base: `/api/keeply-ws`

- `GET /health`
- `GET /session`
- `POST /login`
- `POST /logout`
- `GET /devices`
- `POST /devices/register`
- `GET /agent/overview`
- `GET /backups`
