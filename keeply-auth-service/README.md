# Keeply Auth Service

Microservico Spring Boot para autenticacao do Keeply.

## Rodar com Docker

No diretorio `Keeply-V2`:

```bash
docker compose -f docker-compose.auth.yml up --build
```

API base: `http://localhost:18081/api/auth`

Endpoints principais:
- `POST /register`
- `POST /login`
- `GET /status`
- `POST /machines/register`
- `GET /machines`

## Rodar local (sem Docker)

```bash
mvn spring-boot:run
```

Variaveis suportadas:
- `SERVER_PORT` (default `8081`)
- `KEEPLY_AUTH_PG_URL` (default `jdbc:postgresql://localhost:5432/keeply_auth`)
- `KEEPLY_AUTH_PG_USERNAME` (default `keeply`)
- `KEEPLY_AUTH_PG_PASSWORD` (default `keeply123`)
