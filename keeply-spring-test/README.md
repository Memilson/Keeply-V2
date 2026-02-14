# keeply-spring-test

Backend Spring Boot para manter o Keeply rodando como servico HTTP para o frontend.

## Rodar local

```bash
cd keeply-spring-test
mvn spring-boot:run
```

API: `http://localhost:8081`

## Endpoints

- `GET /api/keeply/health`
- `GET /api/keeply/db-check`
- `GET /api/keeply/history?limit=20`
- `GET /api/keeply/cli-history`
- `POST /api/keeply/scan`

Exemplo `scan`:

```bash
curl -X POST http://localhost:8081/api/keeply/scan \
  -H "Content-Type: application/json" \
  -d '{"root":"C:/dados","dest":"D:/backups"}'
```

## Configuracao

Arquivo `src/main/resources/application.properties`:

- `keeply.db.file`: nome do arquivo sqlite no projeto Keeply (padrao `keeply.db`).
- `keeply.cli.project-dir`: caminho opcional do projeto Keeply (se vazio, tenta autodetectar).

## Rodar como servico no Windows (NSSM)

1. Gere o jar:

```bash
cd keeply-spring-test
mvn -DskipTests package
```

2. Instale o servico (PowerShell admin):

```powershell
nssm install KeeplyApi "C:\Program Files\Java\jdk-21\bin\java.exe" "-jar C:\Users\angel\OneDrive - Olegario Marinho\Documentos\GitHub\Keeply-V2\keeply-spring-test\target\keeply-spring-test-0.0.1-SNAPSHOT.jar"
nssm set KeeplyApi AppDirectory "C:\Users\angel\OneDrive - Olegario Marinho\Documentos\GitHub\Keeply-V2\keeply-spring-test"
nssm start KeeplyApi
```

3. Verifique:

```powershell
curl http://localhost:8081/api/keeply/health
```
