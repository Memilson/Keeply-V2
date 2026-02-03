# Keeply

Projeto Java simples usando Maven (sem Spring).

Como compilar e executar:

```bash
mvn package
java -jar target/keeply-scanner-gui-1.0.0.jar
```

Ou rodar diretamente com o Maven (JavaFX):

```bash
mvn javafx:run
```

Estrutura criada:

- `pom.xml`
- `src/main/java/com/keeply/app/Main.java`

Configuração (opcional):

- `KEEPLY_DB_ENCRYPTION=1` habilita criptografia do arquivo `.enc` (AES-GCM) em repouso
- `KEEPLY_SECRET_KEY=...` define a chave (obrigatória quando `KEEPLY_DB_ENCRYPTION=1`)

