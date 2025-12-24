# Keeply

Projeto Java simples usando Maven (sem Spring).

Como compilar e executar:

```bash
mvn package
java -jar target/keeply-0.1.0.jar
```

Ou rodar diretamente com o Maven (requer plugin exec se preferir):

```bash
mvn -q compile exec:java -Dexec.mainClass="com.keeply.app.Main"
```

Estrutura criada:

- `pom.xml`
- `src/main/java/com/keeply/app/Main.java`

