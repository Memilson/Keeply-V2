package com.keeply.app;

public class Main {
    public static void main(String[] args) throws Exception {
        if (args == null || args.length == 0 || "--help".equalsIgnoreCase(args[0])) {
            printHelp();
            return;
        }

        String cmd = args[0].toLowerCase();
        String[] tail = java.util.Arrays.copyOfRange(args, 1, args.length);

        switch (cmd) {
            case "scan" -> runScan(tail);
            case "gen" -> runGenerator(tail);
            default -> {
                System.err.println("Comando desconhecido: " + cmd);
                printHelp();
            }
        }
    }

    private static void runScan(String[] tail) throws Exception {
        // Sem args: scanner em C:\ salvando em scan-incremental.db
        String[] finalArgs = (tail.length == 0)
                ? new String[] { "C:\\", "scan-incremental.db" }
                : tail;

        leitorDeArquivos.runCli(finalArgs);
    }

    private static void runGenerator(String[] tail) throws Exception {
        // repassa os args para o gerador de sandbox (modo random/zero, etc.)
        com.keeply.tools.SandboxGenerator.main(tail);
    }

    private static void printHelp() {
        System.out.println("""
                Uso:
                  java -cp target/classes com.keeply.app.Main scan [<root> [<dbfile>]]
                    - Default sem args: root=C:\\ e db=scan-incremental.db

                  java -cp target/classes com.keeply.app.Main gen --root <pasta> --total-gb 1 --file-mb 4 --mode random --threads 6 --seed 42
                    - Repassa direto para com.keeply.tools.SandboxGenerator (geração de sandbox).

                Exemplos:
                  scan: java -cp target/classes com.keeply.app.Main scan "C:\\Users\\Angelo\\Documents" scan.db
                  gen (random 1GB): java -cp target/classes com.keeply.app.Main gen --root sandbox-random --total-gb 1 --file-mb 4 --mode random --threads 6 --seed 42
                """);
    }
}
