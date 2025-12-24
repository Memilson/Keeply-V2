package com.keeply.app;

public class Main {
    public static void main(String[] args) throws Exception {
        // Sem argumentos: roda o scanner em "src" gravando em "scan.db".
        // Com argumentos: repassa direto para o wrapper CLI do leitor.
        String[] finalArgs = (args == null || args.length == 0)
                ? new String[] { "src", "scan.db" }
                : args;

        leitorDeArquivos.runCli(finalArgs);
    }
}
