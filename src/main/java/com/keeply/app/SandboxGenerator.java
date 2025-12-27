package com.keeply.tools;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.FileAttribute;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * SandboxGenerator
 * - Cria uma pasta "sandbox" com dados para teste (zeros ou aleatórios).
 * - Gera muitos arquivos (inclusive milhões) com estrutura de diretórios.
 * - Escreve/atualiza .gitignore para ignorar "sandbox/" automaticamente.
 *
 * Segurança:
 * - Checa espaço livre e não deixa você estourar o disco sem avisar.
 * - Limita por padrão totalGB a 50, a menos que use --force.
 *
 * Exemplo (PowerShell):
 *   mvn -q -DskipTests exec:java "-Dexec.mainClass=com.keeply.tools.SandboxGenerator" "-Dexec.args=--total-gb 5 --file-kb 64 --mode zero --threads 8"
 *
 * Ou rodando direto:
 *   java com.keeply.tools.SandboxGenerator --total-gb 2 --file-mb 1 --mode random
 */
public class SandboxGenerator {

    enum Mode { ZERO, RANDOM }

    static final class Config {
        Path root = Paths.get(System.getProperty("user.dir")).resolve("sandbox");
        long totalBytes = 1L * 1024 * 1024 * 1024; // 1 GB default
        int fileBytes = 1 * 1024 * 1024;           // 1 MB default
        int filesPerDir = 2000;                    // evita diretórios gigantes
        int depth = 2;                             // 2 níveis costuma ser suficiente
        int dirsPerLevel = 200;                    // 200^2 = 40k dirs possíveis
        int threads = Math.max(2, Runtime.getRuntime().availableProcessors());
        Mode mode = Mode.ZERO;
        long seed = 123456789L;
        boolean force = false;
        boolean dryRun = false;
        boolean writeGitignore = true;
        boolean cleanFirst = false;
        String prefix = "blob";
    }

    public static void main(String[] args) throws Exception {
        Config cfg = parseArgs(args);

        if (cfg.writeGitignore) ensureGitignoreHasSandboxRule(cfg.root);

        if (cfg.cleanFirst) {
            System.out.println("[INFO] Cleaning sandbox: " + cfg.root.toAbsolutePath());
            if (!cfg.dryRun) deleteRecursively(cfg.root);
        }

        Files.createDirectories(cfg.root);

        // Guard rails
        long totalGB = cfg.totalBytes / (1024L * 1024 * 1024);
        if (!cfg.force && totalGB > 50) {
            System.err.println("[ABORT] total-gb > 50 sem --force. Você pediu: " + totalGB + " GB");
            System.err.println("Use --force se você realmente quer isso (e tenha certeza do espaço).");
            System.exit(2);
        }

        long free = Files.getFileStore(cfg.root).getUsableSpace();
        long needed = cfg.totalBytes;
        if (needed > (long)(free * 0.90)) {
            System.err.println("[ABORT] Espaço livre insuficiente no disco da sandbox.");
            System.err.println("Free:  " + human(free));
            System.err.println("Need:  " + human(needed));
            System.err.println("Dica: reduza --total-gb, use outro drive, ou limpe espaço.");
            System.exit(3);
        }

        long totalFiles = (cfg.totalBytes + cfg.fileBytes - 1L) / cfg.fileBytes;

        System.out.println("[INFO] Sandbox root: " + cfg.root.toAbsolutePath());
        System.out.println("[INFO] Mode: " + cfg.mode);
        System.out.println("[INFO] Total: " + human(cfg.totalBytes) + "  | File: " + human(cfg.fileBytes));
        System.out.println("[INFO] Target files: " + String.format("%,d", totalFiles));
        System.out.println("[INFO] Layout: depth=" + cfg.depth + " dirs/level=" + cfg.dirsPerLevel + " files/dir=" + cfg.filesPerDir);
        System.out.println("[INFO] Threads: " + cfg.threads + (cfg.dryRun ? " (DRY RUN)" : ""));

        AtomicLong next = new AtomicLong(0);
        LongAdder bytesDone = new LongAdder();
        LongAdder filesDone = new LongAdder();
        LongAdder errors = new LongAdder();

        Instant start = Instant.now();

        ExecutorService pool = Executors.newFixedThreadPool(cfg.threads);
        ScheduledExecutorService ticker = Executors.newSingleThreadScheduledExecutor();

        ticker.scheduleAtFixedRate(() -> {
            long f = filesDone.sum();
            long b = bytesDone.sum();
            Duration d = Duration.between(start, Instant.now());
            double sec = Math.max(1.0, d.toMillis() / 1000.0);
            double mbps = (b / 1024.0 / 1024.0) / sec;
            double fps = f / sec;

            long remaining = Math.max(0L, cfg.totalBytes - b);
            long etaSec = (mbps > 0) ? (long)((remaining / 1024.0 / 1024.0) / mbps) : -1;

            System.err.printf(
                    "\r[GEN] Files %,d/%,d | Bytes %s/%s | %.1f MB/s | %.1f files/s | Err %d | ETA %s     ",
                    f, totalFiles,
                    human(b), human(cfg.totalBytes),
                    mbps, fps, errors.sum(),
                    (etaSec >= 0 ? fmtEta(etaSec) : "...")
            );
        }, 0, 1000, TimeUnit.MILLISECONDS);

        List<Future<?>> futures = new ArrayList<>();
        for (int t = 0; t < cfg.threads; t++) {
            final int tid = t;
            futures.add(pool.submit(() -> workerLoop(cfg, tid, totalFiles, next, bytesDone, filesDone, errors)));
        }

        for (Future<?> f : futures) f.get();

        ticker.shutdownNow();
        pool.shutdown();

        System.err.println();
        Duration total = Duration.between(start, Instant.now());
        System.out.println("\n[DONE] Wrote " + String.format("%,d", filesDone.sum()) +
                " files, " + human(bytesDone.sum()) +
                " in " + total.toSeconds() + "s" +
                " | errors=" + errors.sum());
    }

    private static void workerLoop(
            Config cfg,
            int tid,
            long totalFiles,
            AtomicLong next,
            LongAdder bytesDone,
            LongAdder filesDone,
            LongAdder errors
    ) {
        // RNG por thread (para RANDOM)
        SplittableRandom rng = new SplittableRandom(cfg.seed ^ (0x9E3779B97F4A7C15L * (tid + 1L)));

        // buffer reutilizado (para RANDOM)
        byte[] buffer = new byte[Math.min(4 * 1024 * 1024, cfg.fileBytes)]; // até 4MB

        while (true) {
            long i = next.getAndIncrement();
            if (i >= totalFiles) break;

            long remaining = cfg.totalBytes - (i * (long) cfg.fileBytes);
            int size = (int) Math.min(cfg.fileBytes, Math.max(0L, remaining));
            if (size <= 0) break;

            Path dir = computeDir(cfg.root, i, cfg.filesPerDir, cfg.depth, cfg.dirsPerLevel);
            Path file = dir.resolve(cfg.prefix + "-" + String.format("%09d", i) + ".bin");

            try {
                if (!cfg.dryRun) {
                    Files.createDirectories(dir);

                    if (cfg.mode == Mode.ZERO) {
                        createZeroFile(file, size);
                    } else {
                        createRandomFile(file, size, rng, buffer);
                    }
                }
                filesDone.increment();
                bytesDone.add(size);
            } catch (Exception e) {
                errors.increment();
                // Você pode logar o path aqui se quiser depurar algum caso específico:
                // System.err.println("\n[ERR] " + file + " -> " + e);
            }
        }
    }

    /** Distribui arquivos em diretórios usando "dirId = i/filesPerDir" e decomposição em base dirsPerLevel. */
    private static Path computeDir(Path root, long fileIndex, int filesPerDir, int depth, int dirsPerLevel) {
        long dirId = fileIndex / Math.max(1, filesPerDir);
        Path p = root;
        for (int level = 0; level < depth; level++) {
            long seg = dirId % dirsPerLevel;
            p = p.resolve("d" + level + "-" + String.format("%03d", seg));
            dirId /= dirsPerLevel;
        }
        return p;
    }

    /** Cria arquivo "em branco" (zeros) com tamanho exato. */
    private static void createZeroFile(Path file, int size) throws IOException {
        // RandomAccessFile setLength é bem eficiente para "arquivo cheio de zero".
        try (RandomAccessFile raf = new RandomAccessFile(file.toFile(), "rw")) {
            raf.setLength(size);
        }
    }

    /** Cria arquivo com bytes aleatórios (mais pesado, mas bom pra testar hash/dedupe real). */
    private static void createRandomFile(Path file, int size, SplittableRandom rng, byte[] buffer) throws IOException {
        try (FileChannel ch = FileChannel.open(file,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {

            int left = size;
            while (left > 0) {
                int chunk = Math.min(left, buffer.length);
                fillRandom(buffer, chunk, rng);

                ByteBuffer bb = ByteBuffer.wrap(buffer, 0, chunk);
                while (bb.hasRemaining()) ch.write(bb);

                left -= chunk;
            }
        }
    }

    private static void fillRandom(byte[] buf, int len, SplittableRandom rng) {
        int i = 0;
        while (i + 8 <= len) {
            long v = rng.nextLong();
            buf[i++] = (byte) (v);
            buf[i++] = (byte) (v >>> 8);
            buf[i++] = (byte) (v >>> 16);
            buf[i++] = (byte) (v >>> 24);
            buf[i++] = (byte) (v >>> 32);
            buf[i++] = (byte) (v >>> 40);
            buf[i++] = (byte) (v >>> 48);
            buf[i++] = (byte) (v >>> 56);
        }
        while (i < len) buf[i++] = (byte) rng.nextInt(256);
    }

    /** Garante que o .gitignore do projeto ignore a sandbox. */
    private static void ensureGitignoreHasSandboxRule(Path sandboxRoot) throws IOException {
        Path projectRoot = Paths.get(System.getProperty("user.dir")).toAbsolutePath().normalize();
        Path gitignore = projectRoot.resolve(".gitignore");

        // regra relativa (o normal é sandbox/ na raiz do repo)
        String rule = "sandbox/";

        // Se a sandbox não estiver na raiz do projeto, ainda assim ignora "sandbox/" por padrão.
        // (Se você quiser algo mais específico, pode adaptar para calcular o path relativo.)
        if (!Files.exists(gitignore)) {
            Files.writeString(gitignore,
                    "# Keeply test sandbox (gerado automaticamente)\n" +
                    rule + "\n" +
                    "\n# Build outputs\n" +
                    "target/\n" +
                    "\n# DB/logs de teste\n" +
                    "*.db\n" +
                    "logs/\n",
                    StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE);
            System.out.println("[INFO] Created .gitignore with sandbox rule.");
            return;
        }

        String content = Files.readString(gitignore, StandardCharsets.UTF_8);
        if (content.contains("\n" + rule) || content.startsWith(rule)) {
            return; // já tem
        }

        String toAppend =
                "\n# Keeply test sandbox (gerado automaticamente)\n" +
                rule + "\n";

        Files.writeString(gitignore, toAppend, StandardCharsets.UTF_8, StandardOpenOption.APPEND);
        System.out.println("[INFO] Updated .gitignore: added '" + rule + "'");
    }

    private static void deleteRecursively(Path root) throws IOException {
        if (!Files.exists(root)) return;
        Files.walk(root)
                .sorted(Comparator.reverseOrder())
                .forEach(p -> {
                    try { Files.deleteIfExists(p); }
                    catch (IOException ignored) {}
                });
    }

    // ---------------- ARG PARSE ----------------

    private static Config parseArgs(String[] args) {
        Config cfg = new Config();
        List<String> a = (args == null) ? List.of() : Arrays.asList(args);

        for (int i = 0; i < a.size(); i++) {
            String k = a.get(i);

            switch (k) {
                case "--root" -> cfg.root = Paths.get(next(a, ++i));
                case "--total-gb" -> cfg.totalBytes = parseLong(next(a, ++i)) * 1024L * 1024 * 1024;
                case "--total-mb" -> cfg.totalBytes = parseLong(next(a, ++i)) * 1024L * 1024;
                case "--file-mb" -> cfg.fileBytes = (int) (parseLong(next(a, ++i)) * 1024L * 1024);
                case "--file-kb" -> cfg.fileBytes = (int) (parseLong(next(a, ++i)) * 1024L);
                case "--files-per-dir" -> cfg.filesPerDir = (int) parseLong(next(a, ++i));
                case "--depth" -> cfg.depth = (int) parseLong(next(a, ++i));
                case "--dirs-per-level" -> cfg.dirsPerLevel = (int) parseLong(next(a, ++i));
                case "--threads" -> cfg.threads = (int) parseLong(next(a, ++i));
                case "--seed" -> cfg.seed = parseLong(next(a, ++i));
                case "--prefix" -> cfg.prefix = next(a, ++i);

                case "--mode" -> {
                    String v = next(a, ++i).trim().toLowerCase(Locale.ROOT);
                    cfg.mode = v.equals("random") ? Mode.RANDOM : Mode.ZERO;
                }

                case "--force" -> cfg.force = true;
                case "--dry-run" -> cfg.dryRun = true;
                case "--no-gitignore" -> cfg.writeGitignore = false;
                case "--clean" -> cfg.cleanFirst = true;

                case "--help" -> {
                    printHelpAndExit();
                }

                default -> {
                    // ignora unknowns pra não quebrar seu exec args
                }
            }
        }

        // sanitiza
        cfg.fileBytes = Math.max(1, cfg.fileBytes);
        cfg.filesPerDir = Math.max(100, cfg.filesPerDir);
        cfg.depth = Math.max(1, cfg.depth);
        cfg.dirsPerLevel = Math.max(10, cfg.dirsPerLevel);
        cfg.threads = Math.max(1, cfg.threads);
        cfg.prefix = (cfg.prefix == null || cfg.prefix.isBlank()) ? "blob" : cfg.prefix;

        return cfg;
    }

    private static String next(List<String> a, int idx) {
        if (idx < 0 || idx >= a.size()) throw new IllegalArgumentException("Missing value for arg at index " + idx);
        return a.get(idx);
    }

    private static long parseLong(String s) {
        return Long.parseLong(s.replace("_", "").trim());
    }

    private static void printHelpAndExit() {
        System.out.println("""
                SandboxGenerator args:
                  --root <path>             (default: ./sandbox)
                  --total-gb <N>            total de dados
                  --total-mb <N>
                  --file-mb <N>             tamanho de cada arquivo
                  --file-kb <N>
                  --mode zero|random        (default: zero)
                  --threads <N>             (default: cpu count)
                  --files-per-dir <N>       (default: 2000)
                  --depth <N>               (default: 2)
                  --dirs-per-level <N>      (default: 200)
                  --seed <N>                (random mode)
                  --prefix <name>           (default: blob)
                  --clean                   apaga sandbox antes
                  --dry-run                 não escreve no disco
                  --force                   permite >50GB
                  --no-gitignore            não mexe no .gitignore
                """);
        System.exit(0);
    }

    // ---------------- FORMAT ----------------

    private static String fmtEta(long sec) {
        long h = sec / 3600;
        long m = (sec % 3600) / 60;
        long s = sec % 60;
        return String.format("%02d:%02d:%02d", h, m, s);
    }

    private static String human(long bytes) {
        double b = bytes;
        String[] u = {"B", "KB", "MB", "GB", "TB"};
        int i = 0;
        while (b >= 1024 && i < u.length - 1) { b /= 1024; i++; }
        return String.format(Locale.ROOT, "%.2f %s", b, u[i]);
    }
}
