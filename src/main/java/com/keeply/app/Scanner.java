package com.keeply.app;

import java.io.InputStream;
import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.security.MessageDigest;
import java.sql.Connection;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;

public class Scanner {

    public enum StageEnum { WALK, HASH, DB, IGNORE }
    public enum FileStatus { NEW, MODIFIED, MOVED, UNCHANGED, HASH_FAILED, SKIPPED_SIZE, SKIPPED_DISABLED }

    public record ScanIssue(
            long scanId, StageEnum stage, String path,
            String identityType, String identityValue,
            String errorType, String message, String rule
    ) {}

    public record FileMeta(
            String rootPath,
            String fullPath,
            String name,
            long sizeBytes,
            String createdAt,
            String modifiedAt,
            long createdMillis,
            long modifiedMillis,
            String fileKey,
            String identityType,
            String identityValue
    ) {}

    public record PrevInfo(
            long pathId,
            String knownPath,
            long sizeBytes,
            long modifiedAtMillis,
            String modifiedAt,
            String contentHash,
            String contentAlgo
    ) {}

    public record FileResult(FileMeta meta, FileStatus status, String contentAlgo, String contentHash, String reason) {}

    // --- Exclude Rules Otimizadas ---
    public static final class ExcludeRule {
        final PathMatcher matcher;
        public ExcludeRule(String glob) {
            this.matcher = FileSystems.getDefault().getPathMatcher("glob:" + glob);
        }
        boolean matches(Path p) { return matcher.matches(p); }
    }

    public record ScanConfig(
            int workers,
            int batchLimit,
            boolean computeHash,
            long hashMaxBytes,
            List<String> excludes,
            long preloadIndexMaxRows,
            int dbPoolSize
    ) {
        public static Builder builder() { return new Builder(); }
        public static class Builder {
            private int workers = 8, batchLimit = 2000, dbPoolSize = 10;
            private boolean computeHash = true;
            private long hashMaxBytes = 200L * 1024 * 1024;
            private long preloadIndexMaxRows = 5_000_000;
            private final List<String> excludes = new ArrayList<>();

            public Builder workers(int v) { this.workers = v; return this; }
            public Builder batchLimit(int v) { this.batchLimit = v; return this; }
            public Builder dbPoolSize(int v) { this.dbPoolSize = v; return this; }
            public Builder computeHash(boolean v) { this.computeHash = v; return this; }
            public Builder hashMaxBytes(long v) { this.hashMaxBytes = v; return this; }
            public Builder preloadIndexMaxRows(long v) { this.preloadIndexMaxRows = v; return this; }
            public Builder addExclude(String glob) { this.excludes.add(glob); return this; }

            public ScanConfig build() {
                return new ScanConfig(workers, batchLimit, computeHash, hashMaxBytes, List.copyOf(excludes), preloadIndexMaxRows, dbPoolSize);
            }
        }
    }

    public static class ScanMetrics {
        public final LongAdder filesScanned = new LongAdder();
        public final LongAdder filesIgnored = new LongAdder();
        public final LongAdder filesHashed = new LongAdder();
        public final LongAdder bytesScanned = new LongAdder();
        public final LongAdder bytesHashed = new LongAdder();
        public final LongAdder errorsWalk = new LongAdder();
        public final LongAdder errorsHash = new LongAdder();
        public final LongAdder dbBatches = new LongAdder();
        public final Instant start = Instant.now();
        public final AtomicBoolean running = new AtomicBoolean(false);
    }

    private record FileJob(Path absPath, long sizeBytes, long createdMillis, long modifiedMillis) {
        static final FileJob POISON = new FileJob(null, -1, -1, -1);
        boolean isPoison() { return absPath == null; }
    }

    private record HashOutcome(String hex, String algo, String reason) {}

    private static final HexFormat HEX = HexFormat.of();
    private static final ThreadLocal<MessageDigest> SHA256 = ThreadLocal.withInitial(() -> {
        try { return MessageDigest.getInstance("SHA-256"); }
        catch (Exception e) { throw new RuntimeException(e); }
    });
    // Buffer maior para IO mais rápido (128KB)
    private static final ThreadLocal<byte[]> HASH_BUF = ThreadLocal.withInitial(() -> new byte[128 * 1024]);

    public static class Engine {

        public static void runScanLogic(
                String rootPath,
                ScanConfig cfg,
                Database.SimplePool pool,
                ScanMetrics metrics,
                AtomicBoolean runningControl,
                java.util.function.Consumer<String> logger
        ) throws Exception {

            long scanId;
            // Mapa otimizado: Chave é o caminho absoluto da string
            Map<String, PrevInfo> index = new ConcurrentHashMap<>(100_000);

            try (Connection c = pool.borrow()) {
                scanId = Database.startScanLog(c, rootPath);
                
                // AQUI ESTÁ O TRUQUE: O Database deve carregar arquivos globais se possível,
                // ou o conceito de "root" deve ser relaxado no banco.
                Database.loadIndex(c, rootPath, cfg.preloadIndexMaxRows(), index);
                
                logger.accept("Index carregado: " + index.size() + " arquivos conhecidos.");
                c.commit();
            }

            Database.ParallelDbWriter writer = new Database.ParallelDbWriter(pool, scanId, cfg, metrics);
            if (index.isEmpty()) writer.clearCache();

            List<ExcludeRule> rules = cfg.excludes().stream().map(ExcludeRule::new).toList();
            Path root = Paths.get(rootPath);
            BlockingQueue<FileJob> queue = new ArrayBlockingQueue<>(10_000); // Fila maior

            Thread walker = Thread.ofVirtual().name("walker").start(() -> {
                try {
                    Files.walkFileTree(root, EnumSet.noneOf(FileVisitOption.class), Integer.MAX_VALUE, new SimpleFileVisitor<>() {
                        @Override
                        public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
                            if (!runningControl.get()) return FileVisitResult.TERMINATE;
                            // Otimização: Verifica exclusão de diretório apenas se não for a raiz
                            if (!dir.equals(root)) {
                                if (isExcluded(root.relativize(dir), rules)) {
                                    metrics.filesIgnored.increment();
                                    return FileVisitResult.SKIP_SUBTREE;
                                }
                            }
                            return FileVisitResult.CONTINUE;
                        }

                        @Override
                        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                            if (!runningControl.get()) return FileVisitResult.TERMINATE;
                            if (!attrs.isRegularFile()) return FileVisitResult.CONTINUE;

                            if (isExcluded(root.relativize(file), rules)) {
                                metrics.filesIgnored.increment();
                                return FileVisitResult.CONTINUE;
                            }

                            metrics.filesScanned.increment();
                            metrics.bytesScanned.add(attrs.size());

                            FileJob job = new FileJob(
                                    file.toAbsolutePath(),
                                    attrs.size(),
                                    attrs.creationTime().toMillis(),
                                    attrs.lastModifiedTime().toMillis()
                            );

                            try {
                                while (!queue.offer(job, 100, TimeUnit.MILLISECONDS)) {
                                    if (!runningControl.get()) return FileVisitResult.TERMINATE;
                                }
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                return FileVisitResult.TERMINATE;
                            }
                            return FileVisitResult.CONTINUE;
                        }

                        @Override
                        public FileVisitResult visitFileFailed(Path file, IOException exc) {
                            metrics.errorsWalk.increment();
                            writer.queueIssue(new ScanIssue(scanId, StageEnum.WALK, file.toString(), "PATH", file.toString(), exc.getClass().getSimpleName(), exc.getMessage(), null));
                            return FileVisitResult.CONTINUE;
                        }
                    });
                } catch (IOException e) {
                    metrics.errorsWalk.increment();
                    writer.queueIssue(new ScanIssue(scanId, StageEnum.WALK, root.toString(), "PATH", root.toString(), e.getClass().getSimpleName(), e.getMessage(), null));
                } finally {
                    for (int i = 0; i < cfg.workers(); i++) {
                        try { queue.put(FileJob.POISON); } catch (InterruptedException ignored) {}
                    }
                }
            });

            ThreadFactory tf = Thread.ofPlatform().name("scan-worker-", 0).factory();
            ExecutorService executor = Executors.newFixedThreadPool(cfg.workers(), tf);

            for (int i = 0; i < cfg.workers(); i++) {
                executor.submit(() -> {
                    try {
                        while (true) {
                            FileJob job = queue.take();
                            if (job.isPoison()) break;
                            if (!runningControl.get()) continue;

                            FileMeta fm = buildMeta(rootPath, job);
                            FileResult res = processFile(scanId, fm, job.absPath(), index, cfg, metrics, writer);
                            writer.queueFile(res);
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
            }

            walker.join();
            executor.shutdown();
            executor.awaitTermination(1, TimeUnit.DAYS);

            logger.accept("Finalizando escritas pendentes...");
            writer.flushAll();
            writer.waitForCompletion();

            try (Connection c = pool.borrow()) {
                Database.handleDeletions(c, scanId, rootPath);
                Database.finishScanLog(c, scanId);
                c.commit();
                // O release será chamado automaticamente pelo try-with-resources aqui
            }
            
            // ADICIONE ESTE LOG:
            logger.accept("Aguardando conexões do banco fecharem...");
            
            // O pool.close() será chamado no finally do ScannerTask no Controller
            logger.accept("Sucesso!");
        }

        private static FileMeta buildMeta(String rootPath, FileJob job) {
            // NORMALIZAÇÃO: Força tudo para '/' (padrão universal)
            String fullPath = job.absPath().toString().replace('\\', '/');
            String cleanRoot = rootPath.replace('\\', '/');
            
            String name = job.absPath().getFileName().toString();
            String createdAt = Database.safeTime(FileTime.fromMillis(job.createdMillis()));
            String modifiedAt = Database.safeTime(FileTime.fromMillis(job.modifiedMillis()));

            return new FileMeta(
                    cleanRoot, // Usa a raiz limpa
                    fullPath,  // Usa o caminho limpo
                    name,
                    job.sizeBytes(),
                    createdAt,
                    modifiedAt,
                    job.createdMillis(),
                    job.modifiedMillis(),
                    null,
                    "PATH",
                    fullPath
            );
        }

        // Unificado: serve para arquivo e diretório (basta passar Path relativo)
        private static boolean isExcluded(Path rel, List<ExcludeRule> rules) {
            for (ExcludeRule r : rules) if (r.matches(rel)) return true;
            return false;
        }

        static FileResult processFile(
                long scanId, FileMeta curr, Path absPath,
                Map<String, PrevInfo> index, ScanConfig cfg,
                ScanMetrics metrics, Database.ParallelDbWriter writer
        ) {
            // Busca no índice pelo CAMINHO ABSOLUTO
            PrevInfo prev = index.get(curr.fullPath());

            if (prev == null) {
                HashOutcome h = computeHashIfNeeded(scanId, curr, absPath, cfg, metrics, writer);
                String reason = h.reason == null ? "NEW" : ("NEW|" + h.reason);
                return new FileResult(curr, FileStatus.NEW, h.algo, h.hex, reason);
            }

            boolean metaDiff;
            // Comparação otimizada por millis se disponível
            if (prev.modifiedAtMillis() > 0) {
                metaDiff = prev.sizeBytes() != curr.sizeBytes() || Math.abs(prev.modifiedAtMillis() - curr.modifiedMillis()) > 1000; // Tolerância de 1s para FS diferentes
            } else {
                metaDiff = prev.sizeBytes() != curr.sizeBytes() || !Objects.equals(prev.modifiedAt(), curr.modifiedAt());
            }

            if (metaDiff) {
                HashOutcome h = computeHashIfNeeded(scanId, curr, absPath, cfg, metrics, writer);
                // Se hash for igual apesar da data/tamanho mudar (raro, mas acontece), considera UNCHANGED
                if (h.hex != null && h.hex.equals(prev.contentHash())) {
                    return new FileResult(curr, FileStatus.UNCHANGED, h.algo, h.hex, "META_CHANGE_HASH_SAME");
                }
                String reason = h.reason == null ? "MODIFIED" : ("MODIFIED|" + h.reason);
                return new FileResult(curr, FileStatus.MODIFIED, h.algo, h.hex, reason);
            }

            return new FileResult(curr, FileStatus.UNCHANGED, prev.contentAlgo(), prev.contentHash(), null);
        }

        static HashOutcome computeHashIfNeeded(
                long scanId, FileMeta m, Path absPath, ScanConfig cfg,
                ScanMetrics metrics, Database.ParallelDbWriter writer
        ) {
            if (!cfg.computeHash()) return new HashOutcome(null, null, FileStatus.SKIPPED_DISABLED.name());
            if (cfg.hashMaxBytes() > 0 && m.sizeBytes() > cfg.hashMaxBytes()) return new HashOutcome(null, null, FileStatus.SKIPPED_SIZE.name());

            MessageDigest md = SHA256.get();
            md.reset();
            byte[] buf = HASH_BUF.get();

            try (InputStream in = Files.newInputStream(absPath)) {
                int r;
                while ((r = in.read(buf)) != -1) md.update(buf, 0, r);
                metrics.filesHashed.increment();
                metrics.bytesHashed.add(m.sizeBytes());
                return new HashOutcome(HEX.formatHex(md.digest()), "SHA-256", null);
            } catch (Exception e) {
                metrics.errorsHash.increment();
                writer.queueIssue(new ScanIssue(scanId, StageEnum.HASH, m.fullPath(), m.identityType(), m.identityValue(), e.getClass().getSimpleName(), e.getMessage(), null));
                return new HashOutcome(null, null, FileStatus.HASH_FAILED.name());
            }
        }
    }
}