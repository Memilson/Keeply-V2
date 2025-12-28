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
import java.util.HexFormat;
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

    /**
     * Mantive createdAt/modifiedAt como String para compatibilidade com seu DB atual,
     * mas também carrego createdMillis/modifiedMillis pra comparação rápida.
     */
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

    /**
     * PrevInfo com modifiedAtMillis (ideal) + modifiedAt (fallback).
     * Se seu Database.loadIndex ainda não tiver millis, pode preencher com -1 e usar modifiedAt.
     */
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

    // --- Exclude Rules ---
    public static final class ExcludeRule {
        final String glob;
        final PathMatcher matcher;

        public ExcludeRule(String glob) {
            this.glob = glob;
            this.matcher = FileSystems.getDefault().getPathMatcher("glob:" + glob);
        }

        boolean matchesFile(Path rel) { return matcher.matches(rel); }

        // truque pra pegar globs tipo **/node_modules/** também no preVisitDirectory
        boolean matchesDir(Path relDir) { return matcher.matches(relDir) || matcher.matches(relDir.resolve("__x__")); }
    }

    // --- Configuration ---
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

    // --- Metrics ---
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

    // --- Internal job: evita enfileirar strings e objetos caros no walker ---
    private record FileJob(Path absPath, long sizeBytes, long createdMillis, long modifiedMillis) {
        static final FileJob POISON = new FileJob(null, -1, -1, -1);
        boolean isPoison() { return absPath == null; }
    }

    private record HashOutcome(String hex, String algo, String reason) {}

    // --- ThreadLocals (hash) ---
    private static final HexFormat HEX = HexFormat.of();
    private static final ThreadLocal<MessageDigest> SHA256 = ThreadLocal.withInitial(() -> {
        try { return MessageDigest.getInstance("SHA-256"); }
        catch (Exception e) { throw new RuntimeException(e); }
    });
    private static final ThreadLocal<byte[]> HASH_BUF = ThreadLocal.withInitial(() -> new byte[64 * 1024]);

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

            // INDEX: chave agora é o fullPath puro (sem "TYPE:value" concatenado por arquivo)
            Map<String, PrevInfo> index = new HashMap<>(1 << 20);

            try (Connection c = pool.borrow()) {
                scanId = Database.startScanLog(c, rootPath);

                // IMPORTANTE: seu Database.loadIndex precisa preencher index.put(fullPath, prevInfo)
                Database.loadIndex(c, rootPath, cfg.preloadIndexMaxRows(), index);

                logger.accept("Index carregado: " + index.size() + " arquivos.");
                c.commit();
            }

            Database.ParallelDbWriter writer = new Database.ParallelDbWriter(pool, scanId, cfg, metrics);
            if (index.isEmpty()) writer.clearCache();

            List<ExcludeRule> rules = cfg.excludes().stream().map(ExcludeRule::new).toList();
            Path root = Paths.get(rootPath);

            BlockingQueue<FileJob> queue = new ArrayBlockingQueue<>(5000);

            // 1) Walker: SKIP_SUBTREE + offer com timeout (cancel-safe)
            Thread walker = Thread.ofVirtual().start(() -> {
                try {
                    Files.walkFileTree(root, EnumSet.noneOf(FileVisitOption.class), Integer.MAX_VALUE, new SimpleFileVisitor<>() {

                        @Override
                        public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
                            if (!runningControl.get()) return FileVisitResult.TERMINATE;

                            if (!dir.equals(root)) {
                                Path rel = root.relativize(dir);
                                if (isExcludedDir(rel, rules)) {
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

                            Path rel = root.relativize(file);
                            if (isExcludedFile(rel, rules)) {
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
                                while (!queue.offer(job, 200, TimeUnit.MILLISECONDS)) {
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
                            writer.queueIssue(new ScanIssue(
                                    scanId, StageEnum.WALK,
                                    file.toString(),
                                    "PATH", file.toString(),
                                    exc.getClass().getSimpleName(),
                                    exc.getMessage(),
                                    null
                            ));
                            return FileVisitResult.CONTINUE;
                        }
                    });
                } catch (IOException e) {
                    metrics.errorsWalk.increment();
                    writer.queueIssue(new ScanIssue(
                            scanId, StageEnum.WALK,
                            root.toString(),
                            "PATH", root.toString(),
                            e.getClass().getSimpleName(),
                            e.getMessage(),
                            null
                    ));
                } finally {
                    for (int i = 0; i < cfg.workers(); i++) {
                        try { queue.put(FileJob.POISON); }
                        catch (InterruptedException e) { Thread.currentThread().interrupt(); break; }
                    }
                }
            });

            // 2) Workers: drenam fila sempre (evita deadlock de cancel)
            ThreadFactory tf = Thread.ofPlatform().name("scan-worker-", 0).factory();
            ExecutorService executor = Executors.newFixedThreadPool(cfg.workers(), tf);

            for (int i = 0; i < cfg.workers(); i++) {
                executor.submit(() -> {
                    for (;;) {
                        FileJob job;
                        try {
                            job = queue.take();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            break;
                        }
                        if (job.isPoison()) break;

                        // se cancelou, só ignora processamento (mas não para de consumir)
                        if (!runningControl.get()) continue;

                        FileMeta fm = buildMeta(rootPath, job);
                        FileResult res = processFile(scanId, fm, job.absPath(), index, cfg, metrics, writer);
                        writer.queueFile(res);
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
            }
            logger.accept("Sucesso!");
        }

        private static FileMeta buildMeta(String rootPath, FileJob job) {
            String fullPath = job.absPath().toString();
            String name = job.absPath().getFileName().toString();

            // compat com DB atual (string) + millis pra comparação
            String createdAt = Database.safeTime(FileTime.fromMillis(job.createdMillis()));
            String modifiedAt = Database.safeTime(FileTime.fromMillis(job.modifiedMillis()));

            return new FileMeta(
                    rootPath,
                    fullPath,
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

        private static boolean isExcludedFile(Path rel, List<ExcludeRule> rules) {
            for (ExcludeRule r : rules) if (r.matchesFile(rel)) return true;
            return false;
        }

        private static boolean isExcludedDir(Path relDir, List<ExcludeRule> rules) {
            for (ExcludeRule r : rules) if (r.matchesDir(relDir)) return true;
            return false;
        }

        static FileResult processFile(
                long scanId,
                FileMeta curr,
                Path absPath,
                Map<String, PrevInfo> index,
                ScanConfig cfg,
                ScanMetrics metrics,
                Database.ParallelDbWriter writer
        ) {
            PrevInfo prev = index.get(curr.fullPath());

            if (prev == null) {
                HashOutcome h = computeHashIfNeeded(scanId, curr, absPath, cfg, metrics, writer);
                String reason = h.reason == null ? "NEW" : ("NEW|" + h.reason);
                return new FileResult(curr, FileStatus.NEW, h.algo, h.hex, reason);
            }

            boolean metaDiff;
            if (prev.modifiedAtMillis() >= 0) {
                metaDiff = prev.sizeBytes() != curr.sizeBytes() || prev.modifiedAtMillis() != curr.modifiedMillis();
            } else {
                metaDiff = prev.sizeBytes() != curr.sizeBytes() || !Objects.equals(prev.modifiedAt(), curr.modifiedAt());
            }

            if (metaDiff) {
                HashOutcome h = computeHashIfNeeded(scanId, curr, absPath, cfg, metrics, writer);
                String reason = h.reason == null ? "MODIFIED" : ("MODIFIED|" + h.reason);
                return new FileResult(curr, FileStatus.MODIFIED, h.algo, h.hex, reason);
            }

            return new FileResult(curr, FileStatus.UNCHANGED, prev.contentAlgo(), prev.contentHash(), null);
        }

        static HashOutcome computeHashIfNeeded(
                long scanId,
                FileMeta m,
                Path absPath,
                ScanConfig cfg,
                ScanMetrics metrics,
                Database.ParallelDbWriter writer
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
                writer.queueIssue(new ScanIssue(
                        scanId, StageEnum.HASH,
                        m.fullPath(),
                        m.identityType(), m.identityValue(),
                        e.getClass().getSimpleName(),
                        e.getMessage(),
                        null
                ));
                return new HashOutcome(null, null, FileStatus.HASH_FAILED.name());
            }
        }
    }
}
