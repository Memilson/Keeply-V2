package com.keeply.app;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.security.MessageDigest;
import java.sql.*;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import java.util.HexFormat;

/**
 * LeitorDeArquivos (Incremental + Snapshot + Histórico + Issues + Summary)
 *
 * Melhorias aplicadas:
 * - Snapshot puro: deletados removidos do file_state (sem fantasmas).
 * - Incremental fino: UNCHANGED => TOUCH (update só do last_scan_id).
 * - Status de hash explícito (hash_status) em file_state.
 * - scan_summary (1 linha por scan) para "timelapse" sem inflar banco.
 * - Índice híbrido: Map em RAM + lookup sob demanda no SQLite com cache LRU.
 *   (Workers usam conexões read-only próprias; writer usa 1 conexão write).
 * - Erros por stage (WALK/HASH/DB/IGNORE) com queue não-bloqueante (best-effort).
 * - Métricas honestas: Scan MB/s (metadados) vs Hash MB/s (bytes lidos).
 *
 * Requisitos:
 * - Java 21+
 * - sqlite-jdbc no classpath
 */
public class leitorDeArquivos {

    static {
        try { Class.forName("org.sqlite.JDBC"); }
        catch (ClassNotFoundException e) {
            System.err.println("SQLite JDBC driver not found: " + e.getMessage());
        }
    }

    // ==================================================================================
    // 0) LOG TEE (opcional)
    // ==================================================================================
    static final class TeeOutputStream extends OutputStream {
        private final OutputStream a, b;
        TeeOutputStream(OutputStream a, OutputStream b) { this.a = a; this.b = b; }
        @Override public void write(int x) throws IOException { a.write(x); b.write(x); }
        @Override public void write(byte[] buf, int off, int len) throws IOException { a.write(buf, off, len); b.write(buf, off, len); }
        @Override public void flush() throws IOException { a.flush(); b.flush(); }
        @Override public void close() throws IOException { b.close(); }
    }

    static LogTee setupLogFile(Path logDir, String prefix) throws IOException {
        Files.createDirectories(logDir);
        String ts = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss").format(LocalDateTime.now());
        Path logFile = logDir.resolve(prefix + "-" + ts + ".log");
        PrintStream fileOut = new PrintStream(
                Files.newOutputStream(logFile, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE),
                true, StandardCharsets.UTF_8
        );
        PrintStream originalOut = System.out;
        System.setOut(new PrintStream(new TeeOutputStream(originalOut, fileOut), true, StandardCharsets.UTF_8));
        System.err.println("Log file: " + logFile.toAbsolutePath());
        return new LogTee(originalOut, fileOut);
    }

    static final class LogTee implements AutoCloseable {
        private final PrintStream originalOut, fileOut;
        LogTee(PrintStream originalOut, PrintStream fileOut) { this.originalOut = originalOut; this.fileOut = fileOut; }
        @Override public void close() {
            System.out.flush();
            System.setOut(originalOut);
            fileOut.flush();
            fileOut.close();
        }
    }

    static void logJson(String ctx, String lvl, String msg, Map<String, Object> kv) {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        sb.append("\"ts\":\"").append(Instant.now()).append("\",");
        sb.append("\"lvl\":\"").append(lvl).append("\",");
        sb.append("\"ctx\":\"").append(ctx).append("\",");
        sb.append("\"msg\":\"").append(escapeJson(msg)).append("\"");
        if (kv != null) {
            for (var e : kv.entrySet()) {
                sb.append(",\"").append(escapeJson(e.getKey())).append("\":");
                Object v = e.getValue();
                if (v == null) sb.append("null");
                else if (v instanceof Number || v instanceof Boolean) sb.append(v);
                else sb.append("\"").append(escapeJson(String.valueOf(v))).append("\"");
            }
        }
        sb.append("}");
        System.out.println(sb);
    }

    static String escapeJson(String s) {
        if (s == null) return "";
        return s.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n").replace("\r", "\\r");
    }

    // helper to build small maps from alternating key,value pairs (avoids Map.of arity limits)
    static Map<String, Object> kv(Object... pairs) {
        Map<String, Object> m = new LinkedHashMap<>();
        for (int i = 0; i + 1 < pairs.length; i += 2) {
            m.put(String.valueOf(pairs[i]), pairs[i + 1]);
        }
        return Map.copyOf(m);
    }

    static boolean isWindows() {
        String os = System.getProperty("os.name", "unknown").toLowerCase(Locale.ROOT);
        return os.contains("win");
    }

    // ==================================================================================
    // 1) DOMAIN
    // ==================================================================================
    public enum Stage { WALK, HASH, DB, IGNORE }

    public enum HashStatus {
        NONE,          // não tinha hash anterior e não calculou agora (ex.: SKIPPED_SIZE, DISABLED)
        OK,            // calculou hash com sucesso
        SKIPPED_SIZE,  // pulou por tamanho
        DISABLED,      // hash desligado
        FAILED         // tentou e falhou
    }

    public enum FileStatus {
        NEW,
        MODIFIED,
        MOVED,
        UNCHANGED,
        HASH_FAILED,
        SKIPPED_SIZE,
        SKIPPED_DISABLED
    }

    public record ScanIssue(
            long scanId,
            Stage stage,
            String path,
            String identityType,
            String identityValue,
            String errorType,
            String message,
            String rule
    ) {}

    public record FileMeta(
            String rootPath,
            String fullPath,
            String name,
            long sizeBytes,
            String createdAt,
            String modifiedAt,
            String fileKey,
            String identityType,
            String identityValue
    ) {}

    public record PrevInfo(
            long pathId,
            String knownPath,
            long sizeBytes,
            String modifiedAt,
            String contentHash,
            String contentAlgo,
            String hashStatus
    ) {}

    public record FileResult(
            FileMeta meta,
            FileStatus status,
            String contentAlgo,
            String contentHash,
            HashStatus hashStatus,
            String reason,
            boolean poison
    ) {}

    static final class ExcludeRule {
        final String glob;
        final PathMatcher matcher;
        final LongAdder count = new LongAdder();
        ExcludeRule(String glob) {
            this.glob = glob;
            this.matcher = FileSystems.getDefault().getPathMatcher("glob:" + glob);
        }
    }

    // ==================================================================================
    // 2) CONFIG
    // ==================================================================================
    public record ScanConfig(
            int workers,
            int queueCapacity,
            int batchLimit,
            boolean computeHash,
            long hashMaxBytes,
            boolean followLinks,
            List<String> excludes,
            long preloadIndexMaxRows,
            long dbRetryBaseMs,
            int dbMaxRetries,
            long writerPollSleepMs,
            long issueOfferTimeoutMs,
            int lruCacheSize,
            boolean enableDbLookupOnMiss
    ) {
        public static Builder builder() { return new Builder(); }

        public static class Builder {
            private int workers = Math.max(2, Runtime.getRuntime().availableProcessors());
            private int queueCapacity = 15_000;
            private int batchLimit = 2_000;
            private boolean computeHash = true;
            private long hashMaxBytes = 200L * 1024 * 1024; // 200MB
            private boolean followLinks = false;
            private final List<String> excludes = new ArrayList<>();
            private long preloadIndexMaxRows = 2_000_000;
            private long dbRetryBaseMs = 50;
            private int dbMaxRetries = 6;
            private long writerPollSleepMs = 8;
            private long issueOfferTimeoutMs = 50;

            private int lruCacheSize = 120_000;
            private boolean enableDbLookupOnMiss = true;

            public Builder workers(int v) { this.workers = v; return this; }
            public Builder queueCapacity(int v) { this.queueCapacity = v; return this; }
            public Builder batchLimit(int v) { this.batchLimit = v; return this; }
            public Builder computeHash(boolean v) { this.computeHash = v; return this; }
            public Builder hashMaxBytes(long v) { this.hashMaxBytes = v; return this; }
            public Builder followLinks(boolean v) { this.followLinks = v; return this; }
            public Builder preloadIndexMaxRows(long v) { this.preloadIndexMaxRows = v; return this; }
            public Builder dbRetryBaseMs(long v) { this.dbRetryBaseMs = v; return this; }
            public Builder dbMaxRetries(int v) { this.dbMaxRetries = v; return this; }
            public Builder writerPollSleepMs(long v) { this.writerPollSleepMs = v; return this; }
            public Builder issueOfferTimeoutMs(long v) { this.issueOfferTimeoutMs = v; return this; }

            public Builder lruCacheSize(int v) { this.lruCacheSize = v; return this; }
            public Builder enableDbLookupOnMiss(boolean v) { this.enableDbLookupOnMiss = v; return this; }

            public Builder addExclude(String glob) { this.excludes.add(glob); return this; }

            public ScanConfig build() {
                return new ScanConfig(
                        workers, queueCapacity, batchLimit,
                        computeHash, hashMaxBytes,
                        followLinks, List.copyOf(excludes),
                        preloadIndexMaxRows, dbRetryBaseMs, dbMaxRetries,
                        writerPollSleepMs, issueOfferTimeoutMs,
                        lruCacheSize, enableDbLookupOnMiss
                );
            }
        }
    }

    // ==================================================================================
    // 3) METRICS (inclui contadores por status)
    // ==================================================================================
    static class ScanMetrics {
        final LongAdder filesScanned = new LongAdder();
        final LongAdder filesIgnored = new LongAdder();
        final LongAdder filesHashed = new LongAdder();
        final LongAdder bytesScanned = new LongAdder();
        final LongAdder bytesHashed = new LongAdder();
        final LongAdder dirsVisited = new LongAdder();
        final LongAdder dirsFailed = new LongAdder();
        final LongAdder dirsSkipped = new LongAdder();

        final LongAdder errorsWalk = new LongAdder();
        final LongAdder errorsHash = new LongAdder();

        final LongAdder skippedSize = new LongAdder();
        final LongAdder skippedDisabled = new LongAdder();

        final LongAdder dbRetries = new LongAdder();
        final LongAdder issuesDropped = new LongAdder();

        // Summary counts
        final LongAdder sNew = new LongAdder();
        final LongAdder sModified = new LongAdder();
        final LongAdder sMoved = new LongAdder();
        final LongAdder sUnchanged = new LongAdder();
        final LongAdder sHashFailed = new LongAdder();
        final LongAdder sSkippedSize = new LongAdder();
        final LongAdder sSkippedDisabled = new LongAdder();
        final LongAdder deletions = new LongAdder();

        final LongAdder dbLookupHits = new LongAdder();
        final LongAdder dbLookupMiss = new LongAdder();

        final Instant start = Instant.now();
        private final AtomicBoolean running = new AtomicBoolean(true);

        void startReporter(long totalFilesEstimate) {
            Thread t = new Thread(() -> {
                while (running.get()) {
                    printProgressBar(totalFilesEstimate);
                    try { Thread.sleep(1500); } catch (InterruptedException e) { break; }
                }
                printProgressBar(totalFilesEstimate);
                System.err.println();
            });
            t.setDaemon(true);
            t.start();
        }

        void stop() { running.set(false); }

        private void printProgressBar(long estimate) {
            long count = filesScanned.sum();
            long bytes = bytesScanned.sum();
            long hashedBytes = bytesHashed.sum();

            Duration d = Duration.between(start, Instant.now());
            double sec = Math.max(1.0, d.toMillis() / 1000.0);

            double rateFiles = count / sec;
            double rateMbScan = (bytes / 1024.0 / 1024.0) / sec;
            double rateMbHash = (hashedBytes / 1024.0 / 1024.0) / sec;

            String eta = (estimate > 0 && rateFiles > 0)
                    ? formatDuration(Duration.ofSeconds((long) ((estimate - count) / rateFiles)))
                    : "...";

            System.err.print(String.format(
                    "\r\033[36m[SCANNING]\033[0m Files: %,d | Ign: %,d | Size: %s | Scan: %.1f MB/s | Hash: %.1f MB/s | Err(W/H): %d/%d | Skip(S/D): %d/%d | ETA: %s      ",
                    count,
                    filesIgnored.sum(),
                    formatBytes(bytes),
                    rateMbScan,
                    rateMbHash,
                    errorsWalk.sum(),
                    errorsHash.sum(),
                    skippedSize.sum(),
                    skippedDisabled.sum(),
                    eta
            ));
        }

        private static String formatBytes(long bytes) {
            if (bytes < 1024) return bytes + " B";
            int exp = (int) (Math.log(bytes) / Math.log(1024));
            String pre = "KMGTPE".charAt(exp - 1) + "";
            return String.format("%.1f %sB", bytes / Math.pow(1024, exp), pre);
        }

        private static String formatDuration(Duration d) {
            long s = Math.max(0, d.getSeconds());
            return String.format("%02d:%02d:%02d", s / 3600, (s % 3600) / 60, s % 60);
        }
    }

    // ==================================================================================
    // 4) INDEX LOOKUP (Hybrid: Memory + DB + LRU)
    // ==================================================================================
    interface IndexLookup {
        PrevInfo get(String key);
        boolean isTruncated();
    }

    static final class LruCache<K,V> extends LinkedHashMap<K,V> {
        private final int max;
        LruCache(int max) { super(16, 0.75f, true); this.max = max; }
        @Override protected boolean removeEldestEntry(Map.Entry<K, V> eldest) { return size() > max; }
    }

    static final class HybridIndexLookup implements IndexLookup {
        private final Map<String, PrevInfo> mem;
        private final boolean truncated;
        private final ScanConfig cfg;
        private final ScanMetrics metrics;
        private final String dbUrl;
        private final String rootPath;

        // LRU per worker thread (evita lock no map global)
        private final ThreadLocal<Map<String, PrevInfo>> lruLocal;
        private final ThreadLocal<Connection> roConn;
        private final ThreadLocal<PreparedStatement> psLookup;

        HybridIndexLookup(Map<String, PrevInfo> mem,
                          boolean truncated,
                          ScanConfig cfg,
                          ScanMetrics metrics,
                          String dbUrl,
                          String rootPath) {
            this.mem = mem;
            this.truncated = truncated;
            this.cfg = cfg;
            this.metrics = metrics;
            this.dbUrl = dbUrl;
            this.rootPath = rootPath;

            this.lruLocal = ThreadLocal.withInitial(() -> Collections.synchronizedMap(new LruCache<>(cfg.lruCacheSize())));

            this.roConn = ThreadLocal.withInitial(() -> {
                try {
                    Connection c = DriverManager.getConnection(dbUrl);
                    c.setReadOnly(true);
                    c.setAutoCommit(true);
                    try (Statement st = c.createStatement()) {
                        st.execute("PRAGMA busy_timeout=5000;");
                        st.execute("PRAGMA foreign_keys=ON;");
                    }
                    return c;
                } catch (SQLException e) {
                    throw new RuntimeException("Failed to open read-only sqlite connection: " + e.getMessage(), e);
                }
            });

            this.psLookup = ThreadLocal.withInitial(() -> {
                try {
                    Connection c = roConn.get();
                    return c.prepareStatement("""
                        SELECT fs.path_id, p.full_path,
                               fs.size_bytes, fs.modified_at, fs.content_hash, fs.content_algo, fs.hash_status
                        FROM file_state fs
                        JOIN path p ON p.id = fs.path_id
                        WHERE fs.root_path=? AND fs.identity_type=? AND fs.identity_value=?
                        LIMIT 1
                    """);
                } catch (SQLException e) {
                    throw new RuntimeException("Failed to prepare lookup statement: " + e.getMessage(), e);
                }
            });
        }

        @Override
        public PrevInfo get(String key) {
            PrevInfo p = mem.get(key);
            if (p != null) return p;

            if (!cfg.enableDbLookupOnMiss()) return null;

            // LRU
            PrevInfo cached = lruLocal.get().get(key);
            if (cached != null) {
                metrics.dbLookupHits.increment();
                return cached;
            }

            // DB lookup on miss
            String[] parts = key.split(":", 2);
            if (parts.length != 2) return null;
            String it = parts[0];
            String iv = parts[1];

            try {
                PreparedStatement ps = psLookup.get();
                ps.setString(1, rootPath);
                ps.setString(2, it);
                ps.setString(3, iv);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        PrevInfo pi = new PrevInfo(
                                rs.getLong(1),
                                rs.getString(2),
                                rs.getLong(3),
                                rs.getString(4),
                                rs.getString(5),
                                rs.getString(6),
                                rs.getString(7)
                        );
                        lruLocal.get().put(key, pi);
                        metrics.dbLookupHits.increment();
                        return pi;
                    }
                }
                metrics.dbLookupMiss.increment();
                return null;
            } catch (SQLException e) {
                // best-effort: se lookup falhar, trata como null
                return null;
            }
        }

        @Override public boolean isTruncated() { return truncated; }

        void closeThreadLocals() {
            // opcional: deixar o GC fechar, mas se quiser chamar manualmente no final do worker:
            try { PreparedStatement ps = psLookup.get(); ps.close(); } catch (Exception ignored) {}
            try { Connection c = roConn.get(); c.close(); } catch (Exception ignored) {}
        }
    }

    // ==================================================================================
    // 5) FILE PROCESSOR (Worker brain)
    // ==================================================================================
    static final class FileProcessor {
        private final ScanConfig cfg;
        private final IndexLookup index;
        private final ScanMetrics metrics;

        FileProcessor(ScanConfig cfg, IndexLookup index, ScanMetrics metrics) {
            this.cfg = cfg;
            this.index = index;
            this.metrics = metrics;
        }

        FileResult process(FileMeta current) {
            String key = current.identityType() + ":" + current.identityValue();
            PrevInfo prev = index.get(key);

            // NEW
            if (prev == null) {
                HashOutcome ho = maybeHash(current);
                FileStatus st = ho.statusOverride != null ? ho.statusOverride : FileStatus.NEW;
                trackStatus(st);
                return new FileResult(current, st, ho.algo, ho.hash, ho.hashStatus, ho.reasonOverride != null ? ho.reasonOverride : "NEW_FILE", false);
            }

            boolean metaChanged = prev.sizeBytes() != current.sizeBytes()
                    || !Objects.equals(prev.modifiedAt(), current.modifiedAt());

            boolean pathChanged = prev.knownPath() != null
                    && !prev.knownPath().equalsIgnoreCase(current.fullPath());

            if (metaChanged) {
                HashOutcome ho = maybeHash(current);
                FileStatus st = ho.statusOverride != null ? ho.statusOverride : FileStatus.MODIFIED;
                trackStatus(st);
                return new FileResult(current, st, ho.algo, ho.hash, ho.hashStatus, ho.reasonOverride != null ? ho.reasonOverride : "META_CHANGED", false);
            }

            if (pathChanged) {
                // MOVED: conteúdo igual => reaproveita hash anterior
                trackStatus(FileStatus.MOVED);
                HashStatus hs = parseHashStatus(prev.hashStatus());
                return new FileResult(current, FileStatus.MOVED, prev.contentAlgo(), prev.contentHash(), hs, "PATH_CHANGED", false);
            }

            // UNCHANGED: touch only
            trackStatus(FileStatus.UNCHANGED);
            HashStatus hs = parseHashStatus(prev.hashStatus());
            return new FileResult(current, FileStatus.UNCHANGED, prev.contentAlgo(), prev.contentHash(), hs, null, false);
        }

        private void trackStatus(FileStatus st) {
            switch (st) {
                case NEW -> metrics.sNew.increment();
                case MODIFIED -> metrics.sModified.increment();
                case MOVED -> metrics.sMoved.increment();
                case UNCHANGED -> metrics.sUnchanged.increment();
                case HASH_FAILED -> metrics.sHashFailed.increment();
                case SKIPPED_SIZE -> metrics.sSkippedSize.increment();
                case SKIPPED_DISABLED -> metrics.sSkippedDisabled.increment();
            }
        }

        record HashOutcome(String algo, String hash, HashStatus hashStatus, FileStatus statusOverride, String reasonOverride) {}

        private HashOutcome maybeHash(FileMeta m) {
            if (!cfg.computeHash()) {
                metrics.skippedDisabled.increment();
                return new HashOutcome(null, null, HashStatus.DISABLED, FileStatus.SKIPPED_DISABLED, "HASH_DISABLED");
            }
            if (cfg.hashMaxBytes() > 0 && m.sizeBytes() > cfg.hashMaxBytes()) {
                metrics.skippedSize.increment();
                return new HashOutcome(null, null, HashStatus.SKIPPED_SIZE, FileStatus.SKIPPED_SIZE, "TOO_LARGE");
            }

            try {
                HashResult hr = calculateHashSha256(Path.of(m.fullPath()));
                metrics.filesHashed.increment();
                metrics.bytesHashed.add(hr.bytesRead);
                return new HashOutcome("SHA-256", hr.hex, HashStatus.OK, null, null);
            } catch (IOException e) {
                metrics.errorsHash.increment();
                return new HashOutcome(null, null, HashStatus.FAILED, FileStatus.HASH_FAILED, "HASH_IO_FAIL");
            }
        }

        private HashStatus parseHashStatus(String s) {
            if (s == null) return HashStatus.NONE;
            try { return HashStatus.valueOf(s); }
            catch (Exception e) { return HashStatus.NONE; }
        }
    }

    // ==================================================================================
    // 6) DB ADAPTER (Single-writer)
    // ==================================================================================
    static final class DbAdapter implements AutoCloseable {
        private final Connection conn;
        private final ScanConfig cfg;
        private final ScanMetrics metrics;

        private final PreparedStatement psPathUpsert;
        private final PreparedStatement psPathSelect;

        private final PreparedStatement psContentUpsert;

        private final PreparedStatement psFileStateUpsert;
        private final PreparedStatement psTouch;

        private final PreparedStatement psChangeLog;
        private final PreparedStatement psIssueInsert;

        private final PreparedStatement psSummaryUpsert;

        private final Map<String, Long> pathCache = new HashMap<>();
        private final List<FileResult> fileBatch = new ArrayList<>();
        private final List<ScanIssue> issueBatch = new ArrayList<>();

        DbAdapter(Connection conn, ScanConfig cfg, ScanMetrics metrics) throws SQLException {
            this.conn = conn;
            this.cfg = cfg;
            this.metrics = metrics;

            executePragmas(conn);
            conn.setAutoCommit(false);

            psPathUpsert = conn.prepareStatement("""
                    INSERT INTO path(full_path) VALUES(?)
                    ON CONFLICT(full_path) DO NOTHING
            """);
            psPathSelect = conn.prepareStatement("SELECT id FROM path WHERE full_path=?");

            psContentUpsert = conn.prepareStatement("""
                    INSERT INTO content(algo, hash_hex, size_bytes)
                    VALUES(?,?,?)
                    ON CONFLICT(algo, hash_hex) DO NOTHING
            """);

            psFileStateUpsert = conn.prepareStatement("""
                INSERT INTO file_state(root_path, identity_type, identity_value,
                                       path_id, name, size_bytes, created_at, modified_at,
                                       file_key, content_algo, content_hash, hash_status, last_scan_id)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(root_path, identity_type, identity_value) DO UPDATE SET
                    path_id      = excluded.path_id,
                    name         = excluded.name,
                    size_bytes   = excluded.size_bytes,
                    created_at   = excluded.created_at,
                    modified_at  = excluded.modified_at,
                    file_key     = excluded.file_key,
                    content_algo = excluded.content_algo,
                    content_hash = excluded.content_hash,
                    hash_status  = excluded.hash_status,
                    last_scan_id = excluded.last_scan_id
            """);

            psTouch = conn.prepareStatement("""
                UPDATE file_state
                SET last_scan_id = ?
                WHERE root_path=? AND identity_type=? AND identity_value=?
            """);

            psChangeLog = conn.prepareStatement("""
                INSERT INTO file_change(root_path, identity_type, identity_value,
                                        scan_id, size_bytes, modified_at, content_algo, content_hash, reason)
                VALUES(?,?,?,?,?,?,?,?,?)
            """);

            psIssueInsert = conn.prepareStatement("""
                INSERT INTO scan_issue(scan_id, stage, path,
                                       identity_type, identity_value,
                                       error_type, message, rule, created_at)
                VALUES(?,?,?,?,?,?,?,?,?)
            """);

            psSummaryUpsert = conn.prepareStatement("""
                INSERT INTO scan_summary(scan_id, root_path, started_at, finished_at,
                                         files_total, bytes_scanned, bytes_hashed,
                                         new_count, modified_count, moved_count, unchanged_count, deleted_count,
                                         walk_errors, hash_errors, skipped_size, skipped_disabled,
                                         db_retries, issues_dropped, db_lookup_hits, db_lookup_miss)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(scan_id) DO UPDATE SET
                    finished_at=excluded.finished_at,
                    files_total=excluded.files_total,
                    bytes_scanned=excluded.bytes_scanned,
                    bytes_hashed=excluded.bytes_hashed,
                    new_count=excluded.new_count,
                    modified_count=excluded.modified_count,
                    moved_count=excluded.moved_count,
                    unchanged_count=excluded.unchanged_count,
                    deleted_count=excluded.deleted_count,
                    walk_errors=excluded.walk_errors,
                    hash_errors=excluded.hash_errors,
                    skipped_size=excluded.skipped_size,
                    skipped_disabled=excluded.skipped_disabled,
                    db_retries=excluded.db_retries,
                    issues_dropped=excluded.issues_dropped,
                    db_lookup_hits=excluded.db_lookup_hits,
                    db_lookup_miss=excluded.db_lookup_miss
            """);
        }

        static void executePragmas(Connection conn) throws SQLException {
                boolean prevAuto = true;
                try {
                    try {
                        prevAuto = conn.getAutoCommit();
                    } catch (Exception ignored) {}

                    if (!prevAuto) {
                        try { conn.commit(); } catch (Exception ignored) {}
                        try { conn.setAutoCommit(true); } catch (Exception ignored) {}
                    }

                    try (Statement st = conn.createStatement()) {
                        try {
                            st.execute("PRAGMA journal_mode=WAL;");
                        } catch (SQLException e) {
                            String msg = e.getMessage() != null ? e.getMessage() : "";
                            if (!msg.toLowerCase(Locale.ROOT).contains("safety")) throw e;
                            // ignore safety-level change errors (cannot switch journal mode inside transaction)
                        }
                        try { st.execute("PRAGMA synchronous=NORMAL;"); } catch (SQLException ignored) {}
                        try { st.execute("PRAGMA busy_timeout=5000;"); } catch (SQLException ignored) {}
                        try { st.execute("PRAGMA foreign_keys=ON;"); } catch (SQLException ignored) {}
                    }
                } finally {
                    if (!prevAuto) {
                        try { conn.setAutoCommit(false); } catch (Exception ignored) {}
                    }
                }
        }

        void addFile(FileResult r, long scanId) throws SQLException {
            fileBatch.add(r);
            if (fileBatch.size() >= cfg.batchLimit()) flushFiles(scanId);
        }

        void addIssue(ScanIssue issue, long scanId) throws SQLException {
            issueBatch.add(issue);
            if (issueBatch.size() >= cfg.batchLimit()) flushIssues(scanId);
        }

        void flushAll(long scanId) throws SQLException {
            flushFiles(scanId);
            flushIssues(scanId);
        }

        void flushFiles(long scanId) throws SQLException {
            if (fileBatch.isEmpty()) return;
            List<FileResult> items = new ArrayList<>(fileBatch);
            fileBatch.clear();

            runWithRetry(() -> {
                executeFileBatch(scanId, items);
                conn.commit();
            });
        }

        void flushIssues(long scanId) throws SQLException {
            if (issueBatch.isEmpty()) return;
            List<ScanIssue> items = new ArrayList<>(issueBatch);
            issueBatch.clear();

            runWithRetry(() -> {
                String now = Instant.now().toString();
                for (ScanIssue i : items) {
                    psIssueInsert.setLong(1, scanId);
                    psIssueInsert.setString(2, i.stage().name());
                    psIssueInsert.setString(3, i.path());
                    psIssueInsert.setString(4, i.identityType());
                    psIssueInsert.setString(5, i.identityValue());
                    psIssueInsert.setString(6, i.errorType());
                    psIssueInsert.setString(7, i.message());
                    psIssueInsert.setString(8, i.rule());
                    psIssueInsert.setString(9, now);
                    psIssueInsert.addBatch();
                }
                psIssueInsert.executeBatch();
                conn.commit();
            });
        }

        private void executeFileBatch(long scanId, List<FileResult> items) throws SQLException {
            for (FileResult res : items) {
                if (res.poison()) continue;

                if (res.status() == FileStatus.UNCHANGED) {
                    psTouch.setLong(1, scanId);
                    psTouch.setString(2, res.meta().rootPath());
                    psTouch.setString(3, res.meta().identityType());
                    psTouch.setString(4, res.meta().identityValue());
                    psTouch.addBatch();
                    continue;
                }

                long pathId = getOrCreatePathId(res.meta().fullPath());

                if (res.contentAlgo() != null && res.contentHash() != null) {
                    psContentUpsert.setString(1, res.contentAlgo());
                    psContentUpsert.setString(2, res.contentHash());
                    psContentUpsert.setLong(3, res.meta().sizeBytes());
                    psContentUpsert.addBatch();
                }

                psFileStateUpsert.setString(1, res.meta().rootPath());
                psFileStateUpsert.setString(2, res.meta().identityType());
                psFileStateUpsert.setString(3, res.meta().identityValue());
                psFileStateUpsert.setLong(4, pathId);
                psFileStateUpsert.setString(5, res.meta().name());
                psFileStateUpsert.setLong(6, res.meta().sizeBytes());
                psFileStateUpsert.setString(7, res.meta().createdAt());
                psFileStateUpsert.setString(8, res.meta().modifiedAt());
                psFileStateUpsert.setString(9, res.meta().fileKey());
                psFileStateUpsert.setString(10, res.contentAlgo());
                psFileStateUpsert.setString(11, res.contentHash());
                psFileStateUpsert.setString(12, res.hashStatus() != null ? res.hashStatus().name() : HashStatus.NONE.name());
                psFileStateUpsert.setLong(13, scanId);
                psFileStateUpsert.addBatch();

                // Histórico: tudo que não é UNCHANGED
                psChangeLog.setString(1, res.meta().rootPath());
                psChangeLog.setString(2, res.meta().identityType());
                psChangeLog.setString(3, res.meta().identityValue());
                psChangeLog.setLong(4, scanId);
                psChangeLog.setLong(5, res.meta().sizeBytes());
                psChangeLog.setString(6, res.meta().modifiedAt());
                psChangeLog.setString(7, res.contentAlgo());
                psChangeLog.setString(8, res.contentHash());
                psChangeLog.setString(9, (res.reason() != null ? res.status().name() + ":" + res.reason() : res.status().name()));
                psChangeLog.addBatch();
            }

            psTouch.executeBatch();
            psContentUpsert.executeBatch();
            psFileStateUpsert.executeBatch();
            psChangeLog.executeBatch();
        }

        void upsertSummary(long scanId, String rootPath, String startedAt, String finishedAt) throws SQLException {
            runWithRetry(() -> {
                psSummaryUpsert.setLong(1, scanId);
                psSummaryUpsert.setString(2, rootPath);
                psSummaryUpsert.setString(3, startedAt);
                psSummaryUpsert.setString(4, finishedAt);

                psSummaryUpsert.setLong(5, metrics.filesScanned.sum());
                psSummaryUpsert.setLong(6, metrics.bytesScanned.sum());
                psSummaryUpsert.setLong(7, metrics.bytesHashed.sum());

                psSummaryUpsert.setLong(8, metrics.sNew.sum());
                psSummaryUpsert.setLong(9, metrics.sModified.sum());
                psSummaryUpsert.setLong(10, metrics.sMoved.sum());
                psSummaryUpsert.setLong(11, metrics.sUnchanged.sum());
                psSummaryUpsert.setLong(12, metrics.deletions.sum());

                psSummaryUpsert.setLong(13, metrics.errorsWalk.sum());
                psSummaryUpsert.setLong(14, metrics.errorsHash.sum());
                psSummaryUpsert.setLong(15, metrics.skippedSize.sum());
                psSummaryUpsert.setLong(16, metrics.skippedDisabled.sum());

                psSummaryUpsert.setLong(17, metrics.dbRetries.sum());
                psSummaryUpsert.setLong(18, metrics.issuesDropped.sum());
                psSummaryUpsert.setLong(19, metrics.dbLookupHits.sum());
                psSummaryUpsert.setLong(20, metrics.dbLookupMiss.sum());

                psSummaryUpsert.executeUpdate();
                conn.commit();
            });
        }

        private long getOrCreatePathId(String fullPath) throws SQLException {
            Long cached = pathCache.get(fullPath);
            if (cached != null) return cached;

            psPathUpsert.setString(1, fullPath);
            psPathUpsert.executeUpdate();

            psPathSelect.setString(1, fullPath);
            try (ResultSet rs = psPathSelect.executeQuery()) {
                if (rs.next()) {
                    long id = rs.getLong(1);
                    if (pathCache.size() < 120_000) pathCache.put(fullPath, id);
                    return id;
                }
            }
            throw new SQLException("Failed to resolve path_id for: " + fullPath);
        }

        @FunctionalInterface interface RunnableSql { void run() throws SQLException; }

        private void runWithRetry(RunnableSql task) throws SQLException {
            int attempt = 0;
            while (true) {
                try {
                    task.run();
                    return;
                } catch (SQLException e) {
                    boolean busy = (e.getErrorCode() == 5) || (e.getMessage() != null && e.getMessage().toLowerCase().contains("busy"));
                    if (!busy) throw e;

                    attempt++;
                    if (attempt > cfg.dbMaxRetries()) throw e;

                    metrics.dbRetries.increment();
                    try { Thread.sleep(cfg.dbRetryBaseMs() * (1L << attempt)); }
                    catch (InterruptedException ie) { Thread.currentThread().interrupt(); throw e; }
                }
            }
        }

        @Override public void close() throws SQLException {
            psPathUpsert.close();
            psPathSelect.close();
            psContentUpsert.close();
            psFileStateUpsert.close();
            psTouch.close();
            psChangeLog.close();
            psIssueInsert.close();
            psSummaryUpsert.close();
        }
    }

    // ==================================================================================
    // 7) SCHEMA + SCAN ROWS
    // ==================================================================================
    static void initSchema(Connection conn) throws SQLException {
        List<String> ddl = List.of(
                """
                CREATE TABLE IF NOT EXISTS scan (
                  id INTEGER PRIMARY KEY,
                  root_path TEXT,
                  started_at TEXT,
                  finished_at TEXT,
                  status TEXT,
                  error_message TEXT
                )
                """,
                """
                CREATE TABLE IF NOT EXISTS path (
                  id INTEGER PRIMARY KEY,
                  full_path TEXT UNIQUE
                )
                """,
                """
                CREATE TABLE IF NOT EXISTS content (
                  algo TEXT,
                  hash_hex TEXT,
                  size_bytes INTEGER,
                  PRIMARY KEY(algo, hash_hex)
                )
                """,
                """
                CREATE TABLE IF NOT EXISTS file_state (
                  root_path TEXT,
                  identity_type TEXT,
                  identity_value TEXT,
                  path_id INTEGER,
                  name TEXT,
                  size_bytes INTEGER,
                  created_at TEXT,
                  modified_at TEXT,
                  file_key TEXT,
                  content_algo TEXT,
                  content_hash TEXT,
                  hash_status TEXT,
                  last_scan_id INTEGER,
                  PRIMARY KEY(root_path, identity_type, identity_value),
                  FOREIGN KEY(path_id) REFERENCES path(id)
                )
                """,
                """
                CREATE TABLE IF NOT EXISTS file_change (
                  id INTEGER PRIMARY KEY,
                  root_path TEXT,
                  identity_type TEXT,
                  identity_value TEXT,
                  scan_id INTEGER,
                  size_bytes INTEGER,
                  modified_at TEXT,
                  content_algo TEXT,
                  content_hash TEXT,
                  reason TEXT
                )
                """,
                """
                CREATE TABLE IF NOT EXISTS scan_issue (
                  id INTEGER PRIMARY KEY,
                  scan_id INTEGER,
                  stage TEXT,
                  path TEXT,
                  identity_type TEXT,
                  identity_value TEXT,
                  error_type TEXT,
                  message TEXT,
                  rule TEXT,
                  created_at TEXT
                )
                """,
                """
                CREATE TABLE IF NOT EXISTS scan_summary (
                  scan_id INTEGER PRIMARY KEY,
                  root_path TEXT,
                  started_at TEXT,
                  finished_at TEXT,
                  files_total INTEGER,
                  bytes_scanned INTEGER,
                  bytes_hashed INTEGER,
                  new_count INTEGER,
                  modified_count INTEGER,
                  moved_count INTEGER,
                  unchanged_count INTEGER,
                  deleted_count INTEGER,
                  walk_errors INTEGER,
                  hash_errors INTEGER,
                  skipped_size INTEGER,
                  skipped_disabled INTEGER,
                  db_retries INTEGER,
                  issues_dropped INTEGER,
                  db_lookup_hits INTEGER,
                  db_lookup_miss INTEGER
                )
                """,
                "CREATE INDEX IF NOT EXISTS idx_file_state_root_lastscan ON file_state(root_path, last_scan_id)",
                "CREATE INDEX IF NOT EXISTS idx_issue_scan_stage ON scan_issue(scan_id, stage)",
                "CREATE INDEX IF NOT EXISTS idx_change_scan ON file_change(scan_id)",
                "CREATE INDEX IF NOT EXISTS idx_path_full ON path(full_path)",
                "CREATE INDEX IF NOT EXISTS idx_file_state_path ON file_state(path_id)"
        );

        try (Statement st = conn.createStatement()) {
            for (String sql : ddl) st.execute(sql);
        }
    }

    static long startScan(Connection conn, String root) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO scan(root_path, started_at, status) VALUES(?, ?, 'RUNNING')",
                Statement.RETURN_GENERATED_KEYS
        )) {
            ps.setString(1, root);
            ps.setString(2, Instant.now().toString());
            ps.executeUpdate();
            try (ResultSet rs = ps.getGeneratedKeys()) {
                rs.next();
                return rs.getLong(1);
            }
        }
    }

    static String getScanStartedAt(Connection conn, long scanId) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("SELECT started_at FROM scan WHERE id=?")) {
            ps.setLong(1, scanId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) return rs.getString(1);
            }
        }
        return Instant.now().toString();
    }

    static void finishScan(Connection conn, long id, String status, String error) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "UPDATE scan SET finished_at=?, status=?, error_message=? WHERE id=?"
        )) {
            ps.setString(1, Instant.now().toString());
            ps.setString(2, status);
            ps.setString(3, error);
            ps.setLong(4, id);
            ps.executeUpdate();
        }
    }

    static final class LoadedIndex {
        final Map<String, PrevInfo> map;
        final boolean truncated;
        LoadedIndex(Map<String, PrevInfo> map, boolean truncated) { this.map = map; this.truncated = truncated; }
    }

    static LoadedIndex loadIndex(Connection conn, String rootPath, long maxRows) throws SQLException {
        Map<String, PrevInfo> idx = new HashMap<>();
        boolean truncated = false;

        try (PreparedStatement ps = conn.prepareStatement("""
            SELECT fs.identity_type, fs.identity_value,
                   fs.path_id, p.full_path,
                   fs.size_bytes, fs.modified_at, fs.content_hash, fs.content_algo, fs.hash_status
            FROM file_state fs
            JOIN path p ON p.id = fs.path_id
            WHERE fs.root_path=?
            LIMIT ?
        """)) {
            ps.setString(1, rootPath);
            ps.setLong(2, maxRows);

            long count = 0;
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String it = rs.getString(1);
                    String iv = rs.getString(2);
                    long pathId = rs.getLong(3);
                    String knownPath = rs.getString(4);
                    long size = rs.getLong(5);
                    String modAt = rs.getString(6);
                    String hash = rs.getString(7);
                    String algo = rs.getString(8);
                    String hs = rs.getString(9);

                    idx.put(it + ":" + iv, new PrevInfo(pathId, knownPath, size, modAt, hash, algo, hs));
                    count++;
                }
            }
            if (count >= maxRows) truncated = true;
        }
        return new LoadedIndex(idx, truncated);
    }

    static int handleDeletions(Connection conn, long scanId, String rootPath) throws SQLException {
        int deletedCount;

        try (PreparedStatement ps = conn.prepareStatement("""
            INSERT INTO file_change(root_path, identity_type, identity_value, scan_id, size_bytes, modified_at, content_algo, content_hash, reason)
            SELECT root_path, identity_type, identity_value, ?, size_bytes, modified_at, content_algo, content_hash, 'DELETED'
            FROM file_state
            WHERE root_path=? AND last_scan_id < ?
        """)) {
            ps.setLong(1, scanId);
            ps.setString(2, rootPath);
            ps.setLong(3, scanId);
            ps.executeUpdate();
        }

        try (PreparedStatement ps = conn.prepareStatement("""
            DELETE FROM file_state
            WHERE root_path=? AND last_scan_id < ?
        """)) {
            ps.setString(1, rootPath);
            ps.setLong(2, scanId);
            deletedCount = ps.executeUpdate();
        }

        conn.commit();
        return deletedCount;
    }

    // ==================================================================================
    // 8) ENGINE (Orchestrator)
    // ==================================================================================
    public static void runScan(String dbUrl, Connection writerConn, Path root, ScanConfig cfg) throws Exception {
        ScanMetrics metrics = new ScanMetrics();
        initSchema(writerConn);

        Path rootAbs = root.toAbsolutePath().normalize();
        String rootPath = rootAbs.toString();

        // validações (inclui "caminho com espaço" -> orientação)
        if (!Files.exists(rootAbs)) {
            logJson("Orchestrator", "ERROR", "Root path does not exist", kv(
                "root", rootPath,
                "hint", "Se o caminho tiver espaço, use aspas no terminal: \"C:\\\\Games\\\\Fall Guys\""
            ));
            return;
        }

        long scanId = startScan(writerConn, rootPath);
        String startedAt = getScanStartedAt(writerConn, scanId);

        logJson("Benchmark", "INFO", "Benchmark started", kv(
            "root", rootPath,
            "db", dbUrl,
            "os", System.getProperty("os.name"),
            "hash_max_bytes", cfg.hashMaxBytes(),
            "preload_index_max_rows", cfg.preloadIndexMaxRows(),
            "workers", cfg.workers()
        ));

        LoadedIndex loaded = loadIndex(writerConn, rootPath, cfg.preloadIndexMaxRows());
        HybridIndexLookup index = new HybridIndexLookup(loaded.map, loaded.truncated, cfg, metrics, dbUrl, rootPath);

        long estimate = Math.max(1000, loaded.map.size());
        metrics.startReporter(estimate);

        List<ExcludeRule> rules = cfg.excludes().stream().map(ExcludeRule::new).toList();

        BlockingQueue<FileMeta> inQ = new ArrayBlockingQueue<>(cfg.queueCapacity());
        BlockingQueue<FileResult> outQ = new ArrayBlockingQueue<>(cfg.queueCapacity());

        // Issue queue: best-effort (não pode travar scan)
        BlockingQueue<ScanIssue> issueQ = new ArrayBlockingQueue<>(Math.max(2_000, cfg.queueCapacity() / 5));

        AtomicBoolean abort = new AtomicBoolean(false);

        // Writer (single-writer)
        Thread writer = Thread.ofVirtual().name("writer").start(() -> {
            try (DbAdapter db = new DbAdapter(writerConn, cfg, metrics)) {
                int poisonCount = 0;
                while (!abort.get()) {
                    FileResult r = outQ.poll();
                    if (r != null) {
                        if (r.poison()) {
                            poisonCount++;
                            if (poisonCount >= cfg.workers()) break;
                        } else {
                            db.addFile(r, scanId);
                        }
                    }

                    ScanIssue si;
                    while ((si = issueQ.poll()) != null) {
                        db.addIssue(si, scanId);
                    }

                    if (r == null) {
                        try { Thread.sleep(cfg.writerPollSleepMs()); }
                        catch (InterruptedException e) { Thread.currentThread().interrupt(); break; }
                    }
                }

                db.flushAll(scanId);
            } catch (Exception e) {
                abort.set(true);
                e.printStackTrace();
            }
        });

        // Workers
        ExecutorService pool = Executors.newFixedThreadPool(cfg.workers(), Thread.ofVirtual().factory());
        for (int i = 0; i < cfg.workers(); i++) {
            pool.submit(() -> {
                FileProcessor processor = new FileProcessor(cfg, index, metrics);
                try {
                    while (!abort.get()) {
                        FileMeta m = inQ.poll(250, TimeUnit.MILLISECONDS);
                        if (m == null) continue;
                        if ("__POISON__".equals(m.identityType())) break;

                        if (isExcluded(m, rules, rootAbs, metrics)) continue;

                        FileResult res = processor.process(m);

                        if (res.status() == FileStatus.HASH_FAILED) {
                            offerIssue(issueQ, cfg, metrics, new ScanIssue(
                                    scanId, Stage.HASH, m.fullPath(),
                                    m.identityType(), m.identityValue(),
                                    "IOException", "HASH_FAILED (read error during hashing)", null
                            ));
                        }

                        if (!offerWithAbort(outQ, res, abort)) break;
                    }
                } catch (Exception e) {
                    abort.set(true);
                } finally {
                    // fecha conexão read-only desse worker (se você quiser forçar)
                    // index.closeThreadLocals(); // se usar por-thread explicitamente, chamaria aqui (mas é ThreadLocal; opcional)
                }
            });
        }

        // Walker
        EnumSet<FileVisitOption> opts = cfg.followLinks()
                ? EnumSet.of(FileVisitOption.FOLLOW_LINKS)
                : EnumSet.noneOf(FileVisitOption.class);

        Thread walker = Thread.ofVirtual().name("walker").start(() -> {
            try {
                Files.walkFileTree(rootAbs, opts, Integer.MAX_VALUE, new SimpleFileVisitor<Path>() {
                    @Override
                    public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
                        if (abort.get()) return FileVisitResult.TERMINATE;

                        metrics.dirsVisited.increment();

                        // Evita reparse points/symlinks no Windows (podem explodir a arvore).
                        if (isWindows() && Files.isSymbolicLink(dir)) {
                            metrics.dirsSkipped.increment();
                            return FileVisitResult.SKIP_SUBTREE;
                        }

                        if (!Files.isReadable(dir)) {
                            metrics.dirsFailed.increment();
                            metrics.errorsWalk.increment();
                            offerIssue(issueQ, cfg, metrics, new ScanIssue(
                                    scanId, Stage.WALK,
                                    dir.toAbsolutePath().normalize().toString(),
                                    null, null,
                                    "AccessDenied",
                                    "Directory not readable; skipping subtree",
                                    null
                            ));
                            return FileVisitResult.SKIP_SUBTREE;
                        }
                        return FileVisitResult.CONTINUE;
                    }

                    @Override
                    public FileVisitResult postVisitDirectory(Path dir, IOException exc) {
                        if (exc != null) {
                            metrics.dirsFailed.increment();
                            metrics.errorsWalk.increment();
                            offerIssue(issueQ, cfg, metrics, new ScanIssue(
                                    scanId, Stage.WALK,
                                    dir.toAbsolutePath().normalize().toString(),
                                    null, null,
                                    exc.getClass().getSimpleName(),
                                    String.valueOf(exc.getMessage()),
                                    null
                            ));
                        }
                        return FileVisitResult.CONTINUE;
                    }

                    @Override
                    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                        if (abort.get()) return FileVisitResult.TERMINATE;
                        if (!attrs.isRegularFile()) return FileVisitResult.CONTINUE;

                        metrics.filesScanned.increment();
                        metrics.bytesScanned.add(attrs.size());

                        String fk = (attrs.fileKey() != null) ? attrs.fileKey().toString() : null;
                        String full = file.toAbsolutePath().normalize().toString();

                        FileMeta meta = new FileMeta(
                                rootPath,
                                full,
                                file.getFileName().toString(),
                                attrs.size(),
                                safeInstant(attrs.creationTime()),
                                safeInstant(attrs.lastModifiedTime()),
                                fk,
                                (fk != null ? "FILE_KEY" : "PATH"),
                                (fk != null ? fk : full)
                        );

                        try {
                            if (!offerWithAbort(inQ, meta, abort)) return FileVisitResult.TERMINATE;
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            return FileVisitResult.TERMINATE;
                        }
                        return FileVisitResult.CONTINUE;
                    }

                    @Override
                    public FileVisitResult visitFileFailed(Path file, IOException exc) {
                        metrics.errorsWalk.increment();
                        offerIssue(issueQ, cfg, metrics, new ScanIssue(
                                scanId, Stage.WALK,
                                file.toAbsolutePath().normalize().toString(),
                                null, null,
                                exc.getClass().getSimpleName(),
                                String.valueOf(exc.getMessage()),
                                null
                        ));
                        return FileVisitResult.CONTINUE;
                    }
                });
            } catch (Exception e) {
                abort.set(true);
            } finally {
                for (int i = 0; i < cfg.workers(); i++) {
                    inQ.offer(new FileMeta(null, null, null, 0, null, null, null, "__POISON__", null));
                }
            }
        });

        logJson("Orchestrator", "INFO", "Scan started", kv("scan_id", scanId, "root", rootPath));

        // Shutdown
        walker.join();
        pool.shutdown();
        pool.awaitTermination(1, TimeUnit.DAYS);

        for (int i = 0; i < cfg.workers(); i++) outQ.put(new FileResult(null, null, null, null, null, null, true));
        writer.join();

        metrics.stop();

        // Deletions (somente se índice NÃO foi truncado)
        int deleted = 0;
        if (!loaded.truncated) {
            deleted = handleDeletions(writerConn, scanId, rootPath);
            metrics.deletions.add(deleted);
        } else {
                logJson("Orchestrator", "WARN", "Index truncated; skipping deletions to avoid false DELETED", kv(
                    "max_rows", cfg.preloadIndexMaxRows(),
                    "loaded", loaded.map.size()
                ));
        }

        finishScan(writerConn, scanId, "SUCCESS", null);
        writerConn.commit();

        String finishedAt = Instant.now().toString();

        // Summary row
        try (DbAdapter db = new DbAdapter(writerConn, cfg, metrics)) {
            db.upsertSummary(scanId, rootPath, startedAt, finishedAt);
        }

        Duration dur = Duration.between(metrics.start, Instant.now());
        logJson("Orchestrator", "INFO", "Scan completed", kv(
            "scan_id", scanId,
            "duration_sec", dur.toSeconds(),
            "files_total", metrics.filesScanned.sum(),
            "deleted", deleted,
            "new", metrics.sNew.sum(),
            "modified", metrics.sModified.sum(),
            "moved", metrics.sMoved.sum(),
            "unchanged", metrics.sUnchanged.sum(),
            "dirs_visited", metrics.dirsVisited.sum(),
            "dirs_failed", metrics.dirsFailed.sum(),
            "dirs_skipped", metrics.dirsSkipped.sum(),
            "errs_walk", metrics.errorsWalk.sum(),
            "errs_hash", metrics.errorsHash.sum(),
            "skipped_size", metrics.skippedSize.sum(),
            "skipped_disabled", metrics.skippedDisabled.sum(),
            "db_retries", metrics.dbRetries.sum(),
            "issues_dropped", metrics.issuesDropped.sum(),
            "db_lookup_hits", metrics.dbLookupHits.sum(),
            "db_lookup_miss", metrics.dbLookupMiss.sum()
        ));

        System.out.println("\n--- Ignore Statistics ---");
        for (ExcludeRule r : rules) {
            long c = r.count.sum();
            if (c > 0) System.out.printf("Rule [%s]: %,d files%n", r.glob, c);
        }
    }

    // ==================================================================================
    // 9) EXCLUDES + ISSUE HELPERS
    // ==================================================================================
    static boolean isExcluded(FileMeta m, List<ExcludeRule> rules, Path rootAbs, ScanMetrics metrics) {
        if (rules.isEmpty()) return false;

        Path abs = Path.of(m.fullPath());
        Path rel = relativizeOrName(rootAbs, abs);

        for (ExcludeRule r : rules) {
            if (r.matcher.matches(rel)) {
                r.count.increment();
                metrics.filesIgnored.increment();
                return true;
            }
        }
        return false;
    }

    static void offerIssue(BlockingQueue<ScanIssue> issueQ, ScanConfig cfg, ScanMetrics metrics, ScanIssue issue) {
        try {
            boolean ok = issueQ.offer(issue, cfg.issueOfferTimeoutMs(), TimeUnit.MILLISECONDS);
            if (!ok) metrics.issuesDropped.increment();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            metrics.issuesDropped.increment();
        }
    }

    static <T> boolean offerWithAbort(BlockingQueue<T> q, T o, AtomicBoolean abort) throws InterruptedException {
        while (!abort.get()) {
            if (q.offer(o, 100, TimeUnit.MILLISECONDS)) return true;
        }
        return false;
    }

    // ==================================================================================
    // 10) HASH
    // ==================================================================================
    record HashResult(String hex, long bytesRead) {}

    static HashResult calculateHashSha256(Path file) throws IOException {
        MessageDigest md;
        try { md = MessageDigest.getInstance("SHA-256"); }
        catch (Exception e) { throw new RuntimeException(e); }

        long total = 0;
        try (InputStream in = new BufferedInputStream(Files.newInputStream(file))) {
            byte[] buf = new byte[64 * 1024];
            int r;
            while ((r = in.read(buf)) != -1) {
                md.update(buf, 0, r);
                total += r;
            }
        }
        return new HashResult(HexFormat.of().formatHex(md.digest()), total);
    }

    // ==================================================================================
    // 11) UTILS
    // ==================================================================================
    static Path relativizeOrName(Path root, Path abs) {
        try {
            Path rel = root.relativize(abs);
            return rel != null ? rel : abs.getFileName();
        } catch (Exception e) {
            return abs.getFileName();
        }
    }

    static String safeInstant(FileTime t) {
        return t == null ? null : t.toInstant().toString();
    }

    // ==================================================================================
    // 12) MAIN
    // ==================================================================================
    public static void main(String[] args) throws Exception {
        ScanConfig cfg = ScanConfig.builder()
                .workers(Math.max(2, Runtime.getRuntime().availableProcessors()))
                .queueCapacity(15_000)
                .batchLimit(2_000)
                .computeHash(true)
                .hashMaxBytes(200L * 1024 * 1024)
                .preloadIndexMaxRows(2_000_000)
                .enableDbLookupOnMiss(true)
                .lruCacheSize(120_000)
                .addExclude("**/node_modules/**")
                .addExclude("**/.git/**")
                .addExclude("**/$Recycle.Bin/**")
                .addExclude("**/System Volume Information/**")
                .build();

        // Uso:
        // java LeitorDeArquivos "<root com espaço>" "<dbfile>"
        Path root = (args.length >= 1) ? Paths.get(args[0]) : Paths.get("C:\\");
        String dbFile = (args.length >= 2) ? args[1] : "scan-incremental.db";

        // Dica se estiver dando “trava” por espaço no caminho:
        // PowerShell/CMD: sempre use aspas.
        String dbUrl = "jdbc:sqlite:" + dbFile;

        try (LogTee tee = setupLogFile(Paths.get("logs"), "scan-benchmark");
             Connection writerConn = DriverManager.getConnection(dbUrl)) {
            runScan(dbUrl, writerConn, root, cfg);
        }
    }

    public static void runCli(String[] args) throws Exception {
        main(args);
    }
}
