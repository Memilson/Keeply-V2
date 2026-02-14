package com.keeply.springtest;

import jakarta.annotation.PreDestroy;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.sql.*;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;

@RestController
@CrossOrigin(origins = "*") // em prod, troque por origins específicos
@RequestMapping("/api/keeply")
public class KeeplyConnectionController {

    @Value("${keeply.cli.project-dir:}")
    private String keeplyProjectDir;

    @Value("${keeply.cli.jar:}")
    private String keeplyCliJar;

    @Value("${keeply.cli.timeout-seconds:1800}") // 30min default
    private int cliTimeoutSeconds;

    @Value("${keeply.cli.max-log-bytes:65536}") // 64KB default
    private int maxLogBytes;

    @Value("${keeply.db.file:keeply.db}")
    private String keeplyDbFile;

    private final ExecutorService jobPool = Executors.newFixedThreadPool(
            2,
            r -> {
                Thread t = new Thread(r, "keeply-scan-job");
                t.setDaemon(true);
                return t;
            }
    );

    private final ConcurrentMap<String, ScanJob> jobs = new ConcurrentHashMap<>();

    @PreDestroy
    public void shutdown() {
        jobPool.shutdownNow();
    }

    // ------------------------- endpoints -------------------------

    @GetMapping("/health")
    public ResponseEntity<?> health() {
        Path keeplyRoot = resolveKeeplyRootOrThrow();
        Path dbPath = keeplyRoot.resolve(keeplyDbFile).normalize();

        return ResponseEntity.ok(new HealthResponse(
                true,
                "keeply-api",
                Instant.now().toString(),
                keeplyRoot.toString(),
                dbPath.toString(),
                Files.exists(dbPath),
                resolveCliMode(keeplyRoot).mode(),
                resolveCliMode(keeplyRoot).commandPreview()
        ));
    }

    @GetMapping("/db-check")
    public ResponseEntity<?> dbCheck() {
        Path keeplyRoot = resolveKeeplyRootOrThrow();
        Path dbPath = keeplyRoot.resolve(keeplyDbFile).normalize();
        String dbUrl = "jdbc:sqlite:" + dbPath.toAbsolutePath();

        if (!Files.exists(dbPath)) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(ErrorResponse.of("db_not_found", "Arquivo de banco não encontrado", Map.of(
                            "keeplyRoot", keeplyRoot.toString(),
                            "dbPath", dbPath.toString(),
                            "dbUrl", dbUrl
                    )));
        }

        try (Connection conn = DriverManager.getConnection(dbUrl)) {
            // tabela existe?
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name='backup_history' LIMIT 1"
            );
                 ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                            .body(ErrorResponse.of("table_missing", "Tabela backup_history não existe neste banco", Map.of(
                                    "dbPath", dbPath.toString(),
                                    "dbUrl", dbUrl
                            )));
                }
            }

            long count;
            try (Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery("SELECT COUNT(*) AS total FROM backup_history")) {
                count = rs.next() ? rs.getLong("total") : -1;
            }

            return ResponseEntity.ok(Map.of(
                    "ok", true,
                    "keeplyRoot", keeplyRoot.toString(),
                    "dbPath", dbPath.toString(),
                    "dbUrl", dbUrl,
                    "backupHistoryCount", count
            ));
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(ErrorResponse.of("db_error", safeMsg(e), Map.of(
                            "dbPath", dbPath.toString(),
                            "dbUrl", dbUrl
                    )));
        }
    }

    @GetMapping("/history")
    public ResponseEntity<?> history(@RequestParam(defaultValue = "20") int limit) {
        Path keeplyRoot = resolveKeeplyRootOrThrow();
        Path dbPath = keeplyRoot.resolve(keeplyDbFile).normalize();
        String dbUrl = "jdbc:sqlite:" + dbPath.toAbsolutePath();
        int safeLimit = clamp(limit, 1, 200);

        if (!Files.exists(dbPath)) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(ErrorResponse.of("db_not_found", "Arquivo de banco não encontrado", Map.of(
                            "dbPath", dbPath.toString()
                    )));
        }

        String sql = """
                SELECT id, started_at, finished_at, status, root_path, dest_path, files_processed, errors, scan_id, message, backup_type
                FROM backup_history
                ORDER BY id DESC
                LIMIT ?
                """;

        try (Connection conn = DriverManager.getConnection(dbUrl);
             PreparedStatement ps = conn.prepareStatement(sql)) {

            ps.setInt(1, safeLimit);

            List<HistoryItem> items = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    items.add(new HistoryItem(
                            rs.getLong("id"),
                            rs.getString("started_at"),
                            rs.getString("finished_at"),
                            rs.getString("status"),
                            rs.getString("backup_type"),
                            rs.getString("root_path"),
                            rs.getString("dest_path"),
                            rs.getLong("files_processed"),
                            rs.getLong("errors"),
                            (rs.getObject("scan_id") == null ? null : String.valueOf(rs.getObject("scan_id"))),
                            rs.getString("message")
                    ));
                }
            }

            return ResponseEntity.ok(new HistoryResponse(true, safeLimit, items));
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(ErrorResponse.of("history_error", safeMsg(e), Map.of("limit", safeLimit)));
        }
    }

    @PostMapping("/scan")
    public ResponseEntity<?> scan(@RequestBody ScanRequest request) {
        if (request == null || isBlank(request.root()) || isBlank(request.dest())) {
            return ResponseEntity.badRequest()
                    .body(ErrorResponse.of("bad_request", "Informe root e dest", null));
        }

        Path keeplyRoot = resolveKeeplyRootOrThrow();

        String jobId = UUID.randomUUID().toString();
        ScanJob job = ScanJob.created(jobId, request.root(), request.dest());
        jobs.put(jobId, job);

        jobPool.submit(() -> runScanJob(job, keeplyRoot, request));

        return ResponseEntity.status(HttpStatus.ACCEPTED)
                .body(Map.of(
                        "ok", true,
                        "jobId", jobId,
                        "statusUrl", "/api/keeply/scan/" + jobId,
                        "message", "scan enfileirado"
                ));
    }

    @GetMapping("/scan/{jobId}")
    public ResponseEntity<?> scanStatus(@PathVariable String jobId) {
        ScanJob job = jobs.get(jobId);
        if (job == null) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body(ErrorResponse.of("not_found", "jobId não existe", Map.of("jobId", jobId)));
        }
        return ResponseEntity.ok(Map.of("ok", true, "job", job));
    }

    // ------------------------- job runner -------------------------

    private void runScanJob(ScanJob job, Path keeplyRoot, ScanRequest req) {
        job.markRunning();

        CliMode cli = resolveCliMode(keeplyRoot);
        Path logFile = null;

        try {
            logFile = Files.createTempFile("keeply-scan-" + job.id() + "-", ".log");
            logFile.toFile().deleteOnExit();

            List<String> command = cli.buildScanCommand(req);

            ProcessBuilder pb = new ProcessBuilder(command);
            pb.directory(keeplyRoot.toFile());
            pb.redirectErrorStream(true);
            pb.redirectOutput(logFile.toFile());

            job.setCommandPreview(String.join(" ", command));

            Process p = pb.start();

            boolean finished = p.waitFor(cliTimeoutSeconds, TimeUnit.SECONDS);
            if (!finished) {
                p.destroyForcibly();
                job.markFailed(124, "timeout após " + cliTimeoutSeconds + "s");
            } else {
                int exit = p.exitValue();
                if (exit == 0) job.markSuccess();
                else job.markFailed(exit, "exitCode != 0");
            }

            // tail do log (pra não devolver 10MB num JSON)
            long size = Files.size(logFile);
            boolean truncated = size > maxLogBytes;
            String output = tailUtf8(logFile, maxLogBytes);
            job.setOutput(output, truncated);

        } catch (Exception e) {
            job.markFailed(999, safeMsg(e));
            if (logFile != null) {
                try {
                    String output = tailUtf8(logFile, maxLogBytes);
                    job.setOutput(output, Files.size(logFile) > maxLogBytes);
                } catch (Exception ignored) {}
            }
        }
    }

    // ------------------------- CLI selection -------------------------

    private CliMode resolveCliMode(Path keeplyRoot) {
        // Preferir JAR, porque Maven por request é lento
        if (!isBlank(keeplyCliJar)) {
            Path jar = Path.of(keeplyCliJar).toAbsolutePath().normalize();
            if (Files.exists(jar) && Files.isRegularFile(jar)) {
                return CliMode.jar(jar);
            }
        }
        return CliMode.maven(resolveMavenCommand(keeplyRoot));
    }

    private List<String> resolveMavenCommand(Path keeplyRoot) {
        Path mvnwCmd = keeplyRoot.resolve("mvnw.cmd");
        Path mvnwSh = keeplyRoot.resolve("mvnw");

        if (Files.exists(mvnwCmd)) return new ArrayList<>(List.of(mvnwCmd.toString()));
        if (Files.exists(mvnwSh)) return new ArrayList<>(List.of(mvnwSh.toString()));

        boolean isWindows = System.getProperty("os.name", "").toLowerCase().contains("win");
        return new ArrayList<>(List.of(isWindows ? "mvn.cmd" : "mvn"));
    }

    // ------------------------- keeply root -------------------------

    private Path resolveKeeplyRootOrThrow() {
        Path userDir = Path.of(System.getProperty("user.dir")).toAbsolutePath().normalize();

        List<Path> candidates = new ArrayList<>();
        if (!isBlank(keeplyProjectDir)) {
            candidates.add(userDir.resolve(keeplyProjectDir).normalize());
            candidates.add(Path.of(keeplyProjectDir).toAbsolutePath().normalize());
        }
        candidates.add(userDir);
        candidates.add(userDir.resolve("Keeply-V2"));
        if (userDir.getParent() != null) candidates.add(userDir.getParent());

        for (Path c : candidates) {
            if (isKeeplyRoot(c)) return c;
        }

        // fail-fast: melhor do que “adivinhar” e operar em path errado
        throw new IllegalStateException(
                "Não encontrei o projeto Keeply. Configure keeply.cli.project-dir apontando para a raiz (onde tem pom.xml e src/main/java/com/keeply/app/Main.java). " +
                "Tentativas: " + candidates
        );
    }

    private boolean isKeeplyRoot(Path dir) {
        if (dir == null || !Files.isDirectory(dir)) return false;
        return Files.exists(dir.resolve("pom.xml"))
                && Files.exists(dir.resolve("src/main/java/com/keeply/app/Main.java"));
    }

    // ------------------------- helpers -------------------------

    private static int clamp(int v, int min, int max) {
        return Math.max(min, Math.min(max, v));
    }

    private static boolean isBlank(String v) {
        return v == null || v.isBlank();
    }

    private static String safeMsg(Throwable t) {
        return (t.getMessage() == null || t.getMessage().isBlank())
                ? t.getClass().getSimpleName()
                : t.getMessage();
    }

    /**
     * Lê os últimos N bytes de um arquivo e retorna como UTF-8.
     * Se cortar no meio de um multibyte, pode aparecer � no começo — aceitável pra log tail.
     */
    private static String tailUtf8(Path file, int maxBytes) throws IOException {
        long size = Files.size(file);
        long start = Math.max(0, size - maxBytes);

        try (FileChannel ch = FileChannel.open(file, StandardOpenOption.READ)) {
            ch.position(start);

            byte[] buf = new byte[8192];
            ByteBuffer bb = ByteBuffer.wrap(buf);

            ByteArrayOutputStreamEx out = new ByteArrayOutputStreamEx();
            while (true) {
                bb.clear();
                int n = ch.read(bb);
                if (n <= 0) break;
                out.write(buf, 0, n);
            }

            // se começamos no meio do arquivo, descartamos a primeira linha parcial pra ficar “bonito”
            String s = out.toString(StandardCharsets.UTF_8);
            if (start > 0) {
                int idx = s.indexOf('\n');
                if (idx >= 0 && idx + 1 < s.length()) s = s.substring(idx + 1);
            }
            return s.trim();
        }
    }

    private static final class ByteArrayOutputStreamEx extends java.io.ByteArrayOutputStream {
        @Override
        public String toString(java.nio.charset.Charset cs) {
            return new String(this.buf, 0, this.count, cs);
        }
    }

    // ------------------------- DTOs -------------------------

    public record ScanRequest(String root, String dest, String password) {}

    public record HealthResponse(
            boolean ok,
            String service,
            String ts,
            String keeplyRoot,
            String dbPath,
            boolean dbExists,
            String cliMode,
            String cliCommandPreview
    ) {}

    public record HistoryResponse(boolean ok, int limit, List<HistoryItem> items) {}

    public record HistoryItem(
            long id,
            String startedAt,
            String finishedAt,
            String status,
            String backupType,
            String rootPath,
            String destPath,
            long filesProcessed,
            long errors,
            String scanId,
            String message
    ) {}

    public record ErrorResponse(boolean ok, ErrorBody error, Map<String, Object> meta) {
        static ErrorResponse of(String code, String message, Map<String, Object> meta) {
            return new ErrorResponse(false, new ErrorBody(code, message), meta);
        }
    }
    public record ErrorBody(String code, String message) {}

    // ------------------------- CLI mode abstraction -------------------------

    private sealed interface CliMode permits CliModeJar, CliModeMaven {
        String mode();
        String commandPreview();
        List<String> buildScanCommand(ScanRequest req);

        static CliMode jar(Path jar) { return new CliModeJar(jar); }
        static CliMode maven(List<String> base) { return new CliModeMaven(base); }
    }

    private static final class CliModeJar implements CliMode {
        private final Path jar;
        CliModeJar(Path jar) { this.jar = jar; }

        @Override public String mode() { return "jar"; }

        @Override public String commandPreview() {
            return "java -jar " + jar + " scan --root <...> --dest <...> [--password <...>]";
        }

        @Override
        public List<String> buildScanCommand(ScanRequest req) {
            List<String> cmd = new ArrayList<>();
            cmd.add("java");
            cmd.add("-jar");
            cmd.add(jar.toString());
            cmd.add("scan");
            cmd.add("--root");
            cmd.add(req.root());
            cmd.add("--dest");
            cmd.add(req.dest());
            if (!isBlank(req.password())) {
                cmd.add("--password");
                cmd.add(req.password());
            }
            return cmd;
        }
    }

    private static final class CliModeMaven implements CliMode {
        private final List<String> base;
        CliModeMaven(List<String> base) { this.base = new ArrayList<>(base); }

        @Override public String mode() { return "maven"; }

        @Override public String commandPreview() {
            return String.join(" ", base) + " -q exec:java -Dexec.args=\"scan --root <...> --dest <...>\"";
        }

        @Override
        public List<String> buildScanCommand(ScanRequest req) {
            // Maven exec plugin recebe args como STRING: quoting é frágil (principalmente Windows).
            // Por isso o modo JAR é recomendado.
            StringBuilder args = new StringBuilder();
            args.append("scan");
            args.append(" --root ").append(quote(req.root()));
            args.append(" --dest ").append(quote(req.dest()));
            if (!isBlank(req.password())) {
                args.append(" --password ").append(quote(req.password()));
            }

            List<String> cmd = new ArrayList<>(base);
            cmd.add("-q");
            cmd.add("exec:java");
            cmd.add("-Dexec.args=" + args);
            return cmd;
        }

        private String quote(String v) {
            // quoting simples; ainda pode doer no Windows dependendo de caracteres.
            return "\"" + v.replace("\"", "\\\"") + "\"";
        }
    }

    // ------------------------- Job model -------------------------

    public static final class ScanJob {
        private final String id;
        private final String root;
        private final String dest;
        private final String createdAt;

        private volatile String startedAt;
        private volatile String finishedAt;
        private volatile String state;   // created | running | success | failed
        private volatile Integer exitCode;
        private volatile String message;
        private volatile String commandPreview;

        private volatile String output;
        private volatile boolean outputTruncated;

        private ScanJob(String id, String root, String dest) {
            this.id = id;
            this.root = root;
            this.dest = dest;
            this.createdAt = Instant.now().toString();
            this.state = "created";
        }

        static ScanJob created(String id, String root, String dest) {
            return new ScanJob(id, root, dest);
        }

        void markRunning() {
            this.state = "running";
            this.startedAt = Instant.now().toString();
        }

        void markSuccess() {
            this.state = "success";
            this.exitCode = 0;
            this.message = "ok";
            this.finishedAt = Instant.now().toString();
        }

        void markFailed(int exit, String msg) {
            this.state = "failed";
            this.exitCode = exit;
            this.message = msg;
            this.finishedAt = Instant.now().toString();
        }

        void setCommandPreview(String cmd) {
            this.commandPreview = cmd;
        }

        void setOutput(String output, boolean truncated) {
            this.output = output;
            this.outputTruncated = truncated;
        }

        public String id() { return id; }
        public String getId() { return id; }
        public String getRoot() { return root; }
        public String getDest() { return dest; }
        public String getCreatedAt() { return createdAt; }
        public String getStartedAt() { return startedAt; }
        public String getFinishedAt() { return finishedAt; }
        public String getState() { return state; }
        public Integer getExitCode() { return exitCode; }
        public String getMessage() { return message; }
        public String getCommandPreview() { return commandPreview; }
        public String getOutput() { return output; }
        public boolean isOutputTruncated() { return outputTruncated; }
    }
}
