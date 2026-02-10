package com.keeply.app.service;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.ClosedWatchServiceException;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_DELETE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY;
import static java.nio.file.StandardWatchEventKinds.OVERFLOW;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.keeply.app.database.DatabaseBackup;

public final class JournalIngestService implements Closeable {

    public enum Backend { WATCHSERVICE, USN }

    public enum Kind { CREATE, MODIFY, DELETE, MOVE, OVERFLOW }

    public record FsChange(Kind kind, Path root, String pathRel, String oldPathRel, Instant at) {}

    private static final Logger logger = LoggerFactory.getLogger(JournalIngestService.class);

    private final Path root;
    private final ExecutorService ioLoop;
    private final AtomicBoolean running = new AtomicBoolean(false);

    private volatile Closeable source;
    private volatile DbEventWriter writer;

    public JournalIngestService(Path root) {
        this.root = Objects.requireNonNull(root, "root").toAbsolutePath().normalize();
        this.ioLoop = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "keeply-journal-io");
            t.setDaemon(true);
            return t;
        });
    }

    public void start() throws IOException {
        start(selectBestBackend(root));
    }

    public void start(Backend backend) throws IOException {
        if (!running.compareAndSet(false, true)) return;

        DatabaseBackup.init();

        this.writer = new DbEventWriter();
        this.writer.start();

        Consumer<FsChange> sink = this.writer::enqueue;

        Closeable s;
        if (backend == Backend.USN) {
            try {
                s = new UsnSource(root, sink);
                ((UsnSource) s).start();
                this.source = s;
                ioLoop.submit(((UsnSource) s)::loop);
                logger.info("JournalIngestService USN iniciado para {}", root);
                return;
            } catch (Throwable t) {
                logger.warn("USN indisponível/ falhou (fallback para WatchService): {}", t.toString());
            }
        }

        WatchSource ws = new WatchSource(root, sink);
        ws.start();
        this.source = ws;
        ioLoop.submit(ws::loop);
        logger.info("JournalIngestService WatchService iniciado para {}", root);
    }

    private static Backend selectBestBackend(Path root) {
        try {
            String os = System.getProperty("os.name", "generic").toLowerCase();
            if (!os.contains("win")) return Backend.WATCHSERVICE;
            String fs = Files.getFileStore(root).type();
            if (fs != null && fs.equalsIgnoreCase("NTFS")) return Backend.USN;
        } catch (Exception ignored) {}
        return Backend.WATCHSERVICE;
    }

    @Override
    public void close() throws IOException {
        running.set(false);
        try { if (source != null) source.close(); } catch (Exception ignored) {}
        ioLoop.shutdownNow();
        if (writer != null) writer.close();
    }

    private static final class DbEventWriter implements Closeable {
        private final ArrayBlockingQueue<FsChange> q = new ArrayBlockingQueue<>(200_000);
        private final AtomicBoolean alive = new AtomicBoolean(true);
        private Thread worker;

        void start() {
            worker = Thread.ofVirtual().name("keeply-fs-events-writer").start(this::run);
        }

        void enqueue(FsChange ev) {
            if (ev == null || !alive.get()) return;
            if (!q.offer(ev)) {
                q.poll();
                q.offer(new FsChange(Kind.OVERFLOW, ev.root(), "", null, Instant.now()));
            }
        }

        private void run() {
            try (var c = DatabaseBackup.openSingleConnection()) {
                try { c.setAutoCommit(false); } catch (Exception ignored) {}

                var sql = "INSERT INTO fs_events (root_path, path_rel, kind, old_path_rel, event_time) VALUES (?,?,?,?,?)";
                try (var ps = c.prepareStatement(sql)) {

                    int pending = 0;
                    long lastFlush = System.nanoTime();
                    final long MAX_LAT_NS = TimeUnit.MILLISECONDS.toNanos(250);
                    final int BATCH = 800;

                    while (alive.get() || !q.isEmpty()) {
                        FsChange first = q.poll(120, TimeUnit.MILLISECONDS);
                        if (first != null) {
                            add(ps, first);
                            pending++;
                        }

                        while (pending < BATCH) {
                            FsChange e = q.poll();
                            if (e == null) break;
                            add(ps, e);
                            pending++;
                        }

                        boolean timeToFlush = pending > 0 && (System.nanoTime() - lastFlush) >= MAX_LAT_NS;
                        boolean full = pending >= BATCH;
                        boolean draining = pending > 0 && !alive.get() && q.isEmpty();

                        if (full || timeToFlush || draining) {
                            ps.executeBatch();
                            ps.clearBatch();
                            c.commit();
                            pending = 0;
                            lastFlush = System.nanoTime();
                        }
                    }

                    if (pending > 0) {
                        ps.executeBatch();
                        ps.clearBatch();
                        c.commit();
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                logger.warn("FsEvent writer falhou: {}", e.toString());
            }
        }

        private static void add(java.sql.PreparedStatement ps, FsChange ev) throws Exception {
            ps.setString(1, ev.root().toString());
            ps.setString(2, ev.pathRel());
            ps.setString(3, ev.kind().name());
            ps.setString(4, ev.oldPathRel());
            ps.setString(5, ev.at().toString());
            ps.addBatch();
        }

        @Override
        public void close() {
            alive.set(false);
            if (worker != null) {
                try { worker.join(2500); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            }
        }
    }

    private static final class WatchSource implements Closeable {
        private final Path root;
        private final Consumer<FsChange> sink;

        private final WatchService watcher;
        private final Map<WatchKey, Path> keyToDir = new ConcurrentHashMap<>();
        private final AtomicBoolean alive = new AtomicBoolean(true);

        WatchSource(Path root, Consumer<FsChange> sink) throws IOException {
            this.root = root.toAbsolutePath().normalize();
            this.sink = sink;
            this.watcher = FileSystems.getDefault().newWatchService();
        }

        void start() throws IOException {
            registerTree(root);
        }

        void loop() {
            while (alive.get()) {
                WatchKey key;
                try {
                    key = watcher.take();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                } catch (ClosedWatchServiceException e) {
                    return;
                }

                Path dir = keyToDir.get(key);
                if (dir == null) {
                    key.reset();
                    continue;
                }

                for (WatchEvent<?> ev : key.pollEvents()) {
                    WatchEvent.Kind<?> k = ev.kind();

                    if (k == OVERFLOW) {
                        sink.accept(new FsChange(Kind.OVERFLOW, root, rel(dir), null, Instant.now()));
                        continue;
                    }

                    @SuppressWarnings("unchecked")
                    WatchEvent<Path> pev = (WatchEvent<Path>) ev;
                    Path name = pev.context();
                    Path full = dir.resolve(name);

                    Kind outKind = (k == ENTRY_CREATE) ? Kind.CREATE : (k == ENTRY_DELETE) ? Kind.DELETE : Kind.MODIFY;
                    sink.accept(new FsChange(outKind, root, rel(full), null, Instant.now()));

                    if (outKind == Kind.CREATE) {
                        try {
                            if (Files.isDirectory(full, LinkOption.NOFOLLOW_LINKS)) registerTree(full);
                        } catch (IOException ignored) {}
                    }
                }

                boolean valid = key.reset();
                if (!valid) keyToDir.remove(key);
            }
        }

        private void registerTree(Path start) throws IOException {
            Files.walkFileTree(start, new SimpleFileVisitor<>() {
                @Override
                public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                    registerDir(dir);
                    return FileVisitResult.CONTINUE;
                }
            });
        }

        private void registerDir(Path dir) throws IOException {
            WatchKey key = dir.register(watcher, ENTRY_CREATE, ENTRY_DELETE, ENTRY_MODIFY);
            keyToDir.put(key, dir);
        }

        private String rel(Path p) {
            try {
                Path rp = root.toAbsolutePath().normalize();
                Path ap = p.toAbsolutePath().normalize();
                if (ap.startsWith(rp)) return rp.relativize(ap).toString().replace('\\', '/');
            } catch (Exception ignored) {}
            return p.toString().replace('\\', '/');
        }

        @Override
        public void close() throws IOException {
            alive.set(false);
            watcher.close();
            keyToDir.clear();
        }
    }

    private static final class UsnSource implements Closeable {
        private static final int FILE_DEVICE_FILE_SYSTEM = 0x00000009;
        private static final int FILE_ANY_ACCESS = 0;
        private static final int METHOD_BUFFERED = 0;
        private static final int METHOD_NEITHER = 3;

        private static final int FSCTL_QUERY_USN_JOURNAL = ctl(FILE_DEVICE_FILE_SYSTEM, 61, METHOD_BUFFERED, FILE_ANY_ACCESS);
        private static final int FSCTL_READ_USN_JOURNAL  = ctl(FILE_DEVICE_FILE_SYSTEM, 46, METHOD_NEITHER,   FILE_ANY_ACCESS);

        private static int ctl(int dev, int fn, int method, int access) {
            return (dev << 16) | (access << 14) | (fn << 2) | method;
        }

        private static final int USN_REASON_DATA_OVERWRITE     = 0x00000001;
        private static final int USN_REASON_DATA_EXTEND        = 0x00000002;
        private static final int USN_REASON_DATA_TRUNCATION    = 0x00000004;
        private static final int USN_REASON_FILE_CREATE        = 0x00000100;
        private static final int USN_REASON_FILE_DELETE        = 0x00000200;
        private static final int USN_REASON_RENAME_OLD_NAME    = 0x00001000;
        private static final int USN_REASON_RENAME_NEW_NAME    = 0x00002000;
        private static final int USN_REASON_BASIC_INFO_CHANGE  = 0x00008000;
        private static final int USN_REASON_SECURITY_CHANGE    = 0x00010000;
        private static final int USN_REASON_CLOSE              = 0x80000000;

        private static final long FILETIME_EPOCH_DIFF_100NS = 116444736000000000L;

        private final Path root;
        private final Consumer<FsChange> sink;
        private final AtomicBoolean alive = new AtomicBoolean(true);

        private final Path rootAbs;
        private final String volumeDos;

        private final FrnPathResolver resolver;
        private final PendingRenames renames = new PendingRenames();

        private com.sun.jna.platform.win32.WinNT.HANDLE hVol;
        private long journalId;
        private long nextUsn;

        UsnSource(Path root, Consumer<FsChange> sink) {
            this.root = root.toAbsolutePath().normalize();
            this.rootAbs = this.root;
            this.sink = sink;
            this.volumeDos = guessVolumeDos(this.rootAbs);
            this.resolver = new FrnPathResolver(volumeDos);
        }

        void start() throws IOException {
            openVolume();
            queryJournal();
        }

        void loop() {
            final int OUT_CAP = 1024 * 1024;
            final com.sun.jna.Memory outBuf = new com.sun.jna.Memory(OUT_CAP);
            final com.sun.jna.ptr.IntByReference br = new com.sun.jna.ptr.IntByReference();

            while (alive.get() && !Thread.currentThread().isInterrupted()) {
                try {
                    com.sun.jna.Memory inBuf = new com.sun.jna.Memory(40);
                    inBuf.setLong(0, nextUsn);
                    inBuf.setInt(8, 0xFFFFFFFF);
                    inBuf.setInt(12, 0);
                    inBuf.setLong(16, 0);
                    inBuf.setLong(24, 1);
                    inBuf.setLong(32, journalId);

                    br.setValue(0);
                    boolean ok = com.sun.jna.platform.win32.Kernel32.INSTANCE.DeviceIoControl(
                        hVol, FSCTL_READ_USN_JOURNAL,
                        inBuf, (int) inBuf.size(),
                        outBuf, (int) outBuf.size(),
                        br, null
                    );

                    if (!ok) {
                        int err = com.sun.jna.platform.win32.Kernel32.INSTANCE.GetLastError();
                        sink.accept(new FsChange(Kind.OVERFLOW, rootAbs, "", null, Instant.now()));
                        Thread.sleep(400);
                        if (err == 1179 || err == 1171) { queryJournal(); continue; }
                        continue;
                    }

                    int got = br.getValue();
                    if (got <= 8) { Thread.sleep(180); continue; }

                    long nUsn = outBuf.getLong(0);
                    nextUsn = nUsn;

                    parseRecords(outBuf, got);
                    renames.compact(Instant.now());

                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                } catch (Throwable t) {
                    sink.accept(new FsChange(Kind.OVERFLOW, rootAbs, "", null, Instant.now()));
                    try { Thread.sleep(500); } catch (InterruptedException e) { Thread.currentThread().interrupt(); return; }
                }
            }
        }

        private void parseRecords(com.sun.jna.Memory outBuf, int got) {
            int off = 8;
            while (off + 4 <= got) {
                int len = outBuf.getInt(off);
                if (len <= 0 || off + len > got) break;

                short major = outBuf.getShort(off + 4);
                if (major < 2) { off += len; continue; }

                long parentFrn = outBuf.getLong(off + 16);
                long tsFiletime = outBuf.getLong(off + 32);
                int reason = outBuf.getInt(off + 40);

                short nameLen = outBuf.getShort(off + 56);
                short nameOff = outBuf.getShort(off + 58);

                String name = "";
                if (nameLen > 0 && nameOff > 0 && (off + nameOff + nameLen) <= (off + len)) {
                    byte[] nb = outBuf.getByteArray(off + nameOff, nameLen);
                    name = new String(nb, java.nio.charset.StandardCharsets.UTF_16LE);
                }

                String parentPath = resolver.pathForFrn(parentFrn);
                if (parentPath != null) {
                    Path full = safeResolve(parentPath, name);
                    if (full != null) {
                        full = full.toAbsolutePath().normalize();
                        if (full.startsWith(rootAbs)) {
                            String rel = rootAbs.relativize(full).toString().replace('\\', '/');
                            Instant at = filetimeToInstant(tsFiletime);

                            Kind k = mapReason(reason);
                            if (k == Kind.MOVE) {
                                if ((reason & USN_REASON_RENAME_OLD_NAME) != 0) {
                                    renames.put(parentFrn, rel, at);
                                } else if ((reason & USN_REASON_RENAME_NEW_NAME) != 0) {
                                    String old = renames.take(parentFrn);
                                    sink.accept(new FsChange(Kind.MOVE, rootAbs, rel, old, at));
                                } else {
                                    sink.accept(new FsChange(Kind.MOVE, rootAbs, rel, null, at));
                                }
                            } else {
                                sink.accept(new FsChange(k, rootAbs, rel, null, at));
                            }
                        }
                    }
                }

                off += len;
            }
        }

        private static Path safeResolve(String parentPath, String name) {
            try {
                if (parentPath == null || parentPath.isBlank()) return null;
                Path p = Path.of(parentPath);
                if (name == null || name.isBlank()) return p;
                return p.resolve(name);
            } catch (Exception e) {
                return null;
            }
        }

        private static Kind mapReason(int reason) {
            if ((reason & USN_REASON_FILE_CREATE) != 0) return Kind.CREATE;
            if ((reason & USN_REASON_FILE_DELETE) != 0) return Kind.DELETE;
            if ((reason & USN_REASON_RENAME_OLD_NAME) != 0) return Kind.MOVE;
            if ((reason & USN_REASON_RENAME_NEW_NAME) != 0) return Kind.MOVE;

            int modMask = USN_REASON_DATA_OVERWRITE | USN_REASON_DATA_EXTEND | USN_REASON_DATA_TRUNCATION |
                          USN_REASON_BASIC_INFO_CHANGE | USN_REASON_SECURITY_CHANGE | USN_REASON_CLOSE;
            if ((reason & modMask) != 0) return Kind.MODIFY;

            return Kind.MODIFY;
        }

        private static Instant filetimeToInstant(long filetime100ns) {
            long v = filetime100ns - FILETIME_EPOCH_DIFF_100NS;
            if (v <= 0) return Instant.EPOCH;
            long sec = v / 10_000_000L;
            long nanos = (v % 10_000_000L) * 100L;
            return Instant.ofEpochSecond(sec, nanos);
        }

        private void openVolume() throws IOException {
            com.sun.jna.platform.win32.WinNT.HANDLE h = com.sun.jna.platform.win32.Kernel32.INSTANCE.CreateFile(
                "\\\\.\\" + volumeDos,
                com.sun.jna.platform.win32.WinNT.GENERIC_READ,
                com.sun.jna.platform.win32.WinNT.FILE_SHARE_READ |
                com.sun.jna.platform.win32.WinNT.FILE_SHARE_WRITE |
                com.sun.jna.platform.win32.WinNT.FILE_SHARE_DELETE,
                null,
                com.sun.jna.platform.win32.WinNT.OPEN_EXISTING,
                0,
                null
            );
            if (com.sun.jna.platform.win32.WinBase.INVALID_HANDLE_VALUE.equals(h)) {
                int err = com.sun.jna.platform.win32.Kernel32.INSTANCE.GetLastError();
                throw new IOException("CreateFile(volume) falhou err=" + err);
            }
            this.hVol = h;
        }

        private void queryJournal() throws IOException {
            com.sun.jna.Memory out = new com.sun.jna.Memory(64);
            com.sun.jna.ptr.IntByReference br = new com.sun.jna.ptr.IntByReference();

            boolean ok = com.sun.jna.platform.win32.Kernel32.INSTANCE.DeviceIoControl(
                hVol, FSCTL_QUERY_USN_JOURNAL,
                null, 0,
                out, (int) out.size(),
                br, null
            );
            if (!ok) {
                int err = com.sun.jna.platform.win32.Kernel32.INSTANCE.GetLastError();
                throw new IOException("FSCTL_QUERY_USN_JOURNAL falhou err=" + err);
            }

            long jid = out.getLong(0);
            long next = out.getLong(16);

            this.journalId = jid;
            this.nextUsn = next;
        }

        private static String guessVolumeDos(Path p) {
            String s = p.toAbsolutePath().toString();
            if (s.length() >= 2 && s.charAt(1) == ':') return s.substring(0, 2);
            return "C:";
        }

        @Override
        public void close() throws IOException {
            alive.set(false);
            try { resolver.close(); } catch (Exception ignored) {}
            if (hVol != null && !com.sun.jna.platform.win32.WinBase.INVALID_HANDLE_VALUE.equals(hVol)) {
                com.sun.jna.platform.win32.Kernel32.INSTANCE.CloseHandle(hVol);
            }
        }

        private static final class PendingRenames {
            private static final class Entry { final String oldRel; final Instant at; Entry(String r, Instant t){oldRel=r;at=t;} }
            private final ConcurrentHashMap<Long, Entry> map = new ConcurrentHashMap<>();

            void put(long key, String oldRel, Instant at) { map.put(key, new Entry(oldRel, at)); }
            String take(long key) { Entry e = map.remove(key); return e == null ? null : e.oldRel; }

            void compact(Instant now) {
                for (var e : map.entrySet()) {
                    if (e.getValue() != null && e.getValue().at != null) {
                        if (e.getValue().at.plusSeconds(12).isBefore(now)) map.remove(e.getKey());
                    }
                }
            }
        }

        private static final class FrnPathResolver implements Closeable {
            private final String volumeDos;
            private final LruCache<Long, String> cache = new LruCache<>(80_000);

            FrnPathResolver(String volumeDos) {
                this.volumeDos = volumeDos;
            }

            String pathForFrn(long frn) {
                if (frn <= 0) return volumeDos + "\\";
                String hit = cache.get(frn);
                if (hit != null) return hit;

                String p = queryPathById(volumeDos, frn);
                if (p != null) cache.put(frn, p);
                return p;
            }

            private static String queryPathById(String vol, long frn) {
                com.sun.jna.platform.win32.WinNT.HANDLE hVol = com.sun.jna.platform.win32.Kernel32.INSTANCE.CreateFile(
                    "\\\\.\\" + vol,
                    com.sun.jna.platform.win32.WinNT.GENERIC_READ,
                    com.sun.jna.platform.win32.WinNT.FILE_SHARE_READ |
                    com.sun.jna.platform.win32.WinNT.FILE_SHARE_WRITE |
                    com.sun.jna.platform.win32.WinNT.FILE_SHARE_DELETE,
                    null,
                    com.sun.jna.platform.win32.WinNT.OPEN_EXISTING,
                    0,
                    null
                );
                if (com.sun.jna.platform.win32.WinBase.INVALID_HANDLE_VALUE.equals(hVol)) return null;

                try {
                    FILE_ID_DESCRIPTOR desc = new FILE_ID_DESCRIPTOR();
                    desc.dwSize = desc.size();
                    desc.Type = 0;
                    desc.union.setType(long.class);
                    desc.union.FileId = frn;
                    desc.union.write();
                    desc.write();

                    com.sun.jna.platform.win32.WinNT.HANDLE h = Kernel32Ex.INSTANCE.OpenFileById(
                        hVol,
                        desc,
                        FILE_READ_ATTRIBUTES,
                        com.sun.jna.platform.win32.WinNT.FILE_SHARE_READ |
                        com.sun.jna.platform.win32.WinNT.FILE_SHARE_WRITE |
                        com.sun.jna.platform.win32.WinNT.FILE_SHARE_DELETE,
                        null,
                        com.sun.jna.platform.win32.WinNT.FILE_FLAG_BACKUP_SEMANTICS
                    );

                    if (com.sun.jna.platform.win32.WinBase.INVALID_HANDLE_VALUE.equals(h)) return null;

                    try {
                        char[] buf = new char[8192];
                        int n = Kernel32Ex.INSTANCE.GetFinalPathNameByHandleW(h, buf, buf.length, 0);
                        if (n <= 0) return null;
                        String raw = new String(buf, 0, Math.min(n, buf.length));
                        return normalizeWinPath(raw);
                    } finally {
                        com.sun.jna.platform.win32.Kernel32.INSTANCE.CloseHandle(h);
                    }

                } finally {
                    com.sun.jna.platform.win32.Kernel32.INSTANCE.CloseHandle(hVol);
                }
            }

            private static String normalizeWinPath(String raw) {
                if (raw == null) return null;
                String s = raw;
                if (s.startsWith("\\\\?\\")) s = s.substring(4);
                if (s.startsWith("UNC\\")) s = "\\\\" + s.substring(4);
                return s;
            }

            @Override public void close() {}
        }

        private static final int FILE_READ_ATTRIBUTES = 0x80;

        @com.sun.jna.Structure.FieldOrder({ "dwSize", "Type", "union" })
        public static class FILE_ID_DESCRIPTOR extends com.sun.jna.Structure {
            public int dwSize;
            public int Type;
            public FILE_ID_UNION union = new FILE_ID_UNION();
        }

        public static class FILE_ID_UNION extends com.sun.jna.Union {
            public long FileId;
            public byte[] ObjectId = new byte[16];
        }

        public interface Kernel32Ex extends com.sun.jna.win32.StdCallLibrary {
            Kernel32Ex INSTANCE = com.sun.jna.Native.load("kernel32", Kernel32Ex.class, com.sun.jna.win32.W32APIOptions.DEFAULT_OPTIONS);
            com.sun.jna.platform.win32.WinNT.HANDLE OpenFileById(
                com.sun.jna.platform.win32.WinNT.HANDLE hVolumeHint,
                FILE_ID_DESCRIPTOR lpFileId,
                int dwDesiredAccess,
                int dwShareMode,
                com.sun.jna.Pointer lpSecurityAttributes,
                int dwFlags
            );
            int GetFinalPathNameByHandleW(com.sun.jna.platform.win32.WinNT.HANDLE hFile, char[] lpszFilePath, int cchFilePath, int dwFlags);
        }

        private static final class LruCache<K, V> {
            private final int max;
            private final java.util.LinkedHashMap<K, V> map;

            LruCache(int max) {
                this.max = Math.max(1024, max);
                this.map = new java.util.LinkedHashMap<>(4096, 0.75f, true) {
                    @Override protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
                        return size() > LruCache.this.max;
                    }
                };
            }

            synchronized V get(K k) { return map.get(k); }
            synchronized void put(K k, V v) { map.put(k, v); }
        }
    }

    public interface Snapshotter extends Closeable {
        Path openSnapshotRoot(Path root) throws Exception;
    }

    public static final class WindowsVssSnapshotter implements Snapshotter {
        private String shadowId;
        private String deviceObject;

        @Override
        public Path openSnapshotRoot(Path root) throws Exception {
            Path abs = root.toAbsolutePath().normalize();
            String vol = abs.toString().substring(0, 3);
            shadowId = ps("(Get-WmiObject -List Win32_ShadowCopy).Create('" + escapePs(vol) + "','ClientAccessible').ShadowID");
            if (shadowId == null || shadowId.isBlank()) throw new IOException("VSS create falhou (ShadowID vazio)");

            deviceObject = ps("(Get-WmiObject Win32_ShadowCopy | Where-Object { $_.ID -eq '" + escapePs(shadowId) + "' }).DeviceObject");
            if (deviceObject == null || deviceObject.isBlank()) throw new IOException("VSS query DeviceObject falhou");

            String rel = abs.toString().substring(3);
            if (!rel.isEmpty() && (rel.startsWith("\\") || rel.startsWith("/"))) rel = rel.substring(1);

            String snapRoot = deviceObject.endsWith("\\") ? deviceObject : (deviceObject + "\\");
            String full = snapRoot + rel;

            return Path.of(full);
        }

        @Override
        public void close() throws IOException {
            if (shadowId == null || shadowId.isBlank()) return;
            try {
                ps("([WMI]'Win32_ShadowCopy.ID=\"" + escapePs(shadowId) + "\"').Delete() | Out-Null; 'OK'");
            } catch (Exception ignored) {}
        }

        private static String ps(String cmd) throws IOException {
            Process p = new ProcessBuilder("powershell.exe", "-NoProfile", "-NonInteractive", "-Command", cmd)
                .redirectErrorStream(true)
                .start();
            try (BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream()))) {
                StringBuilder sb = new StringBuilder();
                String line;
                while ((line = br.readLine()) != null) {
                    if (!line.isBlank()) {
                        if (sb.length() > 0) sb.append('\n');
                        sb.append(line.trim());
                    }
                }
                try { p.waitFor(8, TimeUnit.SECONDS); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                return sb.toString().trim();
            } finally {
                p.destroyForcibly();
            }
        }

        private static String escapePs(String s) {
            return s.replace("'", "''");
        }
    }
}
