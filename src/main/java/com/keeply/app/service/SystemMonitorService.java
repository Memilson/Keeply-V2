package com.keeply.app.service;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import oshi.SystemInfo;
import oshi.hardware.CentralProcessor;
import oshi.hardware.GlobalMemory;
import oshi.hardware.HWDiskStore;
import oshi.hardware.HardwareAbstractionLayer;
import oshi.hardware.NetworkIF;
import oshi.software.os.FileSystem;
import oshi.software.os.OSFileStore;
import oshi.software.os.OSProcess;
import oshi.software.os.OperatingSystem;
import oshi.util.FormatUtil;

public class SystemMonitorService {

    private final SystemInfo systemInfo;
    private final HardwareAbstractionLayer hardware;
    private final OperatingSystem os;

    private long[] oldCpuTicks;

    private final Map<Integer, ProcSnap> prevProc = new ConcurrentHashMap<>();
    private final Map<Integer, Ewma> procCpuAvg = new ConcurrentHashMap<>();

    private final Map<String, NetSnap> prevNet = new ConcurrentHashMap<>();
    private final Map<String, DiskSnap> prevDisk = new ConcurrentHashMap<>();

    public SystemMonitorService() {
        this.systemInfo = new SystemInfo();
        this.hardware = systemInfo.getHardware();
        this.os = systemInfo.getOperatingSystem();
        this.oldCpuTicks = hardware.getProcessor().getSystemCpuLoadTicks();
    }

    // ---------------- CPU / MEM / UPTIME ----------------

    public double getCpuLoad() {
        CentralProcessor processor = hardware.getProcessor();
        double load = processor.getSystemCpuLoadBetweenTicks(oldCpuTicks);
        oldCpuTicks = processor.getSystemCpuLoadTicks();
        return clamp01(load);
    }

    public String getCpuModel() {
        return hardware.getProcessor().getProcessorIdentifier().getName();
    }

    public MemoryStats getMemoryStats() {
        GlobalMemory mem = hardware.getMemory();
        return new MemoryStats(mem.getAvailable(), mem.getTotal());
    }

    public String getUptime() {
        long uptimeSeconds = os.getSystemUptime();
        return FormatUtil.formatElapsedSecs(uptimeSeconds);
    }

    // ---------------- DISK (TOTAL USAGE + TOTAL IO) ----------------

    public DiskUsageTotals getDiskUsageTotals() {
        FileSystem fs = os.getFileSystem();
        long total = 0L;
        long usable = 0L;

        for (OSFileStore s : fs.getFileStores()) {
            long t = safeNonNeg(s.getTotalSpace());
            long u = safeNonNeg(s.getUsableSpace());
            if (t <= 0) continue;
            total += t;
            usable += Math.min(u, t);
        }

        long used = Math.max(0, total - usable);
        return new DiskUsageTotals(used, total);
    }

    public DiskTotals getDiskIoTotals(Duration intervalHint) {
        double dt = Math.max(0.5, intervalHint.toMillis() / 1000.0);
        Instant now = Instant.now();

        double readBps = 0.0;
        double writeBps = 0.0;

        List<HWDiskStore> disks = hardware.getDiskStores();
        for (HWDiskStore d : disks) {
            d.updateAttributes();

            String key = d.getName();
            long r = safeNonNeg(d.getReadBytes());
            long w = safeNonNeg(d.getWriteBytes());

            DiskSnap prev = prevDisk.get(key);
            if (prev != null) {
                long dr = safeNonNeg(r - prev.readBytes);
                long dw = safeNonNeg(w - prev.writeBytes);
                readBps += (dr / dt);
                writeBps += (dw / dt);
            }
            prevDisk.put(key, new DiskSnap(r, w, now));
        }

        return new DiskTotals(Math.max(0, readBps), Math.max(0, writeBps));
    }

    // ---------------- TOP PROCESSOS ----------------

    public List<ProcessRow> getTopProcessesCpu(int limit) {
        List<OSProcess> procs = os.getProcesses(
                p -> true,
                Comparator.comparingDouble(OSProcess::getProcessCpuLoadCumulative).reversed(),
                limit
        );
        return buildProcessRows(procs);
    }

    public List<ProcessRow> getTopProcessesRam(int limit) {
        List<OSProcess> procs = os.getProcesses(
                p -> true,
                Comparator.comparingLong(OSProcess::getResidentSetSize).reversed(),
                limit
        );
        return buildProcessRows(procs);
    }

    public List<ProcessRow> getTopProcessesDiskIo(int limit, Duration intervalHint) {
        int scanN = 60;

        List<OSProcess> cpuTop = os.getProcesses(
                p -> true,
                Comparator.comparingDouble(OSProcess::getProcessCpuLoadCumulative).reversed(),
                scanN
        );
        List<OSProcess> memTop = os.getProcesses(
                p -> true,
                Comparator.comparingLong(OSProcess::getResidentSetSize).reversed(),
                scanN
        );

        LinkedHashMap<Integer, OSProcess> uniq = new LinkedHashMap<>();
        for (OSProcess p : cpuTop) uniq.putIfAbsent(p.getProcessID(), p);
        for (OSProcess p : memTop) uniq.putIfAbsent(p.getProcessID(), p);

        List<ProcessRow> rows = buildProcessRows(new ArrayList<>(uniq.values()));

        rows.sort(Comparator.comparingDouble((ProcessRow r) -> (r.readBps() + r.writeBps())).reversed());
        return rows.subList(0, Math.min(limit, rows.size()));
    }

    private List<ProcessRow> buildProcessRows(List<OSProcess> procs) {
        Instant now = Instant.now();
        List<ProcessRow> rows = new ArrayList<>(procs.size());

        for (OSProcess p : procs) {
            int pid = p.getProcessID();

            ProcSnap prev = prevProc.get(pid);
            double dt = (prev == null) ? 0 : Math.max(0.5, Duration.between(prev.ts, now).toMillis() / 1000.0);

            double cpu = 0.0;
            double readBps = 0.0;
            double writeBps = 0.0;

            if (prev != null) {
                cpu = clamp01(p.getProcessCpuLoadBetweenTicks(prev.proc));

                long dr = safeNonNeg(p.getBytesRead() - prev.proc.getBytesRead());
                long dw = safeNonNeg(p.getBytesWritten() - prev.proc.getBytesWritten());
                readBps = dr / dt;
                writeBps = dw / dt;
            }

            Ewma ew = procCpuAvg.computeIfAbsent(pid, k -> new Ewma(0.25));
            ew.update(cpu);

            rows.add(new ProcessRow(
                    pid,
                    p.getName(),
                    cpu * 100.0,
                    ew.value() * 100.0,
                    safeNonNeg(p.getResidentSetSize()),
                    Math.max(0, readBps),
                    Math.max(0, writeBps)
            ));

            prevProc.put(pid, new ProcSnap(p, now));
        }

        return rows;
    }

    // ---------------- INTERNET (interfaces) ----------------

    public List<NetIfRow> getTopNetworkInterfaces(int limit, Duration intervalHint) {
        double dt = Math.max(0.5, intervalHint.toMillis() / 1000.0);
        Instant now = Instant.now();

        List<NetworkIF> ifs = hardware.getNetworkIFs();
        List<NetIfRow> rows = new ArrayList<>(ifs.size());

        for (NetworkIF nif : ifs) {
            nif.updateAttributes();
            String key = nif.getName();

            long recv = safeNonNeg(nif.getBytesRecv());
            long sent = safeNonNeg(nif.getBytesSent());

            NetSnap prev = prevNet.get(key);
            double recvBps = 0.0;
            double sentBps = 0.0;

            if (prev != null) {
                recvBps = Math.max(0, (recv - prev.recvBytes) / dt);
                sentBps = Math.max(0, (sent - prev.sentBytes) / dt);
            }

            rows.add(new NetIfRow(key, recvBps, sentBps));
            prevNet.put(key, new NetSnap(recv, sent, now));
        }

        rows.sort(Comparator.comparingDouble((NetIfRow r) -> (r.recvBps() + r.sentBps())).reversed());
        return rows.subList(0, Math.min(limit, rows.size()));
    }

    // ---------------- Records ----------------

    public record ProcessRow(int pid, String name, double cpuPct, double cpuAvgPct, long rssBytes, double readBps, double writeBps) {}
    public record NetIfRow(String name, double recvBps, double sentBps) {}

    public record DiskTotals(double readBps, double writeBps) {}
    private record DiskSnap(long readBytes, long writeBytes, Instant ts) {}

    private record ProcSnap(OSProcess proc, Instant ts) {}
    private record NetSnap(long recvBytes, long sentBytes, Instant ts) {}

    public record DiskUsageTotals(long usedBytes, long totalBytes) {
        public double usedPct() {
            return totalBytes > 0 ? (double) usedBytes / (double) totalBytes : 0.0;
        }
        public String usedString() { return FormatUtil.formatBytes(usedBytes); }
        public String totalString() { return FormatUtil.formatBytes(totalBytes); }
    }

    public record MemoryStats(long available, long total) {
        public double getUsagePercentage() {
            return total > 0 ? 1.0 - ((double) available / total) : 0;
        }
        public String getUsedString() {
            return FormatUtil.formatBytes(Math.max(0, total - available));
        }
        public String getTotalString() {
            return FormatUtil.formatBytes(Math.max(0, total));
        }
    }

    private static final class Ewma {
        private final double alpha;
        private boolean init = false;
        private double v = 0;
        Ewma(double alpha) { this.alpha = alpha; }
        void update(double x) {
            if (!init) { v = x; init = true; }
            else v = alpha * x + (1 - alpha) * v;
        }
        double value() { return v; }
    }

    private static double clamp01(double v) {
        if (Double.isNaN(v) || Double.isInfinite(v)) return 0.0;
        return Math.max(0.0, Math.min(1.0, v));
    }

    private static long safeNonNeg(long v) {
        return Math.max(0L, v);
    }
}
