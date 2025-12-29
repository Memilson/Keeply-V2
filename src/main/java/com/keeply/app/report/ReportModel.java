package com.keeply.app.report;

import com.keeply.app.Database.InventoryRow;
import com.keeply.app.Database.ScanSummary;
import org.apache.commons.lang3.StringUtils;

import java.util.*;

import static java.util.Objects.requireNonNull;

public record ReportModel(
        Summary summary,
        Map<String, Long> statusCounts,
        List<TypeStat> topTypes,
        List<FolderStat> topFolders,
        List<TopFile> topFiles
) {
    public static ReportModel from(List<InventoryRow> rows, ScanSummary scan, ReportExporter.ReportOptions opts) {
        requireNonNull(rows, "rows");
        requireNonNull(opts, "opts");

        long totalBytes = 0;
        var statusCounts = new HashMap<String, Long>(8);

        // Ext -> [bytes, count]
        var typeAgg = new HashMap<String, long[]>(256);

        // Folder -> [bytes, count]
        var folderAgg = new HashMap<String, long[]>(4096);

        // top files min-heap
        var topFiles = new PriorityQueue<TopFile>(Comparator.comparingLong(TopFile::sizeBytes));

        for (var r : rows) {
            long size = Math.max(0L, r.sizeBytes());
            totalBytes += size;

            var status = normalizeStatus(r.status());
            statusCounts.merge(status, 1L, Long::sum);

            var ext = extensionOf(r);
            typeAgg.computeIfAbsent(ext, __ -> new long[2]);
            typeAgg.get(ext)[0] += size;
            typeAgg.get(ext)[1] += 1;

            var folder = extractFolder(r.pathRel());
            folderAgg.computeIfAbsent(folder, __ -> new long[2]);
            folderAgg.get(folder)[0] += size;
            folderAgg.get(folder)[1] += 1;

            // top N files
            var tf = new TopFile(
                    safe(r.name(), fallbackName(r)),
                    safe(r.pathRel(), "-"),
                    safe(r.rootPath(), scan != null ? scan.rootPath() : "-"),
                    size
            );

            int limit = Math.max(1, opts.topFiles());
            if (topFiles.size() < limit) {
                topFiles.add(tf);
            } else if (tf.sizeBytes() > Objects.requireNonNull(topFiles.peek()).sizeBytes()) {
                topFiles.poll();
                topFiles.add(tf);
            }
        }

        var rootPath = (scan != null && scan.rootPath() != null && !scan.rootPath().isBlank())
                ? scan.rootPath()
                : (rows.isEmpty() ? "-" : safe(rows.getFirst().rootPath(), "-"));

        var summary = new Summary(
                rootPath,
                rows.size(),
                totalBytes,
                scan == null ? null : scan.scanId(),
                scan == null ? null : scan.finishedAt()
        );

        var topTypes = topNTypeStats(typeAgg, opts.topTypes());
        var topFolders = topNFolderStats(folderAgg, opts.topFolders());

        // heap -> sorted desc
        var topFilesList = new ArrayList<>(topFiles);
        topFilesList.sort(Comparator.comparingLong(TopFile::sizeBytes).reversed());

        return new ReportModel(summary, statusCounts, topTypes, topFolders, topFilesList);
    }

    public record Summary(
            String rootPath,
            int totalFiles,
            long totalBytes,
            Object scanId,       // mantém compatível com o teu ScanSummary (sem supor tipo)
            String finishedAt    // idem (no teu código atual é String)
    ) {}

    public record TypeStat(String ext, long bytes, long count) {}
    public record FolderStat(String folder, long bytes, long count) {}
    public record TopFile(String name, String pathRel, String rootPath, long sizeBytes) {}

    private static List<TypeStat> topNTypeStats(Map<String, long[]> agg, int limit) {
        limit = Math.max(1, limit);
        var pq = new PriorityQueue<TypeStat>(Comparator.comparingLong(TypeStat::bytes));
        for (var e : agg.entrySet()) {
            var a = e.getValue();
            var stat = new TypeStat(e.getKey(), a[0], a[1]);
            if (pq.size() < limit) pq.add(stat);
            else if (stat.bytes() > Objects.requireNonNull(pq.peek()).bytes()) { pq.poll(); pq.add(stat); }
        }
        var out = new ArrayList<>(pq);
        out.sort(Comparator.comparingLong(TypeStat::bytes).reversed());
        return out;
    }

    private static List<FolderStat> topNFolderStats(Map<String, long[]> agg, int limit) {
        limit = Math.max(1, limit);
        var pq = new PriorityQueue<FolderStat>(Comparator.comparingLong(FolderStat::bytes));
        for (var e : agg.entrySet()) {
            var a = e.getValue();
            var stat = new FolderStat(e.getKey(), a[0], a[1]);
            if (pq.size() < limit) pq.add(stat);
            else if (stat.bytes() > Objects.requireNonNull(pq.peek()).bytes()) { pq.poll(); pq.add(stat); }
        }
        var out = new ArrayList<>(pq);
        out.sort(Comparator.comparingLong(FolderStat::bytes).reversed());
        return out;
    }

    private static String normalizeStatus(String status) {
        if (status == null) return "OTHER";
        var s = status.trim().toUpperCase(Locale.ROOT);
        return switch (s) {
            case "NEW", "MODIFIED", "STABLE" -> s;
            default -> "OTHER";
        };
    }

    private static String extensionOf(InventoryRow row) {
        var name = safe(row.name(), row.pathRel());
        if (name.isBlank()) return "NO_EXT";
        int idx = name.lastIndexOf('.');
        if (idx <= 0 || idx >= name.length() - 1) return "NO_EXT";
        return ("." + name.substring(idx + 1)).toUpperCase(Locale.ROOT);
    }

    private static String extractFolder(String pathRel) {
        if (pathRel == null || pathRel.isBlank()) return "<root>";
        var normalized = pathRel.replace('\\', '/');
        int idx = normalized.lastIndexOf('/');
        if (idx <= 0) return "<root>";
        var folder = normalized.substring(0, idx);
        return folder.isBlank() ? "<root>" : folder;
    }

    private static String safe(String v, String def) {
        if (v == null) return def;
        var s = v.trim();
        return s.isEmpty() ? def : s;
    }

    private static String fallbackName(InventoryRow r) {
        var rel = safe(r.pathRel(), "");
        if (rel.isBlank()) return "unknown";
        var n = rel.replace('\\', '/');
        int idx = n.lastIndexOf('/');
        return idx >= 0 && idx < n.length() - 1 ? n.substring(idx + 1) : StringUtils.defaultIfBlank(n, "unknown");
    }
}
