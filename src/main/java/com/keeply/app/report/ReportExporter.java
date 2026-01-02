    package com.keeply.app.report;

    import com.keeply.app.Database.InventoryRow;
    import com.keeply.app.Database.ScanSummary;
    import com.openhtmltopdf.pdfboxout.PdfRendererBuilder;
    import com.typesafe.config.Config;
    import com.typesafe.config.ConfigFactory;
    import io.pebbletemplates.pebble.PebbleEngine;
    import io.pebbletemplates.pebble.loader.ClasspathLoader;
    import org.apache.commons.io.FileUtils;
    import org.apache.commons.lang3.StringUtils;
    import org.knowm.xchart.BitmapEncoder;
    import org.knowm.xchart.CategoryChartBuilder;
    import org.knowm.xchart.CategorySeries;
    import org.knowm.xchart.PieChartBuilder;
    import org.knowm.xchart.style.PieStyler;
    import org.knowm.xchart.style.Styler;
    import org.slf4j.Logger;
    import org.slf4j.LoggerFactory;

    import javax.imageio.ImageIO;
    import java.awt.image.BufferedImage;
    import java.io.*;
    import java.nio.charset.StandardCharsets;
    import java.time.ZoneId;
    import java.time.ZonedDateTime;
    import java.time.format.DateTimeFormatter;
    import java.util.*;

    import static java.util.Objects.requireNonNull;

    public final class ReportExporter {

        private static final Logger log = LoggerFactory.getLogger(ReportExporter.class);

        private static final String TEMPLATE_PATH = "report-inventory.peb";
        private static final String FONT_REGULAR = "/fonts/NotoSans-Regular.ttf";
        private static final String FONT_BOLD = "/fonts/NotoSans-Bold.ttf";

        private ReportExporter() {
        }

        public static void exportPdf(List<InventoryRow> rows, File file, ScanSummary scan) throws IOException {
            exportPdf(rows, file, scan, ReportOptions.load());
        }

        public static void exportPdf(List<InventoryRow> rows, File file, ScanSummary scan, ReportOptions opts)
                throws IOException {
            requireNonNull(file, "file");
            requireNonNull(opts, "opts");
            var safeRows = (rows == null) ? List.<InventoryRow>of() : rows;

            if (file.getParentFile() != null) {
                // cria diretório se não existir (robustez)
                // noinspection ResultOfMethodCallIgnored
                file.getParentFile().mkdirs();
            }

            // AWT headless (evita crash em servidor/VM sem GUI)
            System.setProperty("java.awt.headless", "true");

            var model = Model.from(safeRows, scan, opts);

            // Charts -> base64 PNG (pra template)
            var charts = buildCharts(model);

            // Render HTML via template
            var html = renderHtml(model, charts, opts);

            // HTML -> PDF
            try (var out = new BufferedOutputStream(new FileOutputStream(file))) {
                renderPdf(html, out, opts);
            }

            log.info("Report PDF exported: {}", file.getAbsolutePath());
        }

        // -------------------- HTML rendering (Pebble) --------------------

        private static String renderHtml(Model model, Charts charts, ReportOptions opts)
                throws IOException {
            var loader = new ClasspathLoader();
            loader.setCharset("UTF-8");
            // set prefix to the templates folder to avoid ambiguity across classloaders
            loader.setPrefix("templates");
            var engine = new PebbleEngine.Builder()
                    .loader(loader)
                    .autoEscaping(true)
                    .build();

            var template = engine.getTemplate(TEMPLATE_PATH);

            var ctx = new HashMap<String, Object>();
            ctx.put("m", model);
            ctx.put("c", charts);
            ctx.put("o", opts);
            ctx.put("fmt", new TemplateFormatters(opts));

            try (var sw = new StringWriter(32_768)) {
                template.evaluate(sw, ctx, Locale.ROOT);
                return sw.toString();
            }
        }

        // -------------------- PDF rendering (OpenHTMLtoPDF) --------------------

        private static void renderPdf(String html, OutputStream out, ReportOptions opts) throws IOException {
            try {
                var builder = new PdfRendererBuilder();
                builder.useFastMode();
                builder.toStream(out);
                // Sanitize leading characters that may break XML parsing in
                // underlying transformer (BOM or stray whitespace/newlines).
                int firstTag = html.indexOf('<');
                if (firstTag > 0) {
                    log.warn("Trimming {} leading chars from HTML before PDF render", firstTag);
                    html = html.substring(firstTag);
                }
                // Also remove leading BOM if present
                if (html.startsWith("\uFEFF")) html = html.substring(1);

                builder.withHtmlContent(html, null);

                // Fonts (se tiver no resources)
                registerFontIfPresent(builder, FONT_REGULAR, "Noto Sans", 400);
                registerFontIfPresent(builder, FONT_BOLD, "Noto Sans", 700);

                    // Page size (using defaults from HTML/CSS); explicit units caused
                    // incompatibility with some OpenHTMLToPDF versions, so we rely
                    // on the HTML/CSS @page or builder defaults instead.

                builder.run();
            } catch (Exception e) {
                throw new IOException("Failed to render PDF (OpenHTMLtoPDF)", e);
            }
        }

        private static void registerFontIfPresent(PdfRendererBuilder builder, String resourcePath, String family,
                int weight) {
            var is = ReportExporter.class.getResourceAsStream(resourcePath);
            if (is == null) {
                log.warn("Font not found in resources: {}", resourcePath);
                return;
            }
            builder.useFont(() -> is, family, weight, PdfRendererBuilder.FontStyle.NORMAL, true);
        }

        // -------------------- Options --------------------

        public record ReportOptions(
                Locale locale,
                ZoneId zoneId,
                int topFiles,
                int topTypes,
                int topFolders,
                boolean landscape,
                String pageSize, // "LETTER" | "A4"
                Theme theme) {
            public static ReportOptions load() {
                Config cfg = ConfigFactory.load(); // application.conf / reference.conf
                var locale = Locale.forLanguageTag(get(cfg, "keeply.report.locale", "pt-BR"));
                var zoneId = ZoneId.of(get(cfg, "keeply.report.zoneId", "America/Sao_Paulo"));

                int topFiles = getInt(cfg, "keeply.report.topFiles", 20);
                int topTypes = getInt(cfg, "keeply.report.topTypes", 12);
                int topFolders = getInt(cfg, "keeply.report.topFolders", 12);

                var pageSize = get(cfg, "keeply.report.pageSize", "LETTER").toUpperCase(Locale.ROOT);
                var landscape = getBool(cfg, "keeply.report.landscape", false);

                var theme = new Theme(
                        get(cfg, "keeply.report.theme.headerBg", "#18181B"),
                        get(cfg, "keeply.report.theme.accent", "#06B6D4"),
                        get(cfg, "keeply.report.theme.textMain", "#1E1E1E"),
                        get(cfg, "keeply.report.theme.textMuted", "#646464"),
                        get(cfg, "keeply.report.theme.rowAlt", "#F8FAFC"));

                return new ReportOptions(locale, zoneId, topFiles, topTypes, topFolders, landscape, pageSize, theme);
            }

            public float pageWidthPoints() {
                // 1 point = 1/72 inch
                return switch (pageSize) {
                    case "A4" -> landscape ? 841.89f : 595.28f;
                    case "LETTER" -> landscape ? 792f : 612f;
                    default -> landscape ? 792f : 612f;
                };
            }

            public float pageHeightPoints() {
                return switch (pageSize) {
                    case "A4" -> landscape ? 595.28f : 841.89f;
                    case "LETTER" -> landscape ? 612f : 792f;
                    default -> landscape ? 612f : 792f;
                };
            }
        }

        public record Theme(
                String headerBg,
                String accent,
                String textMain,
                String textMuted,
                String rowAlt) {
        }

        private static String get(Config cfg, String path, String def) {
            try {
                return cfg.hasPath(path) ? cfg.getString(path) : def;
            } catch (Exception ignored) {
                return def;
            }
        }

        private static int getInt(Config cfg, String path, int def) {
            try {
                return cfg.hasPath(path) ? cfg.getInt(path) : def;
            } catch (Exception ignored) {
                return def;
            }
        }

        private static boolean getBool(Config cfg, String path, boolean def) {
            try {
                return cfg.hasPath(path) ? cfg.getBoolean(path) : def;
            } catch (Exception ignored) {
                return def;
            }
        }

        // -------------------- Template helper --------------------

        public static final class TemplateFormatters {
            private final ReportOptions opts;
            private final DateTimeFormatter dtf;

            TemplateFormatters(ReportOptions opts) {
                this.opts = opts;
                this.dtf = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss", opts.locale());
            }

            public String bytes(long b) {
                // “não reinventar roda” (commons-io)
                return FileUtils.byteCountToDisplaySize(b);
            }

            public String intN(long n) {
                var nf = java.text.NumberFormat.getIntegerInstance(opts.locale());
                return nf.format(n);
            }

            public String abbreviateMiddle(String s, int max) {
                if (s == null)
                    return "";
                // “não reinventar roda” (commons-lang3)
                return StringUtils.abbreviateMiddle(s, "…", Math.max(8, max));
            }

            public String now() {
                return ZonedDateTime.now(opts.zoneId()).format(dtf);
            }
        }

        // -------------------- Charts (xchart) --------------------

        public static final class Charts {
            public final String typesBarPngBase64;
            public final String statusPiePngBase64;

            public Charts(String typesBarPngBase64, String statusPiePngBase64) {
                this.typesBarPngBase64 = typesBarPngBase64;
                this.statusPiePngBase64 = statusPiePngBase64;
            }
        }

        public static Charts buildCharts(Model model) {
            String types = null;
            String status = null;

            try { types = toBase64Png(buildTypesBar(model.topTypes())); } catch (Exception ignored) {}
            try { status = toBase64Png(buildStatusPie(model.statusCounts())); } catch (Exception ignored) {}

            return new Charts(types, status);
        }

        private static BufferedImage buildTypesBar(List<Model.TypeStat> types) {
            var chart = new CategoryChartBuilder()
                    .width(900).height(420)
                    .title("")
                    .xAxisTitle("")
                    .yAxisTitle("MB")
                    .build();

            var styler = chart.getStyler();
            styler.setLegendVisible(false);
            styler.setPlotGridLinesVisible(false);
            styler.setChartTitleVisible(false);
            styler.setXAxisLabelRotation(35);
            styler.setAvailableSpaceFill(0.75);
            styler.setDefaultSeriesRenderStyle(CategorySeries.CategorySeriesRenderStyle.Bar);
            styler.setYAxisDecimalPattern("0");

            var x = new java.util.ArrayList<String>(types.size());
            var y = new java.util.ArrayList<Double>(types.size());

            for (var t : types) {
                x.add(t.ext());
                y.add(t.bytes() / 1024d / 1024d);
            }

            chart.addSeries("Types", x, y);
            return BitmapEncoder.getBufferedImage(chart);
        }

        private static BufferedImage buildStatusPie(Map<String, Long> counts) {
            var chart = new PieChartBuilder()
                    .width(520).height(360)
                    .title("")
                    .build();

            var styler = chart.getStyler();
            styler.setLegendVisible(true);
            styler.setChartTitleVisible(false);
            styler.setPlotContentSize(0.85);

            long newC = counts.getOrDefault("NEW", 0L);
            long modC = counts.getOrDefault("MODIFIED", 0L);
            long stC  = counts.getOrDefault("STABLE", 0L);
            long otC  = counts.getOrDefault("OTHER", 0L);

            if (newC + modC + stC + otC == 0) {
                chart.addSeries("No data", 1);
            } else {
                if (newC > 0) chart.addSeries("NEW", newC);
                if (modC > 0) chart.addSeries("MODIFIED", modC);
                if (stC > 0) chart.addSeries("STABLE", stC);
                if (otC > 0) chart.addSeries("OTHER", otC);
            }

            return BitmapEncoder.getBufferedImage(chart);
        }

        private static String toBase64Png(BufferedImage img) throws IOException {
            try (var baos = new ByteArrayOutputStream()) {
                ImageIO.write(img, "png", baos);
                return Base64.getEncoder().encodeToString(baos.toByteArray());
            }
        }

        // -------------------- Model (aggregations) --------------------

        public record Model(
                Summary summary,
                Map<String, Long> statusCounts,
                List<TypeStat> topTypes,
                List<FolderStat> topFolders,
                List<TopFile> topFiles
        ) {
            public static Model from(List<InventoryRow> rows, ScanSummary scan, ReportOptions opts) {
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

                return new Model(summary, statusCounts, topTypes, topFolders, topFilesList);
            }

            public record Summary(
                    String rootPath,
                    int totalFiles,
                    long totalBytes,
                    Object scanId,
                    String finishedAt
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
    }
