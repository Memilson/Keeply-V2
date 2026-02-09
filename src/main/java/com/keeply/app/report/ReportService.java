    package com.keeply.app.report;

    import com.keeply.app.database.DatabaseBackup.InventoryRow;
    import com.keeply.app.database.DatabaseBackup.ScanSummary;
    import com.openhtmltopdf.pdfboxout.PdfRendererBuilder;
    import io.pebbletemplates.pebble.PebbleEngine;
    import io.pebbletemplates.pebble.extension.AbstractExtension;
    import io.pebbletemplates.pebble.extension.Filter;
    import io.pebbletemplates.pebble.loader.ClasspathLoader;
    import io.pebbletemplates.pebble.template.EvaluationContext;
    import io.pebbletemplates.pebble.template.PebbleTemplate;
    import org.apache.commons.io.FileUtils;
    import org.apache.commons.lang3.StringUtils;
    import org.jsoup.Jsoup;
    import org.jsoup.nodes.Document;
    import org.jsoup.nodes.Entities;
    import org.knowm.xchart.CategoryChartBuilder;
    import org.knowm.xchart.PieChartBuilder;
    import org.knowm.xchart.style.CategoryStyler;
    import org.knowm.xchart.style.PieStyler;
    import org.slf4j.Logger;
    import org.slf4j.LoggerFactory;

    import java.io.*;
    import java.lang.reflect.Method;
    import java.nio.file.AtomicMoveNotSupportedException;
    import java.nio.file.Files;
    import java.nio.file.Path;
    import java.nio.file.StandardCopyOption;
    import java.text.NumberFormat;
    import java.time.ZoneId;
    import java.time.ZonedDateTime;
    import java.time.format.DateTimeFormatter;
    import java.util.*;
    import java.util.concurrent.CompletableFuture;
    import java.util.concurrent.Executors;
    import java.util.stream.Collector;

    import static java.util.stream.Collectors.*;

    public class ReportService {

        private static final Logger log = LoggerFactory.getLogger(ReportService.class);

        // ===================== Pebble =====================

        private static final class RoundFilterExtension extends AbstractExtension {
            @Override
            public Map<String, Filter> getFilters() {
                return Map.of("round", new RoundFilter());
            }
        }

        private static final class RoundFilter implements Filter {
            @Override
            public Object apply(Object input, Map<String, Object> args, PebbleTemplate self, EvaluationContext context,
                                int lineNumber) {
                double value = toDouble(input);
                int precision = 0;
                if (args != null) {
                    Object arg = args.get("precision");
                    if (arg instanceof Number n) precision = n.intValue();
                }
                double rounded = round(value, precision);
                if (precision <= 0) return (long) rounded;
                return rounded;
            }

            @Override
            public List<String> getArgumentNames() {
                return List.of("precision");
            }

            private static double round(double value, int precision) {
                if (precision <= 0) return Math.round(value);
                double factor = Math.pow(10d, precision);
                return Math.round(value * factor) / factor;
            }

            private static double toDouble(Object input) {
                if (input instanceof Number n) return n.doubleValue();
                if (input == null) return 0d;
                try {
                    return Double.parseDouble(input.toString());
                } catch (NumberFormatException e) {
                    return 0d;
                }
            }
        }

        private static final PebbleEngine PEBBLE_ENGINE = new PebbleEngine.Builder()
                .loader(new ClasspathLoader() {{
                    setPrefix("templates");
                    setCharset("UTF-8");
                }})
                .autoEscaping(false)
                .defaultLocale(Locale.ROOT)
                .extension(new RoundFilterExtension())
                .build();

        private final ReportOptions defaultOptions;

        public ReportService() {
            this(ReportOptions.load());
        }

        public ReportService(ReportOptions defaultOptions) {
            this.defaultOptions = defaultOptions;
        }

        public void exportPdf(List<InventoryRow> rows, File file, ScanSummary scan) throws IOException {
            exportPdf(rows, file, scan, defaultOptions);
        }

        public void exportPdf(List<InventoryRow> rows, File file, ScanSummary scan, ReportOptions opts) throws IOException {
            Objects.requireNonNull(file, "file");
            System.setProperty("java.awt.headless", "true");

            var safeRows = (rows == null) ? List.<InventoryRow>of() : rows;
            var target = file.toPath();

            var model = buildModel(safeRows, scan, opts);
            var charts = buildChartsRaster(model, opts); // <<< PNG base64 (não SVG)
            var html = renderHtml(model, charts, opts);

            writeAtomically(target, tmp -> renderPdf(html, tmp));
        }

        public CompletableFuture<File> exportPdfAsync(List<InventoryRow> rows, File file, ScanSummary scan) {
            var executor = Executors.newVirtualThreadPerTaskExecutor();
            return CompletableFuture.supplyAsync(() -> {
                try {
                    exportPdf(rows, file, scan);
                    return file;
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                } finally {
                    executor.close();
                }
            }, executor);
        }

        private String renderHtml(Model model, Model.Charts charts, ReportOptions opts) throws IOException {
            var context = new HashMap<String, Object>();
            context.put("m", model);
            context.put("c", charts);
            context.put("o", opts);
            context.put("fmt", new TemplateFormatters(opts));

            try (var writer = new StringWriter()) {
                PEBBLE_ENGINE.getTemplate("report-inventory.peb").evaluate(writer, context, opts.locale());
                String rawHtml = writer.toString();

                Document doc = Jsoup.parse(rawHtml);
                doc.outputSettings()
                        .syntax(Document.OutputSettings.Syntax.xml)
                        .escapeMode(Entities.EscapeMode.xhtml)
                        .charset("UTF-8")
                        .prettyPrint(false);

                return doc.html();
            }
        }

        private void renderPdf(String html, OutputStream out) throws IOException {
            var builder = new PdfRendererBuilder();
            builder.useFastMode();

            // Base URL pros assets do classpath (fonts, etc)
            builder.withHtmlContent(html, ReportService.class.getResource("/templates/").toExternalForm());
            builder.toStream(out);

            loadFont(builder, "/fonts/NotoSans-Regular.ttf", "Noto Sans", 400);
            loadFont(builder, "/fonts/NotoSans-Bold.ttf", "Noto Sans", 700);

            builder.run();
        }

        private void loadFont(PdfRendererBuilder builder, String path, String family, int weight) {
            if (getClass().getResource(path) != null) {
                builder.useFont(() -> getClass().getResourceAsStream(path), family, weight,
                        PdfRendererBuilder.FontStyle.NORMAL, true);
            }
        }

        // ===================== Charts (PNG base64) =====================

        private Model.Charts buildChartsRaster(Model model, ReportOptions opts) {
            String typesSizePng = "";
            String typesCountPng = "";
            String statusPng = "";

            try {
                var xTypes = model.topTypes().stream().map(Model.TypeStat::ext).toList();

                // --- 1) Tipos por TAMANHO (MB/GB automático)
                long maxBytes = model.topTypes().stream().mapToLong(Model.TypeStat::bytes).max().orElse(0L);
                boolean useGb = maxBytes >= 1024L * 1024L * 1024L;
                double divisor = useGb ? (1024d * 1024d * 1024d) : (1024d * 1024d);
                String unit = useGb ? "GB" : "MB";

                var ySize = model.topTypes().stream().map(t -> t.bytes() / divisor).toList();
                if (!xTypes.isEmpty()) {
                    var sizeChart = new CategoryChartBuilder()
                            .width(1400).height(520)
                            .build();

                    styleCategoryChart(sizeChart.getStyler(), opts);
                    sizeChart.setYAxisTitle("Tamanho (" + unit + ")");
                    sizeChart.setXAxisTitle("Extensão");
                    sizeChart.addSeries("Tamanho", xTypes, ySize);

                    typesSizePng = toPngDataUri(sizeChart);
                }

                // --- 2) Tipos por QUANTIDADE
                var yCount = model.topTypes().stream().map(t -> (double) t.count()).toList();
                if (!xTypes.isEmpty()) {
                    var countChart = new CategoryChartBuilder()
                            .width(1400).height(520)
                            .build();

                    styleCategoryChart(countChart.getStyler(), opts);
                    countChart.setYAxisTitle("Arquivos");
                    countChart.setXAxisTitle("Extensão");
                    countChart.addSeries("Arquivos", xTypes, yCount);

                    typesCountPng = toPngDataUri(countChart);
                }

                // --- 3) Status (Pie)
                var pie = new PieChartBuilder()
                        .width(1100).height(520)
                        .build();

                var ps = pie.getStyler();
                ps.setChartTitleVisible(false);
                ps.setLegendPosition(PieStyler.LegendPosition.OutsideE);
                tryInvoke(ps, "setChartBackgroundColor", awtWhite());
                ps.setPlotBorderVisible(false);

                // cores (SaaS)
                    tryInvokeSeriesColors(ps, new Object[]{
                        awtDecode(opts.theme().accent()),
                        awtColor(34, 197, 94),
                        awtColor(251, 191, 36),
                        awtColor(148, 163, 184)
                    });

                // ordem estável
                List<String> keys = List.of("NEW", "MODIFIED", "STABLE", "OTHER");
                for (var k : keys) {
                    var v = model.statusCounts().getOrDefault(k, 0L);
                    if (v > 0) pie.addSeries(k, v);
                }
                if (pie.getSeriesMap().isEmpty()) pie.addSeries("Sem dados", 1);

                statusPng = toPngDataUri(pie);

            } catch (IOException | RuntimeException e) {
                log.error("Erro ao gerar gráficos (PNG)", e);
            }

            return new Model.Charts(typesSizePng, typesCountPng, statusPng);
        }

        private void styleCategoryChart(CategoryStyler s, ReportOptions opts) {
            s.setLegendVisible(false);
            s.setChartTitleVisible(false);

            tryInvoke(s, "setChartBackgroundColor", awtWhite());
            tryInvoke(s, "setPlotBackgroundColor", awtWhite());

            s.setPlotGridLinesVisible(true);
            tryInvoke(s, "setPlotGridLinesColor", awtColor(226, 232, 240));
            s.setPlotBorderVisible(false);

            tryInvokeSeriesColors(s, new Object[]{awtDecode(opts.theme().accent())});

            // legibilidade
            s.setXAxisLabelRotation(35);
            s.setAvailableSpaceFill(0.68);
            s.setOverlapped(false);

            // “tenta” deixar texto mais escuro sem depender da versão do XChart
            tryInvoke(s, "setChartFontColor", awtColor(15, 23, 42));
            tryInvoke(s, "setAxisTickLabelsColor", awtColor(15, 23, 42));
            tryInvoke(s, "setAxisTitleColor", awtColor(15, 23, 42));
        }

        private static void tryInvoke(Object target, String method, Object value) {
            if (target == null || value == null) return;
            try {
                Method m = target.getClass().getMethod(method, value.getClass());
                m.invoke(target, value);
            } catch (ReflectiveOperationException | SecurityException ignored) {
                // compatibilidade entre versões do XChart
            }
        }

        private static void tryInvokeSeriesColors(Object target, Object[] colors) {
            if (target == null || colors == null || colors.length == 0 || colors[0] == null) return;
            try {
                Class<?> colorClass = colors[0].getClass();
                Object array = java.lang.reflect.Array.newInstance(colorClass, colors.length);
                for (int i = 0; i < colors.length; i++) {
                    if (colors[i] == null) return;
                    java.lang.reflect.Array.set(array, i, colors[i]);
                }
                Method m = target.getClass().getMethod("setSeriesColors", array.getClass());
                m.invoke(target, array);
            } catch (ReflectiveOperationException | SecurityException ignored) {
                // compatibilidade entre versões do XChart
            }
        }

        private static Object awtColor(int r, int g, int b) {
            try {
                Class<?> color = Class.forName("java.awt.Color");
                return color.getConstructor(int.class, int.class, int.class).newInstance(r, g, b);
            } catch (ReflectiveOperationException | SecurityException e) {
                return null;
            }
        }

        private static Object awtDecode(String hex) {
            try {
                Class<?> color = Class.forName("java.awt.Color");
                Method decode = color.getMethod("decode", String.class);
                return decode.invoke(null, hex);
            } catch (ReflectiveOperationException | SecurityException e) {
                return null;
            }
        }

        private static Object awtWhite() {
            try {
                Class<?> color = Class.forName("java.awt.Color");
                return color.getField("WHITE").get(null);
            } catch (ReflectiveOperationException | SecurityException e) {
                return null;
            }
        }

        private static String toPngDataUri(Object chart) throws IOException {
            byte[] png = chartToPngBytes(chart);
            return "data:image/png;base64," + Base64.getEncoder().encodeToString(png);
        }

        /**
         * Faz PNG via XChart BitmapEncoder usando reflection (compatível com variações de versão).
         */
        private static byte[] chartToPngBytes(Object chart) throws IOException {
            try {
                Class<?> enc = Class.forName("org.knowm.xchart.BitmapEncoder");
                Class<?> fmt = Class.forName("org.knowm.xchart.BitmapEncoder$BitmapFormat");
                Class<?> enumType = fmt.asSubclass(Enum.class);
                @SuppressWarnings({"unchecked", "rawtypes"})
                Enum<?> pngEnum = Enum.valueOf((Class) enumType, "PNG");

                // 1) tenta getBitmapBytes(chart, BitmapFormat.PNG)
                for (Method m : enc.getMethods()) {
                    if (!m.getName().equals("getBitmapBytes")) continue;
                    if (m.getParameterCount() != 2) continue;
                    if (!m.getParameterTypes()[0].isAssignableFrom(chart.getClass())) continue;
                    if (!m.getParameterTypes()[1].isAssignableFrom(fmt)) continue;
                    Object out = m.invoke(null, chart, pngEnum);
                    if (out instanceof byte[] bytes) return bytes;
                }

                // 2) fallback: getBufferedImage(chart) + ImageIO
                for (Method m : enc.getMethods()) {
                    if (!m.getName().equals("getBufferedImage")) continue;
                    if (m.getParameterCount() != 1) continue;
                    if (!m.getParameterTypes()[0].isAssignableFrom(chart.getClass())) continue;

                    Object img = m.invoke(null, chart);
                    // evita import direto de BufferedImage/ImageIO no caso de classpath estranho
                    var baos = new ByteArrayOutputStream();
                    Class<?> imageIO = Class.forName("javax.imageio.ImageIO");
                    Method write = imageIO.getMethod("write",
                            Class.forName("java.awt.image.RenderedImage"),
                            String.class,
                            OutputStream.class);
                    write.invoke(null, img, "png", baos);
                    return baos.toByteArray();
                }

                throw new IOException("BitmapEncoder não encontrou método compatível para gerar PNG.");

            } catch (IOException e) {
                throw e;
            } catch (ReflectiveOperationException | SecurityException e) {
                throw new IOException("Falha ao gerar PNG do gráfico via BitmapEncoder.", e);
            }
        }

        // ===================== Processamento =====================

        @SuppressWarnings("unchecked")
        private Model buildModel(List<InventoryRow> rows, ScanSummary scan, ReportOptions opts) {
            Collector<InventoryRow, ?, Long> totalBytesCol = summingLong(InventoryRow::sizeBytes);
            Collector<InventoryRow, ?, Map<String, Long>> statusCol = groupingBy(r -> normalizeStatus(r.status()), counting());

            Collector<InventoryRow, ?, Map<String, long[]>> typesCol = groupingBy(
                    ReportService::extensionOf,
                    collectingAndThen(summarizingLong(InventoryRow::sizeBytes),
                            s -> new long[]{s.getSum(), s.getCount()}));

            Collector<InventoryRow, ?, Map<String, long[]>> foldersCol = groupingBy(
                    r -> extractFolder(r.pathRel()),
                    collectingAndThen(summarizingLong(InventoryRow::sizeBytes),
                            s -> new long[]{s.getSum(), s.getCount()}));

            int limitFiles = opts.topFiles();
            Collector<InventoryRow, ?, List<Model.TopFile>> topFilesCol = Collector.of(
                    () -> new PriorityQueue<Model.TopFile>(Comparator.comparingLong(Model.TopFile::sizeBytes)),
                    (pq, r) -> {
                        pq.offer(new Model.TopFile(fallbackName(r), r.pathRel(), r.rootPath(), r.sizeBytes()));
                        if (pq.size() > limitFiles) pq.poll();
                    },
                    (pq1, pq2) -> {
                        pq1.addAll(pq2);
                        while (pq1.size() > limitFiles) pq1.poll();
                        return pq1;
                    },
                    pq -> {
                        var l = new ArrayList<>(pq);
                        l.sort(Comparator.comparingLong(Model.TopFile::sizeBytes).reversed());
                        return l;
                    });

            var results = rows.stream().collect(teeing(
                    totalBytesCol,
                    teeing(statusCol,
                            teeing(typesCol,
                                    teeing(foldersCol, topFilesCol, List::of),
                                    List::of),
                            List::of),
                    List::of));

            long totalBytes = (long) results.get(0);
            var l1 = (List<?>) results.get(1);
            var statusMap = (Map<String, Long>) l1.get(0);
            var l2 = (List<?>) l1.get(1);
            var typesMap = (Map<String, long[]>) l2.get(0);
            var l3 = (List<?>) l2.get(1);
            var foldersMap = (Map<String, long[]>) l3.get(0);
            var topFiles = (List<Model.TopFile>) l3.get(1);

            var topTypes = typesMap.entrySet().stream()
                    .map(e -> new Model.TypeStat(e.getKey(), e.getValue()[0], e.getValue()[1]))
                    .sorted(Comparator.comparingLong(Model.TypeStat::bytes).reversed())
                    .limit(opts.topTypes())
                    .toList();

            var topFolders = foldersMap.entrySet().stream()
                    .map(e -> new Model.FolderStat(e.getKey(), e.getValue()[0], e.getValue()[1]))
                    .sorted(Comparator.comparingLong(Model.FolderStat::bytes).reversed())
                    .limit(opts.topFolders())
                    .toList();

            List.of("NEW", "MODIFIED", "STABLE", "OTHER").forEach(k -> statusMap.putIfAbsent(k, 0L));
            var root = (scan != null) ? scan.rootPath() : "-";

            return new Model(
                    new Model.Summary(root, rows.size(), totalBytes,
                            (scan != null) ? scan.scanId() : null,
                            (scan != null) ? scan.finishedAt() : null),
                    statusMap, topTypes, topFolders, topFiles);
        }

        private void writeAtomically(Path target, IOSubroutine action) throws IOException {
            Files.createDirectories(target.getParent());
            Path tmp = Files.createTempFile(target.getParent(), target.getFileName().toString(), ".tmp");
            try {
                try (var out = new BufferedOutputStream(Files.newOutputStream(tmp))) {
                    action.execute(out);
                }
                try {
                    Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
                } catch (AtomicMoveNotSupportedException e) {
                    Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING);
                }
            } catch (IOException | RuntimeException e) {
                Files.deleteIfExists(tmp);
                throw (e instanceof IOException io) ? io : new IOException(e);
            }
        }

        @FunctionalInterface
        interface IOSubroutine {
            void execute(OutputStream out) throws IOException;
        }

        // Helpers
        private static String normalizeStatus(String s) {
            return (s == null) ? "OTHER" : switch (s.toUpperCase().trim()) {
                case "NEW", "MODIFIED", "STABLE" -> s.toUpperCase().trim();
                default -> "OTHER";
            };
        }

        private static String extensionOf(InventoryRow r) {
            String n = StringUtils.firstNonBlank(r.name(), r.pathRel(), "");
            int i = n.lastIndexOf('.');
            return (i > 0 && i < n.length() - 1) ? ("." + n.substring(i + 1)).toUpperCase() : "NO_EXT";
        }

        private static String extractFolder(String path) {
            if (path == null) return "<root>";
            String p = path.replace('\\', '/');
            int i = p.lastIndexOf('/');
            return (i <= 0) ? "<root>" : p.substring(0, i);
        }

        private static String fallbackName(InventoryRow r) {
            return StringUtils.isNotBlank(r.name()) ? r.name() : "unknown";
        }

        // Records
        public record Model(Summary summary,
                            Map<String, Long> statusCounts,
                            List<TypeStat> topTypes,
                            List<FolderStat> topFolders,
                            List<TopFile> topFiles) {

            @SuppressWarnings("unused")
            public record Summary(String rootPath, int totalFiles, long totalBytes, Object scanId, String finishedAt) {}
            @SuppressWarnings("unused")
            public record TypeStat(String ext, long bytes, long count) {}
            @SuppressWarnings("unused")
            public record FolderStat(String folder, long bytes, long count) {}
            @SuppressWarnings("unused")
            public record TopFile(String name, String pathRel, String rootPath, long sizeBytes) {}

            // PNG base64 (data URI)
            @SuppressWarnings("unused")
            public record Charts(String typesSizePng, String typesCountPng, String statusPng) {}
        }

        @SuppressWarnings("unused")
        public record ReportOptions(Locale locale, ZoneId zoneId, int topFiles, int topTypes, int topFolders, Theme theme) {
            public static ReportOptions load() {
                return new ReportOptions(
                        Locale.forLanguageTag("pt-BR"),
                        ZoneId.systemDefault(),
                        20,
                        12,
                        12,
                        new Theme("#09090b", "#06B6D4", "#18181b", "#52525b", "#f8fafc")
                );
            }
        }

        @SuppressWarnings("unused")
        public record Theme(String headerBg, String accent, String textMain, String textMuted, String rowAlt) {}

        public static class TemplateFormatters {
            private final ReportOptions opts;
            private final NumberFormat nf;
            private final DateTimeFormatter dtf;

            public TemplateFormatters(ReportOptions opts) {
                this.opts = opts;
                this.nf = NumberFormat.getIntegerInstance(opts.locale());
                this.dtf = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm", opts.locale());
            }

            public String bytes(Number b) {
                return FileUtils.byteCountToDisplaySize(b != null ? b.longValue() : 0);
            }

            public String format(Number n) {
                return nf.format(n != null ? n : 0);
            }

            public String now() {
                return ZonedDateTime.now(opts.zoneId()).format(dtf);
            }

            public String pathFull(String root, String rel) {
                String r = (root == null) ? "" : root;
                String p = (rel == null) ? "" : rel;
                if (r.isBlank()) return p;
                if (p.isBlank()) return r;
                if (r.endsWith("/") || r.endsWith("\\")) return r + p;
                return r + File.separator + p;
            }

            public String safe(String s) {
                return (s == null) ? "" : s;
            }
        }
    }
