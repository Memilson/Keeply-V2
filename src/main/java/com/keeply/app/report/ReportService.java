package com.keeply.app.report;

import com.keeply.app.Database.InventoryRow;
import com.keeply.app.Database.ScanSummary;
import com.openhtmltopdf.pdfboxout.PdfRendererBuilder;
import com.openhtmltopdf.svgsupport.BatikSVGDrawer;
import io.pebbletemplates.pebble.PebbleEngine;
import io.pebbletemplates.pebble.loader.ClasspathLoader;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Entities;

// Imports do XChart
import org.knowm.xchart.CategoryChartBuilder;
import org.knowm.xchart.PieChartBuilder;
import org.knowm.xchart.VectorGraphicsEncoder;
import org.knowm.xchart.VectorGraphicsEncoder.VectorGraphicsFormat;
import org.knowm.xchart.internal.chartpart.Chart; // Import genérico
import org.knowm.xchart.style.PieStyler;
import org.knowm.xchart.style.Styler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.Color;
import java.io.*;
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

    private static final PebbleEngine PEBBLE_ENGINE = new PebbleEngine.Builder()
            .loader(new ClasspathLoader() {{ setPrefix("templates"); }})
            .autoEscaping(false)
            .defaultLocale(Locale.ROOT)
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
        var charts = buildChartsVector(model, opts);
        var html = renderHtml(model, charts, opts);

        writeAtomically(target, tmp -> renderPdf(html, tmp));
    }

    public CompletableFuture<File> exportPdfAsync(List<InventoryRow> rows, File file, ScanSummary scan) {
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            return CompletableFuture.supplyAsync(() -> {
                try {
                    exportPdf(rows, file, scan);
                    return file;
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }, executor);
        }
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
        builder.useSVGDrawer(new BatikSVGDrawer());
        builder.withHtmlContent(html, ReportService.class.getResource("/templates/").toExternalForm());
        builder.toStream(out);
        loadFont(builder, "/fonts/NotoSans-Regular.ttf", "Noto Sans", 400);
        loadFont(builder, "/fonts/NotoSans-Bold.ttf", "Noto Sans", 700);
        builder.run();
    }

    private void loadFont(PdfRendererBuilder builder, String path, String family, int weight) {
        if (getClass().getResource(path) != null) {
            builder.useFont(() -> getClass().getResourceAsStream(path), family, weight, PdfRendererBuilder.FontStyle.NORMAL, true);
        }
    }

    // -------------------- Gráficos (Correção de Tipos) --------------------

    @SuppressWarnings({"rawtypes", "unchecked"}) // Silencia avisos de cast bruto
    private Model.Charts buildChartsVector(Model model, ReportOptions opts) {
        String typesSvg = "";
        String statusSvg = "";

        try {
            // Chart 1: Bar
            var barChart = new CategoryChartBuilder()
                    .width(800).height(350)
                    .theme(Styler.ChartTheme.GGPlot2).build();

            var bs = barChart.getStyler();
            bs.setLegendVisible(false);
            bs.setPlotGridLinesVisible(false);
            bs.setChartTitleVisible(false);
            bs.setChartBackgroundColor(Color.WHITE);
            bs.setPlotBackgroundColor(Color.WHITE);
            bs.setSeriesColors(new Color[]{Color.decode(opts.theme().accent())});
            
            var xData = model.topTypes().stream().map(Model.TypeStat::ext).toList();
            var yData = model.topTypes().stream().map(t -> t.bytes() / 1024d / 1024d).toList();

            if (!xData.isEmpty()) {
                barChart.addSeries("Types", xData, yData);
                // CORREÇÃO: Usando ByteArrayOutputStream para capturar o SVG
                try (var baos = new ByteArrayOutputStream()) {
                    VectorGraphicsEncoder.saveVectorGraphic(barChart, baos, VectorGraphicsFormat.SVG);
                    typesSvg = cleanSvg(baos.toString("UTF-8"));
                }
            }

            // Chart 2: Pie
            var pieChart = new PieChartBuilder().width(500).height(350).build();
            var ps = pieChart.getStyler();
            ps.setChartTitleVisible(false);
            ps.setLegendPosition(PieStyler.LegendPosition.OutsideE);
            ps.setChartBackgroundColor(Color.WHITE);
            ps.setPlotBorderVisible(false);
            ps.setSeriesColors(new Color[]{
                    new Color(34, 197, 94), new Color(251, 191, 36),
                    new Color(59, 130, 246), new Color(148, 163, 184)
            });

            model.statusCounts().forEach((k, v) -> { if (v > 0) pieChart.addSeries(k, v); });
            if (pieChart.getSeriesMap().isEmpty()) pieChart.addSeries("Sem dados", 1);

            // CORREÇÃO: Usando ByteArrayOutputStream para capturar o SVG
            try (var baos = new ByteArrayOutputStream()) {
                VectorGraphicsEncoder.saveVectorGraphic(pieChart, baos, VectorGraphicsFormat.SVG);
                statusSvg = cleanSvg(baos.toString("UTF-8"));
            }

        } catch (Exception e) {
            log.error("Erro ao gerar gráficos SVG", e);
        }

        return new Model.Charts(typesSvg, statusSvg);
    }

    private String cleanSvg(String svg) {
        if (svg == null) return "";
        return svg.replaceFirst("<\\?xml.*?>", "").trim();
    }

    // -------------------- Processamento --------------------

    @SuppressWarnings("unchecked") // Silencia avisos sobre Casts nos Mapas e Listas
    private Model buildModel(List<InventoryRow> rows, ScanSummary scan, ReportOptions opts) {
        Collector<InventoryRow, ?, Long> totalBytesCol = summingLong(InventoryRow::sizeBytes);
        Collector<InventoryRow, ?, Map<String, Long>> statusCol = groupingBy(r -> normalizeStatus(r.status()), counting());
        
        Collector<InventoryRow, ?, Map<String, long[]>> typesCol = groupingBy(
            ReportService::extensionOf,
            collectingAndThen(summarizingLong(InventoryRow::sizeBytes), s -> new long[]{s.getSum(), s.getCount()})
        );
        
        Collector<InventoryRow, ?, Map<String, long[]>> foldersCol = groupingBy(
            r -> extractFolder(r.pathRel()),
            collectingAndThen(summarizingLong(InventoryRow::sizeBytes), s -> new long[]{s.getSum(), s.getCount()})
        );

        int limitFiles = opts.topFiles();
        Collector<InventoryRow, ?, List<Model.TopFile>> topFilesCol = Collector.of(
            () -> new PriorityQueue<Model.TopFile>(Comparator.comparingLong(Model.TopFile::sizeBytes)),
            (pq, r) -> {
                pq.offer(new Model.TopFile(fallbackName(r), r.pathRel(), r.rootPath(), r.sizeBytes()));
                if (pq.size() > limitFiles) pq.poll();
            },
            (pq1, pq2) -> { pq1.addAll(pq2); while (pq1.size() > limitFiles) pq1.poll(); return pq1; },
            pq -> {
                var l = new ArrayList<>(pq);
                l.sort(Comparator.comparingLong(Model.TopFile::sizeBytes).reversed());
                return l;
            }
        );

        var results = rows.stream().collect(teeing(totalBytesCol, teeing(statusCol, teeing(typesCol, teeing(foldersCol, topFilesCol, List::of), List::of), List::of), List::of));

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
                .sorted(Comparator.comparingLong(Model.TypeStat::bytes).reversed()).limit(opts.topTypes()).toList();

        var topFolders = foldersMap.entrySet().stream()
                .map(e -> new Model.FolderStat(e.getKey(), e.getValue()[0], e.getValue()[1]))
                .sorted(Comparator.comparingLong(Model.FolderStat::bytes).reversed()).limit(opts.topFolders()).toList();

        List.of("NEW", "MODIFIED", "STABLE", "OTHER").forEach(k -> statusMap.putIfAbsent(k, 0L));
        var root = (scan != null) ? scan.rootPath() : "-";
        
        return new Model(new Model.Summary(root, rows.size(), totalBytes, (scan != null) ? scan.scanId() : null, (scan != null) ? scan.finishedAt() : null), 
                         statusMap, topTypes, topFolders, topFiles);
    }

    private void writeAtomically(Path target, IOSubroutine action) throws IOException {
        Files.createDirectories(target.getParent());
        Path tmp = Files.createTempFile(target.getParent(), target.getFileName().toString(), ".tmp");
        try {
            try (var out = new BufferedOutputStream(Files.newOutputStream(tmp))) { action.execute(out); }
            Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (Exception e) {
            Files.deleteIfExists(tmp);
            throw (e instanceof IOException io) ? io : new IOException(e);
        }
    }

    @FunctionalInterface interface IOSubroutine { void execute(OutputStream out) throws Exception; }
    
    // Helpers
    private static String normalizeStatus(String s) { return (s == null) ? "OTHER" : switch (s.toUpperCase().trim()) { case "NEW", "MODIFIED", "STABLE" -> s.toUpperCase().trim(); default -> "OTHER"; }; }
    private static String extensionOf(InventoryRow r) { String n = StringUtils.firstNonBlank(r.name(), r.pathRel(), ""); int i = n.lastIndexOf('.'); return (i > 0 && i < n.length() - 1) ? ("." + n.substring(i + 1)).toUpperCase() : "NO_EXT"; }
    private static String extractFolder(String path) { if (path == null) return "<root>"; String p = path.replace('\\', '/'); int i = p.lastIndexOf('/'); return (i <= 0) ? "<root>" : p.substring(0, i); }
    private static String fallbackName(InventoryRow r) { return StringUtils.isNotBlank(r.name()) ? r.name() : "unknown"; }

    // Records
    public record Model(Summary summary, Map<String, Long> statusCounts, List<TypeStat> topTypes, List<FolderStat> topFolders, List<TopFile> topFiles) {
        public record Summary(String rootPath, int totalFiles, long totalBytes, Object scanId, String finishedAt) {}
        public record TypeStat(String ext, long bytes, long count) {}
        public record FolderStat(String folder, long bytes, long count) {}
        public record TopFile(String name, String pathRel, String rootPath, long sizeBytes) {}
        public record Charts(String typesSvg, String statusSvg) {}
    }
    public record ReportOptions(Locale locale, ZoneId zoneId, int topFiles, int topTypes, int topFolders, Theme theme) {
        public static ReportOptions load() { return new ReportOptions(Locale.forLanguageTag("pt-BR"), ZoneId.systemDefault(), 20, 12, 12, new Theme("#09090b", "#06B6D4", "#1E1E1E", "#71717a", "#F4F4F5")); }
    }
    public record Theme(String headerBg, String accent, String textMain, String textMuted, String rowAlt) {}
    public static class TemplateFormatters {
        private final ReportOptions opts; private final NumberFormat nf; private final DateTimeFormatter dtf;
        public TemplateFormatters(ReportOptions opts) { this.opts = opts; this.nf = NumberFormat.getIntegerInstance(opts.locale()); this.dtf = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm", opts.locale()); }
        public String bytes(Number b) { return FileUtils.byteCountToDisplaySize(b != null ? b.longValue() : 0); }
        public String format(Number n) { return nf.format(n != null ? n : 0); }
        public String now() { return ZonedDateTime.now(opts.zoneId()).format(dtf); }
    }
}