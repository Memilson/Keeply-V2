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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

import static java.util.Objects.requireNonNull;

public final class ReportExporter {

    private static final Logger log = LoggerFactory.getLogger(ReportExporter.class);

    private static final String TEMPLATE_PATH = "templates/report-inventory.peb";
    private static final String FONT_REGULAR = "/fonts/NotoSans-Regular.ttf";
    private static final String FONT_BOLD = "/fonts/NotoSans-Bold.ttf";

    private ReportExporter() {}

    public static void exportPdf(List<InventoryRow> rows, File file, ScanSummary scan) throws IOException {
        exportPdf(rows, file, scan, ReportOptions.load());
    }

    public static void exportPdf(List<InventoryRow> rows, File file, ScanSummary scan, ReportOptions opts) throws IOException {
        requireNonNull(file, "file");
        requireNonNull(opts, "opts");
        var safeRows = (rows == null) ? List.<InventoryRow>of() : rows;

        if (file.getParentFile() != null) {
            // cria diretório se não existir (robustez)
            //noinspection ResultOfMethodCallIgnored
            file.getParentFile().mkdirs();
        }

        // AWT headless (evita crash em servidor/VM sem GUI)
        System.setProperty("java.awt.headless", "true");

        var model = ReportModel.from(safeRows, scan, opts);

        // Charts -> base64 PNG (pra template)
        var charts = ReportCharts.build(model, opts);

        // Render HTML via template
        var html = renderHtml(model, charts, opts);

        // HTML -> PDF
        try (var out = new BufferedOutputStream(new FileOutputStream(file))) {
            renderPdf(html, out, opts);
        }

        log.info("Report PDF exported: {}", file.getAbsolutePath());
    }

    // -------------------- HTML rendering (Pebble) --------------------

    private static String renderHtml(ReportModel model, ReportCharts.Charts charts, ReportOptions opts) throws IOException {
        var loader = new ClasspathLoader();
        loader.setCharset(StandardCharsets.UTF_8);
        loader.setPrefix(""); // resources root
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
            builder.withHtmlContent(html, null);

            // Fonts (se tiver no resources)
            registerFontIfPresent(builder, FONT_REGULAR, "Noto Sans", 400);
            registerFontIfPresent(builder, FONT_BOLD, "Noto Sans", 700);

            // Page size
            builder.useDefaultPageSize(
                    opts.pageWidthPoints(),
                    opts.pageHeightPoints(),
                    opts.landscape()
            );

            builder.run();
        } catch (Exception e) {
            throw new IOException("Failed to render PDF (OpenHTMLtoPDF)", e);
        }
    }

    private static void registerFontIfPresent(PdfRendererBuilder builder, String resourcePath, String family, int weight) {
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
            Theme theme
    ) {
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
                    get(cfg, "keeply.report.theme.rowAlt", "#F8FAFC")
            );

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
            String rowAlt
    ) {}

    private static String get(Config cfg, String path, String def) {
        try { return cfg.hasPath(path) ? cfg.getString(path) : def; }
        catch (Exception ignored) { return def; }
    }

    private static int getInt(Config cfg, String path, int def) {
        try { return cfg.hasPath(path) ? cfg.getInt(path) : def; }
        catch (Exception ignored) { return def; }
    }

    private static boolean getBool(Config cfg, String path, boolean def) {
        try { return cfg.hasPath(path) ? cfg.getBoolean(path) : def; }
        catch (Exception ignored) { return def; }
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
            if (s == null) return "";
            // “não reinventar roda” (commons-lang3)
            return StringUtils.abbreviateMiddle(s, "…", Math.max(8, max));
        }

        public String now() {
            return ZonedDateTime.now(opts.zoneId()).format(dtf);
        }
    }
}
