package com.keeply.app.report;

import com.keeply.app.Database.InventoryRow;
import com.keeply.app.Database.ScanSummary;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.pdmodel.PDPageContentStream;
import org.apache.pdfbox.pdmodel.common.PDRectangle;
import org.apache.pdfbox.pdmodel.font.PDType1Font;
import org.apache.pdfbox.pdmodel.graphics.image.LosslessFactory;
import org.apache.pdfbox.pdmodel.graphics.image.PDImageXObject;
import org.knowm.xchart.BitmapEncoder;
import org.knowm.xchart.CategoryChart;
import org.knowm.xchart.CategoryChartBuilder;
import org.knowm.xchart.style.Styler;

import java.awt.Color;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public final class ReportExporter {

    // Keeply palette
    private static final Color COL_HEADER_BG = new Color(24, 24, 27);
    private static final Color COL_ACCENT = new Color(6, 182, 212);
    private static final Color COL_TEXT_MAIN = new Color(30, 30, 30);
    private static final Color COL_TEXT_MUTED = new Color(100, 100, 100);
    private static final Color COL_ROW_ALT = new Color(248, 250, 252);

    private ReportExporter() {}

    public static void exportPdf(List<InventoryRow> rows, File file, ScanSummary scan) throws IOException {
        List<InventoryRow> safeRows = (rows == null) ? List.of() : rows;
        long totalBytes = safeRows.stream().mapToLong(InventoryRow::sizeBytes).sum();

        String rootPath = resolveRootPath(safeRows, scan);
        String scanInfo = formatScanInfo(scan);

        List<InventoryRow> topFiles = safeRows.stream()
                .sorted(Comparator.comparingLong(InventoryRow::sizeBytes).reversed())
                .limit(20)
                .toList();

        List<TypeStat> typeStats = computeTypeStats(safeRows);
        List<TypeStat> topTypes = typeStats.size() > 12 ? typeStats.subList(0, 12) : typeStats;
        List<FolderStat> topFolders = computeTopFolders(safeRows, 12);
        Map<String, Long> statusCounts = computeStatusCounts(safeRows);

        BufferedImage chartTypes = buildTypeChart(topTypes);

        try (PDDocument doc = new PDDocument()) {
            try (PdfCursor cursor = new PdfCursor(doc)) {
                drawKeeplyHeader(cursor, "Relatorio de Inventario", scanInfo);
                cursor.moveDown(30);

                drawSummaryCards(cursor, rootPath, safeRows.size(), totalBytes);
                if (!statusCounts.isEmpty()) {
                    cursor.moveDown(20);
                    drawStatRow(cursor, List.of(
                            new StatItem("NEW", Long.toString(statusCounts.getOrDefault("NEW", 0L))),
                            new StatItem("MODIFIED", Long.toString(statusCounts.getOrDefault("MODIFIED", 0L))),
                            new StatItem("STABLE", Long.toString(statusCounts.getOrDefault("STABLE", 0L))),
                            new StatItem("OTHER", Long.toString(statusCounts.getOrDefault("OTHER", 0L)))
                    ));
                }
                cursor.moveDown(30);

                if (chartTypes != null) {
                    cursor.checkPageBreak(250);
                    drawSectionTitle(cursor, "Tipos de Arquivo (Top 12 por tamanho)");
                    drawImageCentered(cursor, chartTypes, 220);
                    cursor.moveDown(20);
                }

                if (!topTypes.isEmpty()) {
                    cursor.checkPageBreak(140);
                    drawTypeTable(cursor, topTypes, totalBytes);
                    cursor.moveDown(20);
                }

                if (!topFolders.isEmpty()) {
                    cursor.checkPageBreak(140);
                    drawSectionTitle(cursor, "Pastas com maior tamanho (Top 12)");
                    drawFolderTable(cursor, topFolders);
                    cursor.moveDown(20);
                }

                cursor.checkPageBreak(100);
                drawSectionTitle(cursor, "Maiores Arquivos (Top 20)");
                drawSmartFileTable(cursor, topFiles);
            }
            addPageNumbers(doc);
            doc.save(file);
        }
    }

    // --- CHARTS ---

    private static BufferedImage buildTypeChart(List<TypeStat> types) {
        if (types.isEmpty()) return null;

        CategoryChart chart = new CategoryChartBuilder().width(600).height(350).title("").build();
        chart.getStyler().setLegendVisible(false);
        chart.getStyler().setPlotGridLinesVisible(false);
        chart.getStyler().setChartBackgroundColor(Color.WHITE);
        chart.getStyler().setSeriesColors(new Color[]{ COL_ACCENT });
        chart.getStyler().setAvailableSpaceFill(0.6);
        chart.getStyler().setLegendPosition(Styler.LegendPosition.OutsideE);

        List<String> xData = new ArrayList<>();
        List<Double> yData = new ArrayList<>();
        for (TypeStat stat : types) {
            xData.add(stat.ext());
            yData.add(stat.bytes() / 1024.0 / 1024.0);
        }
        chart.addSeries("MB", xData, yData);
        return BitmapEncoder.getBufferedImage(chart);
    }

    // --- PDF LAYOUT ---

    private static void drawKeeplyHeader(PdfCursor cursor, String title, String subtitle) throws IOException {
        PDPageContentStream cs = cursor.stream;
        cs.setNonStrokingColor(COL_HEADER_BG);
        cs.addRect(0, cursor.pageHeight - 90, cursor.pageWidth, 90);
        cs.fill();
        cs.setNonStrokingColor(COL_ACCENT);
        cs.addRect(0, cursor.pageHeight - 92, cursor.pageWidth, 2);
        cs.fill();
        cs.beginText();
        cs.setFont(PDType1Font.HELVETICA_BOLD, 14);
        cs.setNonStrokingColor(COL_ACCENT);
        cs.newLineAtOffset(40, cursor.pageHeight - 35);
        cs.showText("KEEPLY");
        cs.endText();
        cs.beginText();
        cs.setFont(PDType1Font.HELVETICA_BOLD, 24);
        cs.setNonStrokingColor(Color.WHITE);
        cs.newLineAtOffset(40, cursor.pageHeight - 65);
        cs.showText(title);
        cs.endText();
        float subW = PDType1Font.HELVETICA.getStringWidth(subtitle) / 1000 * 10;
        cs.beginText();
        cs.setFont(PDType1Font.HELVETICA, 10);
        cs.setNonStrokingColor(new Color(180, 180, 180));
        cs.newLineAtOffset(cursor.pageWidth - 40 - subW, cursor.pageHeight - 35);
        cs.showText(subtitle);
        cs.endText();
        cursor.y = cursor.pageHeight - 120;
    }

    private static void drawSummaryCards(PdfCursor cursor, String root, int count, long bytes) throws IOException {
        drawStatRow(cursor, List.of(
                new StatItem("Total Files", String.format("%,d", count)),
                new StatItem("Total Size", humanSize(bytes)),
                new StatItem("Scan Root", shortenPath(root, 25))
        ));
    }

    private static void drawStatItem(PdfCursor cursor, float x, float y, String label, String value) throws IOException {
        PDPageContentStream cs = cursor.stream;
        cs.beginText();
        cs.setFont(PDType1Font.HELVETICA, 9);
        cs.setNonStrokingColor(COL_TEXT_MUTED);
        cs.newLineAtOffset(x, y + 10);
        cs.showText(label.toUpperCase(Locale.ROOT));
        cs.endText();
        cs.beginText();
        cs.setFont(PDType1Font.HELVETICA_BOLD, 14);
        cs.setNonStrokingColor(COL_TEXT_MAIN);
        cs.newLineAtOffset(x, y - 5);
        cs.showText(pdfSafe(value));
        cs.endText();
    }

    private static void drawStatRow(PdfCursor cursor, List<StatItem> items) throws IOException {
        if (items == null || items.isEmpty()) return;
        float colW = (cursor.pageWidth - 80) / items.size();
        float y = cursor.y;
        for (int i = 0; i < items.size(); i++) {
            StatItem item = items.get(i);
            drawStatItem(cursor, 40 + colW * i, y, item.label(), item.value());
        }
        cursor.y -= 30;
    }

    private static void drawSectionTitle(PdfCursor cursor, String title) throws IOException {
        PDPageContentStream cs = cursor.stream;
        cs.setNonStrokingColor(COL_HEADER_BG);
        cs.beginText();
        cs.setFont(PDType1Font.HELVETICA_BOLD, 12);
        cs.newLineAtOffset(40, cursor.y);
        cs.showText(title.toUpperCase(Locale.ROOT));
        cs.endText();
        cs.setStrokingColor(new Color(230, 230, 230));
        cs.setLineWidth(1);
        cs.moveTo(40, cursor.y - 8);
        cs.lineTo(cursor.pageWidth - 40, cursor.y - 8);
        cs.stroke();
        cursor.moveDown(25);
    }

    private static void drawSmartFileTable(PdfCursor cursor, List<InventoryRow> rows) throws IOException {
        float rowH = 35;
        drawSmartHeader(cursor);
        cursor.moveDown(20);
        int i = 1;
        for (InventoryRow row : rows) {
            cursor.checkPageBreak(rowH);
            if (i % 2 == 0) {
                cursor.stream.setNonStrokingColor(COL_ROW_ALT);
                cursor.stream.addRect(40, cursor.y - rowH + 8, cursor.pageWidth - 80, rowH);
                cursor.stream.fill();
            }
            String fullPath = row.pathRel();
            String name = row.name();
            String pathOnly = "/";
            if (fullPath != null && name != null && fullPath.endsWith(name)) {
                if (fullPath.length() > name.length()) {
                    pathOnly = fullPath.substring(0, fullPath.length() - name.length());
                    if (pathOnly.endsWith("/") || pathOnly.endsWith("\\")) {
                        pathOnly = pathOnly.substring(0, pathOnly.length() - 1);
                    }
                }
            }
            drawDoubleLineRow(cursor, String.valueOf(i++), name, pathOnly, humanSize(row.sizeBytes()));
            cursor.moveDown(rowH);
        }
    }

    private static void drawTypeTable(PdfCursor cursor, List<TypeStat> types, long totalBytes) throws IOException {
        float rowH = 16;
        drawTypeHeader(cursor);
        cursor.moveDown(15);
        int i = 0;
        for (TypeStat stat : types) {
            cursor.checkPageBreak(rowH);
            if (i % 2 == 1) {
                cursor.stream.setNonStrokingColor(COL_ROW_ALT);
                cursor.stream.addRect(40, cursor.y - rowH + 6, cursor.pageWidth - 80, rowH);
                cursor.stream.fill();
            }
            drawTypeRow(cursor, stat, totalBytes);
            cursor.moveDown(rowH);
            i++;
        }
    }

    private static void drawTypeHeader(PdfCursor cursor) throws IOException {
        PDPageContentStream cs = cursor.stream;
        cs.setNonStrokingColor(COL_TEXT_MUTED);
        cs.setFont(PDType1Font.HELVETICA_BOLD, 8);
        cs.beginText(); cs.newLineAtOffset(45, cursor.y); cs.showText("EXT"); cs.endText();
        cs.beginText(); cs.newLineAtOffset(160, cursor.y); cs.showText("COUNT"); cs.endText();
        cs.beginText(); cs.newLineAtOffset(255, cursor.y); cs.showText("SIZE"); cs.endText();
        cs.beginText(); cs.newLineAtOffset(340, cursor.y); cs.showText("PCT"); cs.endText();
    }

    private static void drawTypeRow(PdfCursor cursor, TypeStat stat, long totalBytes) throws IOException {
        PDPageContentStream cs = cursor.stream;
        String pct = totalBytes > 0 ? String.format(Locale.US, "%.1f%%", (stat.bytes() * 100.0) / totalBytes) : "0.0%";
        cs.setNonStrokingColor(COL_TEXT_MAIN);
        cs.setFont(PDType1Font.HELVETICA, 9);
        cs.beginText(); cs.newLineAtOffset(45, cursor.y - 10); cs.showText(stat.ext()); cs.endText();
        cs.beginText(); cs.newLineAtOffset(160, cursor.y - 10); cs.showText(Long.toString(stat.count())); cs.endText();
        cs.beginText(); cs.newLineAtOffset(255, cursor.y - 10); cs.showText(humanSize(stat.bytes())); cs.endText();
        cs.beginText(); cs.newLineAtOffset(340, cursor.y - 10); cs.showText(pct); cs.endText();
    }

    private static void drawFolderTable(PdfCursor cursor, List<FolderStat> folders) throws IOException {
        float rowH = 16;
        drawFolderHeader(cursor);
        cursor.moveDown(15);
        int i = 0;
        for (FolderStat stat : folders) {
            cursor.checkPageBreak(rowH);
            if (i % 2 == 1) {
                cursor.stream.setNonStrokingColor(COL_ROW_ALT);
                cursor.stream.addRect(40, cursor.y - rowH + 6, cursor.pageWidth - 80, rowH);
                cursor.stream.fill();
            }
            drawFolderRow(cursor, stat);
            cursor.moveDown(rowH);
            i++;
        }
    }

    private static void drawFolderHeader(PdfCursor cursor) throws IOException {
        PDPageContentStream cs = cursor.stream;
        cs.setNonStrokingColor(COL_TEXT_MUTED);
        cs.setFont(PDType1Font.HELVETICA_BOLD, 8);
        cs.beginText(); cs.newLineAtOffset(45, cursor.y); cs.showText("FOLDER"); cs.endText();
        cs.beginText(); cs.newLineAtOffset(330, cursor.y); cs.showText("FILES"); cs.endText();
        String sizeLbl = "SIZE";
        float w = PDType1Font.HELVETICA_BOLD.getStringWidth(sizeLbl) / 1000 * 8;
        cs.beginText(); cs.newLineAtOffset(cursor.pageWidth - 45 - w, cursor.y); cs.showText(sizeLbl); cs.endText();
    }

    private static void drawFolderRow(PdfCursor cursor, FolderStat stat) throws IOException {
        PDPageContentStream cs = cursor.stream;
        cs.setNonStrokingColor(COL_TEXT_MAIN);
        cs.setFont(PDType1Font.HELVETICA, 9);
        cs.beginText(); cs.newLineAtOffset(45, cursor.y - 10); cs.showText(pdfSafe(shortenPath(stat.folder(), 60))); cs.endText();
        cs.beginText(); cs.newLineAtOffset(330, cursor.y - 10); cs.showText(Long.toString(stat.count())); cs.endText();
        String size = humanSize(stat.bytes());
        float w = PDType1Font.HELVETICA.getStringWidth(size) / 1000 * 9;
        cs.beginText(); cs.newLineAtOffset(cursor.pageWidth - 45 - w, cursor.y - 10); cs.showText(size); cs.endText();
    }

    private static void drawSmartHeader(PdfCursor cursor) throws IOException {
        PDPageContentStream cs = cursor.stream;
        cs.setNonStrokingColor(COL_TEXT_MUTED);
        cs.setFont(PDType1Font.HELVETICA_BOLD, 8);
        cs.beginText(); cs.newLineAtOffset(45, cursor.y); cs.showText("#"); cs.endText();
        cs.beginText(); cs.newLineAtOffset(70, cursor.y); cs.showText("FILE / LOCATION"); cs.endText();
        String sizeLbl = "SIZE";
        float w = PDType1Font.HELVETICA_BOLD.getStringWidth(sizeLbl) / 1000 * 8;
        cs.beginText(); cs.newLineAtOffset(cursor.pageWidth - 45 - w, cursor.y); cs.showText(sizeLbl); cs.endText();
    }

    private static void drawDoubleLineRow(PdfCursor cursor, String index, String name, String path, String size) throws IOException {
        PDPageContentStream cs = cursor.stream;
        cs.setNonStrokingColor(COL_TEXT_MUTED);
        cs.setFont(PDType1Font.HELVETICA, 8);
        cs.beginText(); cs.newLineAtOffset(45, cursor.y - 12); cs.showText(index); cs.endText();
        cs.setNonStrokingColor(COL_TEXT_MAIN);
        cs.setFont(PDType1Font.HELVETICA_BOLD, 10);
        cs.beginText(); cs.newLineAtOffset(70, cursor.y - 5); cs.showText(pdfSafe(shortenPath(name, 55))); cs.endText();
        cs.setNonStrokingColor(new Color(120, 120, 120));
        cs.setFont(PDType1Font.HELVETICA, 8);
        cs.beginText(); cs.newLineAtOffset(70, cursor.y - 16); cs.showText(pdfSafe(shortenPath(path, 75))); cs.endText();
        cs.setNonStrokingColor(COL_TEXT_MAIN);
        cs.setFont(PDType1Font.HELVETICA, 9);
        float w = PDType1Font.HELVETICA.getStringWidth(size) / 1000 * 9;
        cs.beginText(); cs.newLineAtOffset(cursor.pageWidth - 45 - w, cursor.y - 12); cs.showText(size); cs.endText();
    }

    private static void drawImageCentered(PdfCursor cursor, BufferedImage img, float height) throws IOException {
        PDImageXObject pdImage = LosslessFactory.createFromImage(cursor.doc, img);
        float scale = height / img.getHeight();
        float width = img.getWidth() * scale;
        float x = (cursor.pageWidth - width) / 2;
        cursor.stream.drawImage(pdImage, x, cursor.y - height, width, height);
        cursor.y -= height;
    }

    private static void addPageNumbers(PDDocument doc) throws IOException {
        int total = doc.getNumberOfPages();
        for (int i = 0; i < total; i++) {
            PDPage page = doc.getPage(i);
            try (PDPageContentStream content = new PDPageContentStream(doc, page, PDPageContentStream.AppendMode.APPEND, true, true)) {
                content.beginText();
                content.setFont(PDType1Font.HELVETICA, 9);
                content.setNonStrokingColor(COL_TEXT_MUTED);
                String text = (i + 1) + " / " + total;
                float w = PDType1Font.HELVETICA.getStringWidth(text) / 1000 * 9;
                content.newLineAtOffset((page.getMediaBox().getWidth() - w) / 2, 20);
                content.showText(text);
                content.endText();
            }
        }
    }

    // --- CURSOR ---

    private static class PdfCursor implements AutoCloseable {
        final PDDocument doc;
        PDPage page;
        PDPageContentStream stream;
        float pageWidth;
        float pageHeight;
        float y;

        PdfCursor(PDDocument doc) throws IOException {
            this.doc = doc;
            newPage();
        }

        void newPage() throws IOException {
            if (stream != null) stream.close();
            this.page = new PDPage(PDRectangle.LETTER);
            this.doc.addPage(page);
            this.stream = new PDPageContentStream(doc, page);
            this.pageWidth = page.getMediaBox().getWidth();
            this.pageHeight = page.getMediaBox().getHeight();
            this.y = pageHeight;
        }

        void moveDown(float amount) {
            y -= amount;
        }

        void checkPageBreak(float needed) throws IOException {
            if (y - needed < 50) {
                newPage();
                y -= 50;
            }
        }

        @Override
        public void close() throws IOException {
            if (stream != null) stream.close();
        }
    }

    // --- UTILS ---

    private static List<TypeStat> computeTypeStats(List<InventoryRow> rows) {
        Map<String, long[]> stats = new HashMap<>();
        for (InventoryRow r : rows) {
            String ext = extensionOf(r);
            long[] agg = stats.computeIfAbsent(ext, k -> new long[2]);
            agg[0] += r.sizeBytes();
            agg[1] += 1;
        }
        List<TypeStat> list = new ArrayList<>();
        for (Map.Entry<String, long[]> entry : stats.entrySet()) {
            long[] agg = entry.getValue();
            list.add(new TypeStat(entry.getKey(), agg[0], agg[1]));
        }
        list.sort(Comparator.comparingLong(TypeStat::bytes).reversed());
        return list;
    }

    private static List<FolderStat> computeTopFolders(List<InventoryRow> rows, int limit) {
        Map<String, long[]> stats = new HashMap<>();
        for (InventoryRow row : rows) {
            String folder = extractFolder(row.pathRel());
            long[] agg = stats.computeIfAbsent(folder, k -> new long[2]);
            agg[0] += row.sizeBytes();
            agg[1] += 1;
        }
        List<FolderStat> list = new ArrayList<>();
        for (Map.Entry<String, long[]> entry : stats.entrySet()) {
            long[] agg = entry.getValue();
            list.add(new FolderStat(entry.getKey(), agg[0], agg[1]));
        }
        list.sort(Comparator.comparingLong(FolderStat::bytes).reversed());
        if (list.size() > limit) return list.subList(0, limit);
        return list;
    }

    private static Map<String, Long> computeStatusCounts(List<InventoryRow> rows) {
        Map<String, Long> counts = new HashMap<>();
        for (InventoryRow row : rows) {
            String status = normalizeStatus(row.status());
            counts.merge(status, 1L, Long::sum);
        }
        return counts;
    }

    private static String resolveRootPath(List<InventoryRow> rows, ScanSummary scan) {
        if (scan != null && scan.rootPath() != null) return scan.rootPath();
        return rows.isEmpty() ? "-" : rows.get(0).rootPath();
    }

    private static String humanSize(long bytes) {
        if (bytes < 1024) return bytes + " B";
        int exp = (int) (Math.log(bytes) / Math.log(1024));
        String pre = "KMGTPE".charAt(exp - 1) + "";
        return String.format(Locale.US, "%.1f %sB", bytes / Math.pow(1024, exp), pre);
    }

    private static String shortenPath(String path, int max) {
        if (path == null) return "";
        if (path.length() <= max) return path;
        return "..." + path.substring(path.length() - (max - 3));
    }

    private static String extensionOf(InventoryRow row) {
        String name = row.name();
        if (name == null || name.isBlank()) name = row.pathRel();
        if (name == null || name.isBlank()) return "NO_EXT";
        int idx = name.lastIndexOf('.');
        if (idx <= 0 || idx >= name.length() - 1) return "NO_EXT";
        return "." + name.substring(idx + 1).toUpperCase(Locale.ROOT);
    }

    private static String extractFolder(String pathRel) {
        if (pathRel == null || pathRel.isBlank()) return "<root>";
        String normalized = pathRel.replace('\\', '/');
        int idx = normalized.lastIndexOf('/');
        if (idx <= 0) return "<root>";
        String folder = normalized.substring(0, idx);
        if (folder.isBlank()) return "<root>";
        return folder;
    }

    private static String normalizeStatus(String status) {
        if (status == null) return "OTHER";
        String norm = status.trim().toUpperCase(Locale.ROOT);
        if (norm.isEmpty()) return "OTHER";
        if ("NEW".equals(norm) || "MODIFIED".equals(norm) || "STABLE".equals(norm)) return norm;
        return "OTHER";
    }

    private static String formatScanInfo(ScanSummary scan) {
        if (scan == null) return "Export Manual";
        String finished = scan.finishedAt() == null ? "-" : scan.finishedAt();
        return "Snapshot #" + scan.scanId() + " | " + finished;
    }

    private static String pdfSafe(String value) {
        if (value == null) return "";
        StringBuilder sb = new StringBuilder();
        for (char c : value.toCharArray()) {
            if (c > 127) sb.append('?'); else sb.append(c);
        }
        return sb.toString();
    }

    private record StatItem(String label, String value) {}
    private record TypeStat(String ext, long bytes, long count) {}
    private record FolderStat(String folder, long bytes, long count) {}
}
