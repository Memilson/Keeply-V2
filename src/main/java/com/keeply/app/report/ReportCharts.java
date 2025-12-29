package com.keeply.app.report;

import org.knowm.xchart.*;
import org.knowm.xchart.style.PieStyler;
import org.knowm.xchart.style.Styler;
import javax.imageio.ImageIO;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import java.awt.image.BufferedImage;
import java.util.Base64;
import java.util.List;
import java.util.Map;

public final class ReportCharts {

    private ReportCharts() {}

    public record Charts(String typesBarPngBase64, String statusPiePngBase64) {}

    public static Charts build(ReportModel model, ReportExporter.ReportOptions opts) {
        String types = null;
        String status = null;

        try { types = toBase64Png(buildTypesBar(model.topTypes())); } catch (Exception ignored) {}
        try { status = toBase64Png(buildStatusPie(model.statusCounts())); } catch (Exception ignored) {}

        return new Charts(types, status);
    }

    private static BufferedImage buildTypesBar(List<ReportModel.TypeStat> types) {
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
        // AnnotationType API varies across xchart versions; skip explicit setting
        // to keep compatibility and rely on sensible defaults.
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

    private static String toBase64Png(BufferedImage img) throws java.io.IOException {
        try (var baos = new ByteArrayOutputStream()) {
            ImageIO.write(img, "png", baos);
            return Base64.getEncoder().encodeToString(baos.toByteArray());
        }
    }
}
