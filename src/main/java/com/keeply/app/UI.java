package com.keeply.app;

import javafx.application.Platform;
import javafx.beans.property.SimpleStringProperty;
import javafx.beans.property.StringProperty;

public class UI {

    public static class ScanModel {
        public StringProperty filesScannedProperty = new SimpleStringProperty("0");
        public StringProperty mbPerSecProperty = new SimpleStringProperty("0.0");
        public StringProperty rateProperty = new SimpleStringProperty("0");
        public StringProperty dbBatchesProperty = new SimpleStringProperty("0");
        public StringProperty errorsProperty = new SimpleStringProperty("0");

        public void reset() {
            Platform.runLater(() -> {
                filesScannedProperty.set("0");
                mbPerSecProperty.set("0.0");
                rateProperty.set("0");
                dbBatchesProperty.set("0");
                errorsProperty.set("0");
            });
        }
    }
}