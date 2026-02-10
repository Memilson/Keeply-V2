package com.keeply.app.inventory;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Optional;

import com.keeply.app.blob.BlobStore;
import com.keeply.app.config.Config;
import com.keeply.app.database.DatabaseBackup;
import com.keeply.app.templates.KeeplyTemplate.ScanModel;

import javafx.application.Platform;
import javafx.beans.property.BooleanProperty;
import javafx.beans.property.SimpleBooleanProperty;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.Node;
import javafx.scene.control.Alert;
import javafx.scene.control.Button;
import javafx.scene.control.ButtonType;
import javafx.scene.control.CheckBox;
import javafx.scene.control.ChoiceBox;
import javafx.scene.control.Label;
import javafx.scene.control.PasswordField;
import javafx.scene.control.ProgressBar;
import javafx.scene.control.ProgressIndicator;
import javafx.scene.control.ScrollPane;
import javafx.scene.control.Separator;
import javafx.scene.control.TextArea;
import javafx.scene.control.TextField;
import javafx.scene.control.TitledPane;
import javafx.scene.control.ToggleButton;
import javafx.scene.control.ToggleGroup;
import javafx.scene.control.Tooltip;
import javafx.scene.input.Clipboard;
import javafx.scene.input.ClipboardContent;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.Region;
import javafx.scene.layout.StackPane;
import javafx.scene.layout.VBox;
import javafx.scene.shape.SVGPath;
import javafx.stage.DirectoryChooser;
import javafx.stage.Stage;

public final class BackupScreen {
    private static final String ICON_FOLDER="M20 6h-8l-2-2H4c-1.1 0-1.99.9-1.99 2L2 18c0 1.1.9 2 2 2h16c1.1 0 2-.9 2-2V8c0-1.1-.9-2-2-2zm0 12H4V8h16v10z";
    private static final String ICON_PLAY="M8 5v14l11-7z";
    private static final String ICON_STOP="M6 6h12v12H6z";
    private static final String ICON_TRASH="M6 19c0 1.1.9 2 2 2h8c1.1 0 2-.9 2-2V7H6v12zM19 4h-3.5l-1-1h-5l-1 1H5v2h14V4z";
    private static final String ICON_CLOUD="M6 19a4 4 0 0 1 0-8 5 5 0 0 1 9.6-1.6A4 4 0 0 1 18 19H6z";
    private static final String ICON_COPY="M16 1H4c-1.1 0-2 .9-2 2v12h2V3h12V1zm3 4H8c-1.1 0-2 .9-2 2v14c0 1.1.9 2 2 2h11c1.1 0 2-.9 2-2V7c0-1.1-.9-2-2-2zm0 16H8V7h11v14z";
    private static final String ICON_CHECK="M9 16.2l-3.5-3.5L4 14.2l5 5 12-12-1.5-1.5z";
    private static final String ICON_WARN="M1 21h22L12 2 1 21zm12-3h-2v-2h2v2zm0-4h-2v-4h2v4z";
    private final Stage stage;
    private final ScanModel model;
    private final TextField pathField=new TextField();
    private final TextField destField=new TextField();
    private final TextArea consoleArea=new TextArea();
    private final ToggleGroup destinationTypeGroup=new ToggleGroup();
    private final ToggleButton btnLocal=new ToggleButton("Disco local");
    private final ToggleButton btnCloud=new ToggleButton("Nuvem");
    private final Button btnScan=new Button("Iniciar backup");
    private final Button btnStop=new Button("Parar");
    private final Button btnWipe=new Button("Apagar backups");
    private final Button btnBrowse=new Button("Alterar origem");
    private final Button btnBrowseDest=new Button("Alterar destino");
    private final Button btnDbOptions=new Button("Opções DB");
    private final Button btnCopyOrigin=iconOnlyButton(ICON_COPY,"Copiar caminho");
    private final Button btnCopyDest=iconOnlyButton(ICON_COPY,"Copiar caminho");
    private final HBox backupFooterActions=new HBox(10);
    private final BooleanProperty scanning=new SimpleBooleanProperty(false);
    private final BooleanProperty planValid=new SimpleBooleanProperty(false);
    private final ProgressIndicator progressRing=new ProgressIndicator();
    private final ProgressBar progressBar=new ProgressBar(0);
    private final Label progressLabel=new Label("Idle");
    private final Label summaryText=new Label();
    private final Label summaryBadge=new Label();
    private final CheckBox encryptionCheckbox=new CheckBox();
    private final PasswordField backupPasswordField=new PasswordField();
    private boolean suppressEncryptionToggle=false;
    private final BooleanProperty passwordRequired=new SimpleBooleanProperty(false);
    private final CheckBox scheduleCheckbox=new CheckBox();
    private final ChoiceBox<String> scheduleMode=new ChoiceBox<>();
    private final TextField scheduleTimeField=new TextField();
    private final TextField scheduleIntervalField=new TextField();
    private final Label scheduleSummary=new Label();
    private final BooleanProperty scheduleValid=new SimpleBooleanProperty(true);
    private final Label retentionSummary=new Label();
    private final BooleanProperty optionsExpanded=new SimpleBooleanProperty(true);
    private final Label optionsChevron=new Label("▾");
    private java.util.function.Consumer<ScheduleState> scheduleSaveHandler=s->{};
    private java.util.function.IntConsumer retentionSaveHandler=v->{};
    private boolean suppressScheduleSave=false;
    public enum ScheduleMode{DAILY,INTERVAL}
    public record ScheduleState(boolean enabled,ScheduleMode mode,String time,int intervalMinutes){}
    private final BooleanProperty hasExclusionsConfigured=new SimpleBooleanProperty(false);
    public BackupScreen(Stage stage,ScanModel model){
        this.stage=Objects.requireNonNull(stage,"stage");
        this.model=Objects.requireNonNull(model,"model");
        configureControls();}
    private void configureControls(){
        pathField.setText(Objects.requireNonNullElse(Config.getLastPath(),System.getProperty("user.home")));
        pathField.setPromptText("Selecione a pasta de origem…");
        pathField.setEditable(false);
        destField.setText(Objects.requireNonNullElse(Config.getLastBackupDestination(),defaultLocalBackupDestination().toString()));
        destField.setPromptText("Selecione a pasta de destino…");
        destField.setEditable(false);
        try{Files.createDirectories(Path.of(destField.getText()));}catch(java.io.IOException|RuntimeException ignored){}
        btnLocal.setToggleGroup(destinationTypeGroup);
        btnCloud.setToggleGroup(destinationTypeGroup);
        btnLocal.setSelected(true);
        btnLocal.getStyleClass().addAll("segmented","segmented-left");
        btnCloud.getStyleClass().addAll("segmented","segmented-right");
        btnStop.setDisable(true);
        btnBrowse.setOnAction(e->chooseDirectory());
        btnBrowse.setTooltip(new Tooltip("Selecionar pasta de origem"));
        btnBrowseDest.setOnAction(e->chooseDestinationDirectory());
        btnBrowseDest.setTooltip(new Tooltip("Selecionar destino do backup"));
        btnDbOptions.setOnAction(e->showDbOptions());
        btnCopyOrigin.setOnAction(e->copyToClipboard(pathField.getText()));
        btnCopyDest.setOnAction(e->copyToClipboard(destField.getText()));
        consoleArea.setEditable(false);
        consoleArea.setWrapText(true);
        if(Config.hasBackupPasswordHash()){backupPasswordField.setPromptText("Senha configurada (digite para desbloquear)");}
        else{backupPasswordField.setPromptText("Digite a senha do backup");}
        backupPasswordField.textProperty().addListener((obs,oldVal,newVal)->{
            Config.setBackupEncryptionPassword(newVal);
            if(newVal!=null&&!newVal.isBlank()){passwordRequired.set(false);}});
        pathField.textProperty().addListener((o,a,b)->recomputePlanState());
        destField.textProperty().addListener((o,a,b)->recomputePlanState());
        destinationTypeGroup.selectedToggleProperty().addListener((o,a,b)->{
            btnBrowseDest.setDisable(isCloudSelected()||scanning.get());
            btnCopyDest.setDisable(isCloudSelected()||scanning.get());
            recomputePlanState();});
        scheduleMode.getItems().addAll("Diário","Intervalo");
        scheduleMode.getSelectionModel().select("Diário");
        scheduleCheckbox.setSelected(false);
        scheduleTimeField.setText("22:00");
        scheduleIntervalField.setText("120");
        scheduleTimeField.setPromptText("HH:mm");
        scheduleIntervalField.setPromptText("Minutos (ex: 120)");
        scheduleCheckbox.selectedProperty().addListener((o,a,b)->{
            updateScheduleValidity();
            if(suppressScheduleSave)return;
            scheduleSaveHandler.accept(currentScheduleState());});
        scheduleMode.getSelectionModel().selectedItemProperty().addListener((o,a,b)->updateScheduleValidity());
        scheduleTimeField.textProperty().addListener((o,a,b)->updateScheduleValidity());
        scheduleIntervalField.textProperty().addListener((o,a,b)->updateScheduleValidity());
        recomputePlanState();
        updateScheduleValidity();
        retentionSummary.setText("Manter 10 backups");}
    public Node content(){
        var root=new VBox(14);
        root.getStyleClass().add("backup-screen");
        root.setPadding(new Insets(8,0,0,0));
        Label pageTitle=new Label("Configurações");
        pageTitle.getStyleClass().add("page-title");
        Label pageSubtitle=new Label("Parâmetros do plano de backup automatizado");
        pageSubtitle.getStyleClass().add("page-subtitle");
        Node summaryStrip=createSummaryStrip();
        VBox card=new VBox(14);
        card.getStyleClass().addAll("card","backup-plan-card");
        card.setPadding(new Insets(16));
        HBox header=new HBox(10);
        header.setAlignment(Pos.CENTER_LEFT);
        Label h2=new Label("Plano de Backup");
        h2.getStyleClass().add("card-h2");
        Region spacer=new Region();
        HBox.setHgrow(spacer,Priority.ALWAYS);
        HBox segmented=new HBox(0,btnLocal,btnCloud);
        segmented.getStyleClass().add("segmented-host");
        header.getChildren().addAll(h2,spacer,segmented);
        HBox flow=new HBox(14);
        flow.getStyleClass().add("flow-row");
        Node originPanel=createFlowPanel("Origem","O que fazer backup",ICON_FOLDER,pathField,btnCopyOrigin,btnBrowse);
        Label arrow=new Label("→");
        arrow.getStyleClass().add("flow-arrow");
        StackPane arrowWrap=new StackPane(arrow);
        arrowWrap.getStyleClass().add("flow-arrow-wrap");
        Node destPanel=createDestinationPanel();
        flow.getChildren().addAll(originPanel,arrowWrap,destPanel);
        HBox.setHgrow(originPanel,Priority.ALWAYS);
        HBox.setHgrow(destPanel,Priority.ALWAYS);
        Node options=createOptionsList();
        Node progress=createProgressPanel();
        Node logs=createLogsPane();
        card.getChildren().addAll(header,flow,options,progress,logs);
        VBox content=new VBox(14,pageTitle,pageSubtitle,summaryStrip,card);
        content.getStyleClass().add("content-wrap");
        content.setMaxWidth(980);
        root.getChildren().add(content);
        ScrollPane scroll=new ScrollPane(root);
        scroll.setFitToWidth(true);
        scroll.setHbarPolicy(ScrollPane.ScrollBarPolicy.NEVER);
        scroll.getStyleClass().add("content-scroll");
        return scroll;}
    private Node createSummaryStrip(){
        HBox strip=new HBox(10);
        strip.getStyleClass().add("summary-strip");
        strip.setAlignment(Pos.CENTER_LEFT);
        strip.setPadding(new Insets(10,12,10,12));
        StackPane statusIcon=new StackPane();
        statusIcon.getStyleClass().add("summary-icon");
        SVGPath p=new SVGPath();
        p.getStyleClass().add("summary-icon-path");
        statusIcon.getChildren().add(p);
        summaryText.getStyleClass().add("summary-text");
        HBox.setHgrow(summaryText,Priority.ALWAYS);
        summaryBadge.getStyleClass().add("summary-badge");
        strip.getChildren().addAll(statusIcon,summaryText,summaryBadge);
        return strip;}
    private void recomputePlanState(){
        boolean hasOrigin=pathField.getText()!=null&&!pathField.getText().isBlank();
        boolean hasDest=isCloudSelected()||(destField.getText()!=null&&!destField.getText().isBlank());
        boolean valid=hasOrigin&&hasDest;
        planValid.set(valid);
        String origin=safeShort(pathField.getText());
        String dest=isCloudSelected()?"Azure Blob Storage (container-backup)":safeShort(destField.getText());
        summaryText.setText("Origem: "+origin+"  •  Destino: "+dest+"  •  Última: ontem 22:01");
        summaryBadge.setText(valid?"Plano válido":"Atenção");
        summaryBadge.getStyleClass().removeAll("badge-ok","badge-warn");
        summaryBadge.getStyleClass().add(valid?"badge-ok":"badge-warn");
        refreshActionStates();}
    private void updateScheduleValidity(){
        boolean enabled=scheduleCheckbox.isSelected();
        if(!enabled){scheduleValid.set(true);scheduleSummary.setText("Desativado");return;}
        boolean daily="Diário".equalsIgnoreCase(scheduleMode.getValue());
        boolean valid;
        if(daily){
            valid=isValidTime(scheduleTimeField.getText());
            scheduleSummary.setText(valid?"Diário às "+scheduleTimeField.getText().trim():"Horário inválido");
        }else{
            valid=isValidInterval(scheduleIntervalField.getText());
            scheduleSummary.setText(valid?"A cada "+scheduleIntervalField.getText().trim()+" min":"Intervalo inválido");}
        scheduleValid.set(valid);}
    public void setRetentionValue(int retention){
        int safe=Math.max(1,Math.min(retention,365));
        retentionSummary.setText("Manter " + safe + " backups");}
    public void setScheduleState(ScheduleState state){
        if(state==null)return;
        suppressScheduleSave=true;
        scheduleCheckbox.setSelected(state.enabled());
        scheduleMode.getSelectionModel().select(state.mode()==ScheduleMode.INTERVAL?"Intervalo":"Diário");
        scheduleTimeField.setText(state.time()==null?"22:00":state.time());
        scheduleIntervalField.setText(Integer.toString(state.intervalMinutes()));
        updateScheduleValidity();
        suppressScheduleSave=false;}
    public void onScheduleConfigured(java.util.function.Consumer<ScheduleState> handler){
        this.scheduleSaveHandler=(handler==null)?s->{}:handler;}
    public void onRetentionConfigured(java.util.function.IntConsumer handler){
        this.retentionSaveHandler=(handler==null)?v->{}:handler;}
    private ScheduleState currentScheduleState(){
        boolean enabled=scheduleCheckbox.isSelected();
        boolean daily="Diário".equalsIgnoreCase(scheduleMode.getValue());
        String time=scheduleTimeField.getText();
        int interval=120;
        try{interval=Integer.parseInt(scheduleIntervalField.getText().trim());}catch(Exception ignored){}
        return new ScheduleState(enabled,daily?ScheduleMode.DAILY:ScheduleMode.INTERVAL,time,interval);}
    private static boolean isValidTime(String text){
        if(text==null)return false;
        String t=text.trim();
        return t.matches("^([01]\\d|2[0-3]):[0-5]\\d$");}
    private static boolean isValidInterval(String text){
        if(text==null)return false;
        try{int v=Integer.parseInt(text.trim());return v>=15&&v<=1440;}catch(Exception e){return false;}}
    private void openScheduleDialog(){
        Alert alert=new Alert(Alert.AlertType.CONFIRMATION);
        alert.setTitle("Agendamento");
        alert.setHeaderText("Configurar agendamento do backup");
        alert.setGraphic(null);
        ChoiceBox<String> mode=new ChoiceBox<>();
        mode.getItems().addAll("Diário","Intervalo");
        mode.getSelectionModel().select(scheduleMode.getValue());
        TextField timeField=new TextField(scheduleTimeField.getText());
        timeField.setPromptText("HH:mm");
        TextField intervalField=new TextField(scheduleIntervalField.getText());
        intervalField.setPromptText("Minutos (ex: 120)");
        HBox modeRow=new HBox(10,new Label("Modo"),mode);
        modeRow.setAlignment(Pos.CENTER_LEFT);
        HBox timeRow=new HBox(10,new Label("Horário"),timeField);
        timeRow.setAlignment(Pos.CENTER_LEFT);
        HBox intervalRow=new HBox(10,new Label("Intervalo (min)"),intervalField);
        intervalRow.setAlignment(Pos.CENTER_LEFT);
        timeRow.visibleProperty().bind(mode.getSelectionModel().selectedItemProperty().isEqualTo("Diário"));
        timeRow.managedProperty().bind(timeRow.visibleProperty());
        intervalRow.visibleProperty().bind(mode.getSelectionModel().selectedItemProperty().isEqualTo("Intervalo"));
        intervalRow.managedProperty().bind(intervalRow.visibleProperty());
        Label hint=new Label("Use HH:mm ou intervalo entre 15 e 1440 minutos.");
        hint.getStyleClass().addAll("input-hint","hint-error");
        VBox content=new VBox(10,modeRow,timeRow,intervalRow,hint);
        content.setPadding(new Insets(8,0,0,0));
        alert.getDialogPane().setContent(content);
        try{alert.getDialogPane().getStylesheets().add(getClass().getResource("/styles.css").toExternalForm());}catch(Exception ignored){}
        var okButton=alert.getDialogPane().lookupButton(ButtonType.OK);
        Runnable validate=()->{
            boolean daily="Diário".equalsIgnoreCase(mode.getValue());
            boolean valid=daily?isValidTime(timeField.getText()):isValidInterval(intervalField.getText());
            okButton.setDisable(!valid);
            hint.setVisible(!valid);
            hint.setManaged(!valid);};
        mode.getSelectionModel().selectedItemProperty().addListener((o,a,b)->validate.run());
        timeField.textProperty().addListener((o,a,b)->validate.run());
        intervalField.textProperty().addListener((o,a,b)->validate.run());
        validate.run();
        Optional<ButtonType> res=alert.showAndWait();
        if(res.isEmpty()||res.get()!=ButtonType.OK)return;
        boolean daily="Diário".equalsIgnoreCase(mode.getValue());
        String time=timeField.getText().trim();
        int interval=120;
        try{interval=Integer.parseInt(intervalField.getText().trim());}catch(Exception ignored){}
        ScheduleState state=new ScheduleState(true,daily?ScheduleMode.DAILY:ScheduleMode.INTERVAL,time,interval);
        setScheduleState(state);
        scheduleSaveHandler.accept(state);}
    private void openRetentionDialog(){
        Alert alert=new Alert(Alert.AlertType.CONFIRMATION);
        alert.setTitle("Retenção");
        alert.setHeaderText("Configurar retenção de backups");
        alert.setGraphic(null);
        TextField retentionField=new TextField();
        retentionField.setPromptText("Quantidade (ex: 10)");
        retentionField.setText(retentionSummary.getText().replaceAll("\\D+","").trim());
        VBox content=new VBox(8,new Label("Manter no máximo"),retentionField);
        content.setPadding(new Insets(6,0,0,0));
        alert.getDialogPane().setContent(content);
        try{alert.getDialogPane().getStylesheets().add(getClass().getResource("/styles.css").toExternalForm());}catch(Exception ignored){}
        var okButton=alert.getDialogPane().lookupButton(ButtonType.OK);
        okButton.disableProperty().bind(retentionField.textProperty().isEmpty());
        Optional<ButtonType> res=alert.showAndWait();
        if(res.isEmpty()||res.get()!=ButtonType.OK)return;
        try{
            int v=Integer.parseInt(retentionField.getText().trim());
            int safe=Math.max(1,Math.min(v,365));
            retentionSummary.setText("Manter "+safe+" backups");
            retentionSaveHandler.accept(safe);
        }catch(Exception e){showError("Valor inválido","Informe um número entre 1 e 365.");}}
    private static String safeShort(String s){
        if(s==null||s.isBlank())return "-";
        if(s.length()<=58)return s;
        return s.substring(0,26)+"…"+s.substring(s.length()-28);}
    private void refreshActionStates(){
        boolean isScanning=scanning.get();
        boolean valid=planValid.get();
        btnScan.setDisable(isScanning||!valid);
        btnWipe.setDisable(isScanning);
        btnBrowse.setDisable(isScanning);
        btnBrowseDest.setDisable(isScanning||isCloudSelected());
        btnCopyOrigin.setDisable(isScanning);
        btnCopyDest.setDisable(isScanning||isCloudSelected());
        pathField.setDisable(isScanning);
        destField.setDisable(isScanning||isCloudSelected());
        btnStop.setDisable(!isScanning);
        double opacity=isScanning?0.72:1.0;
        pathField.setOpacity(opacity);
        destField.setOpacity(opacity);}
    private Node createProgressPanel(){
        progressRing.setMaxSize(20,20);
        progressRing.setMinSize(20,20);
        progressRing.progressProperty().bind(model.progressProperty);
        progressBar.getStyleClass().add("metric-progress");
        progressBar.setMaxWidth(Double.MAX_VALUE);
        progressBar.progressProperty().bind(model.progressProperty);
        progressLabel.getStyleClass().add("muted");
        progressLabel.textProperty().bind(model.phaseProperty);
        HBox top=new HBox(10,progressRing,progressLabel);
        top.setAlignment(Pos.CENTER_LEFT);
        VBox box=new VBox(8,top,progressBar);
        box.setPadding(new Insets(8,0,0,0));
        box.visibleProperty().bind(scanning);
        box.managedProperty().bind(scanning);
        HBox.setHgrow(progressBar,Priority.ALWAYS);
        return box;}
    public Node footer(){
        var root=new HBox(12);
        root.getStyleClass().add("footer");
        root.setAlignment(Pos.CENTER_LEFT);
        root.setPadding(new Insets(10,0,0,0));
        VBox left=new VBox(2);
        Label state=new Label();
        state.getStyleClass().add("footer-hint");
        state.textProperty().bind(planValid.map(v->v?"Pronto para executar":"Configuração incompleta"));
        left.getChildren().add(state);
        styleIconButton(btnScan,ICON_PLAY);
        btnScan.getStyleClass().addAll("btn","btn-primary");
        btnScan.setMinWidth(170);
        VBox leftCluster=new VBox(6,left,btnScan);
        leftCluster.setAlignment(Pos.CENTER_LEFT);
        Region spacer=new Region();
        HBox.setHgrow(spacer,Priority.ALWAYS);
        backupFooterActions.setAlignment(Pos.CENTER_RIGHT);
        styleIconButton(btnStop,ICON_STOP);
        styleIconButton(btnWipe,ICON_TRASH);
        btnStop.getStyleClass().addAll("btn","btn-secondary");
        btnWipe.getStyleClass().addAll("btn","btn-danger-outline");
        btnDbOptions.getStyleClass().addAll("btn","btn-secondary");
        btnStop.setMinWidth(92);
        btnWipe.setTooltip(new Tooltip("Apaga o histórico (SQLite) e o cofre de backups (.keeply/storage)."));
        backupFooterActions.getChildren().setAll(btnStop,btnWipe,btnDbOptions);
        root.getChildren().addAll(leftCluster,spacer,backupFooterActions);
        return root;}
    private Node createFlowPanel(String title,String subtitle,String iconPath,TextField boundPath,Button copyBtn,Button actionButton){
        VBox panel=new VBox(10);
        panel.getStyleClass().add("flow-panel");
        panel.setPadding(new Insets(12));
        HBox top=new HBox(10);
        top.setAlignment(Pos.CENTER_LEFT);
        SVGPath icon=new SVGPath();
        icon.setContent(iconPath);
        icon.getStyleClass().add("flow-icon");
        VBox titles=new VBox(2);
        Label t=new Label(title);
        t.getStyleClass().add("flow-title");
        Label st=new Label(subtitle);
        st.getStyleClass().add("flow-subtitle");
        titles.getChildren().addAll(t,st);
        top.getChildren().addAll(icon,titles);
        HBox pathRow=new HBox(8);
        pathRow.setAlignment(Pos.CENTER_LEFT);
        boundPath.getStyleClass().add("path-field");
        HBox.setHgrow(boundPath,Priority.ALWAYS);
        copyBtn.getStyleClass().addAll("btn","btn-icon");
        pathRow.getChildren().addAll(boundPath,copyBtn);
        actionButton.getStyleClass().addAll("btn","btn-outline");
        actionButton.setMinWidth(150);
        panel.getChildren().addAll(top,pathRow,actionButton);
        return panel;}
    private Node createDestinationPanel(){
        VBox panel=new VBox(10);
        panel.getStyleClass().add("flow-panel");
        panel.setPadding(new Insets(12));
        HBox top=new HBox(10);
        top.setAlignment(Pos.CENTER_LEFT);
        SVGPath icon=new SVGPath();
        icon.setContent(ICON_CLOUD);
        icon.getStyleClass().add("flow-icon");
        VBox titles=new VBox(2);
        Label t=new Label("Destino");
        t.getStyleClass().add("flow-title");
        Label st=new Label("Onde armazenar");
        st.getStyleClass().add("flow-subtitle");
        titles.getChildren().addAll(t,st);
        top.getChildren().addAll(icon,titles);
        HBox row=new HBox(8);
        row.setAlignment(Pos.CENTER_LEFT);
        Label cloudLabel=new Label("Azure Blob Storage (container-backup)");
        cloudLabel.getStyleClass().add("cloud-pill");
        destField.getStyleClass().add("path-field");
        HBox.setHgrow(destField,Priority.ALWAYS);
        btnCopyDest.getStyleClass().addAll("btn","btn-icon");
        StackPane switcher=new StackPane();
        switcher.getStyleClass().add("dest-switcher");
        switcher.getChildren().addAll(destField,cloudLabel);
        cloudLabel.visibleProperty().bind(destinationTypeGroup.selectedToggleProperty().isEqualTo(btnCloud));
        cloudLabel.managedProperty().bind(destinationTypeGroup.selectedToggleProperty().isEqualTo(btnCloud));
        destField.visibleProperty().bind(destinationTypeGroup.selectedToggleProperty().isNotEqualTo(btnCloud));
        destField.managedProperty().bind(destinationTypeGroup.selectedToggleProperty().isNotEqualTo(btnCloud));
        btnCopyDest.visibleProperty().bind(destinationTypeGroup.selectedToggleProperty().isNotEqualTo(btnCloud));
        btnCopyDest.managedProperty().bind(destinationTypeGroup.selectedToggleProperty().isNotEqualTo(btnCloud));
        row.getChildren().addAll(switcher,btnCopyDest);
        btnBrowseDest.getStyleClass().addAll("btn","btn-outline");
        btnBrowseDest.setMinWidth(150);
        panel.getChildren().addAll(top,row,btnBrowseDest);
        return panel;}
    private Node createOptionsList(){
        VBox wrap=new VBox(10);
        wrap.getStyleClass().add("options-wrap");
        HBox titleRow=new HBox(8);
        titleRow.setAlignment(Pos.CENTER_LEFT);
        Label title=new Label("Opções de Backup");
        title.getStyleClass().add("options-title");
        Region spacer=new Region();
        HBox.setHgrow(spacer,Priority.ALWAYS);
        optionsChevron.getStyleClass().add("options-chevron");
        titleRow.getChildren().addAll(optionsChevron,title,spacer);
        VBox list=new VBox(0);
        list.getStyleClass().add("options-list");
        list.visibleProperty().bind(optionsExpanded);
        list.managedProperty().bind(optionsExpanded);
        titleRow.setOnMouseClicked(e->optionsExpanded.set(!optionsExpanded.get()));
        optionsExpanded.addListener((o,a,b)->optionsChevron.setText(b?"▾":"▸"));
        list.getChildren().addAll(optionRowSchedule(),optionRowRetention(),optionRowEncryption(),optionRowStatic("Integridade","Checksum SHA-256",true,null));
        wrap.getChildren().addAll(titleRow,list);
        return wrap;}
    private Node optionRowStatic(String name,String summary,boolean ok,String actionText){
        HBox row=new HBox(10);
        row.getStyleClass().add("option-row2");
        row.setAlignment(Pos.CENTER_LEFT);
        row.setPadding(new Insets(10,12,10,12));
        Label leftName=new Label(name);
        leftName.getStyleClass().add("option-name");
        Label leftSummary=new Label(summary);
        leftSummary.getStyleClass().add("option-summary");
        Region spacer=new Region();
        HBox.setHgrow(spacer,Priority.ALWAYS);
        Node action=new Region();
        if(actionText!=null){
            Button configure=new Button(actionText);
            configure.getStyleClass().addAll("btn","btn-link");
            configure.setOnAction(e->appendLog("Abrir configuração: "+name));
            action=configure;}
        Node status=ok?statusPillOk():statusPillWarn();
        row.getChildren().addAll(leftName,leftSummary,spacer,action,status);
        return row;}
    private Node optionRowEncryption(){
        HBox row=new HBox(10);
        row.getStyleClass().add("option-row2");
        row.setAlignment(Pos.CENTER_LEFT);
        row.setPadding(new Insets(10,12,10,12));
        Label name=new Label("Criptografia");
        name.getStyleClass().add("option-name");
        Label summary=new Label();
        summary.getStyleClass().add("option-summary");
        summary.textProperty().bind(encryptionCheckbox.selectedProperty().map(v->v?"Ativado":"Não configurado"));
        Region spacer=new Region();
        HBox.setHgrow(spacer,Priority.ALWAYS);
        CheckBox sw=encryptionCheckbox;
        sw.getStyleClass().add("switch");
        Button configure=new Button("Configurar");
        configure.getStyleClass().addAll("btn","btn-link");
        configure.setOnAction(e->{
            String entered=promptSetPassword();
            if(entered==null||entered.isBlank())return;
            if(Config.hasBackupPasswordHash()&&!Config.verifyAndCacheBackupPassword(entered)){
                showError("Senha inválida","A senha informada não confere.");
                return;}
            Config.verifyAndCacheBackupPassword(entered);
            Config.setBackupEncryptionPassword(entered);
            backupPasswordField.setText(entered);
            passwordRequired.set(false);
            suppressEncryptionToggle=true;
            sw.setSelected(true);
            suppressEncryptionToggle=false;
            Config.saveBackupEncryptionEnabled(true);});
        sw.setSelected(Config.isBackupEncryptionEnabled());
        sw.selectedProperty().addListener((obs,oldVal,newVal)->{
            if(suppressEncryptionToggle)return;
            if(!oldVal&&newVal){
                String pass=backupPasswordField.getText();
                if(pass==null||pass.isBlank()){
                    String entered=promptSetPassword();
                    if(entered==null||entered.isBlank()){
                        passwordRequired.set(true);
                        suppressEncryptionToggle=true;
                        sw.setSelected(false);
                        suppressEncryptionToggle=false;
                        return;}
                    if(Config.hasBackupPasswordHash()&&!Config.verifyAndCacheBackupPassword(entered)){
                        showError("Senha inválida","A senha informada não confere.");
                        suppressEncryptionToggle=true;
                        sw.setSelected(false);
                        suppressEncryptionToggle=false;
                        return;}
                    Config.verifyAndCacheBackupPassword(entered);
                    Config.setBackupEncryptionPassword(entered);
                    backupPasswordField.setText(entered);
                    passwordRequired.set(false);}}
            if(oldVal&&!newVal){
                if(!confirmDisableEncryption()){
                    suppressEncryptionToggle=true;
                    sw.setSelected(true);
                    suppressEncryptionToggle=false;
                    return;}
                Config.clearBackupPassword();
                backupPasswordField.clear();
                passwordRequired.set(false);}
            Config.saveBackupEncryptionEnabled(newVal);});
        Node status=statusFromEncryption();
        row.getChildren().addAll(name,summary,spacer,configure,sw,status);
        VBox wrap=new VBox(0);
        wrap.getChildren().add(row);
        wrap.getChildren().add(new Separator());
        return wrap;}
    private Node optionRowSchedule(){
        HBox row=new HBox(10);
        row.getStyleClass().add("option-row2");
        row.setAlignment(Pos.CENTER_LEFT);
        row.setPadding(new Insets(10,12,10,12));
        Label name=new Label("Agendamento");
        name.getStyleClass().add("option-name");
        scheduleSummary.getStyleClass().add("option-summary");
        Region spacer=new Region();
        HBox.setHgrow(spacer,Priority.ALWAYS);
        scheduleCheckbox.getStyleClass().add("switch");
        Button configure=new Button("Configurar");
        configure.getStyleClass().addAll("btn","btn-link");
        configure.setOnAction(e->openScheduleDialog());
        Node status=statusFromSchedule();
        row.getChildren().addAll(name,scheduleSummary,spacer,configure,scheduleCheckbox,status);
        VBox wrap=new VBox(0);
        wrap.getChildren().add(row);
        wrap.getChildren().add(new Separator());
        return wrap;}
    private Node optionRowRetention(){
        HBox row=new HBox(10);
        row.getStyleClass().add("option-row2");
        row.setAlignment(Pos.CENTER_LEFT);
        row.setPadding(new Insets(10,12,10,12));
        Label name=new Label("Retenção");
        name.getStyleClass().add("option-name");
        retentionSummary.getStyleClass().add("option-summary");
        Region spacer=new Region();
        HBox.setHgrow(spacer,Priority.ALWAYS);
        Button configure=new Button("Configurar");
        configure.getStyleClass().addAll("btn","btn-link");
        configure.setOnAction(e->openRetentionDialog());
        Node status=statusPillOk();
        row.getChildren().addAll(name,retentionSummary,spacer,configure,status);
        return row;}
    private Node statusFromSchedule(){
        StackPane holder=new StackPane();
        holder.getChildren().add(statusPillWarn());
        scheduleCheckbox.selectedProperty().addListener((o,a,b)->updateScheduleStatus(holder));
        scheduleValid.addListener((o,a,b)->updateScheduleStatus(holder));
        updateScheduleStatus(holder);
        return holder;}
    private void updateScheduleStatus(StackPane holder){
        boolean enabled=scheduleCheckbox.isSelected();
        boolean ok=enabled&&scheduleValid.get();
        holder.getChildren().setAll(ok?statusPillOk():statusPillWarn());}
    private Node statusFromEncryption(){
        StackPane holder=new StackPane();
        holder.getChildren().add(statusPillWarn());
        encryptionCheckbox.selectedProperty().addListener((o,a,b)->{holder.getChildren().setAll(b?statusPillOk():statusPillWarn());});
        holder.getChildren().setAll(encryptionCheckbox.isSelected()?statusPillOk():statusPillWarn());
        return holder;}
    private Node statusPillOk(){
        HBox pill=new HBox(6);
        pill.getStyleClass().addAll("status-pill","status-ok");
        pill.setAlignment(Pos.CENTER);
        SVGPath icon=new SVGPath();
        icon.setContent(ICON_CHECK);
        icon.getStyleClass().add("status-icon");
        pill.getChildren().add(icon);
        return pill;}
    private Node statusPillWarn(){
        HBox pill=new HBox(6);
        pill.getStyleClass().addAll("status-pill","status-warn");
        pill.setAlignment(Pos.CENTER);
        SVGPath icon=new SVGPath();
        icon.setContent(ICON_WARN);
        icon.getStyleClass().add("status-icon");
        pill.getChildren().add(icon);
        return pill;}
    private Node createLogsPane(){
        consoleArea.getStyleClass().add("console");
        consoleArea.setPrefRowCount(6);
        TitledPane pane=new TitledPane("Logs",consoleArea);
        pane.getStyleClass().add("logs-pane");
        pane.setExpanded(false);
        return pane;}
    private static void styleIconButton(Button btn,String svgPath){
        var icon=new SVGPath();
        icon.setContent(svgPath);
        icon.getStyleClass().add("icon");
        btn.setGraphic(icon);
        btn.setGraphicTextGap(8);}
    private static Button iconOnlyButton(String svgPath,String tooltip){
        Button b=new Button();
        SVGPath icon=new SVGPath();
        icon.setContent(svgPath);
        icon.getStyleClass().add("icon");
        b.setGraphic(icon);
        b.setTooltip(new Tooltip(tooltip));
        b.setFocusTraversable(false);
        return b;}
    private static void copyToClipboard(String value){
        if(value==null)return;
        ClipboardContent cc=new ClipboardContent();
        cc.putString(value);
        Clipboard.getSystemClipboard().setContent(cc);}
    private void chooseDirectory(){
        DirectoryChooser dc=new DirectoryChooser();
        File initial=new File(Objects.requireNonNullElse(Config.getLastPath(),System.getProperty("user.home")));
        if(initial.exists()&&initial.isDirectory())dc.setInitialDirectory(initial);
        dc.setTitle("Selecionar pasta de origem");
        File f=dc.showDialog(stage);
        if(f!=null){
            pathField.setText(f.getAbsolutePath());
            Config.saveLastPath(f.getAbsolutePath());
            recomputePlanState();}}
    private void chooseDestinationDirectory(){
        DirectoryChooser dc=new DirectoryChooser();
        File initial=new File(Objects.requireNonNullElse(Config.getLastBackupDestination(),defaultLocalBackupDestination().toString()));
        if(initial.exists()&&initial.isDirectory())dc.setInitialDirectory(initial);
        dc.setTitle("Selecionar destino do backup");
        File f=dc.showDialog(stage);
        if(f!=null){
            destField.setText(f.getAbsolutePath());
            Config.saveLastBackupDestination(f.getAbsolutePath());
            try{Files.createDirectories(f.toPath());}catch(java.io.IOException|RuntimeException ignored){}
            recomputePlanState();
        }}
    private static Path defaultLocalBackupDestination(){
        String home=Objects.requireNonNullElse(System.getProperty("user.home"),".");
        return Path.of(home,"Documents","Keeply","Backup");}
    public boolean isCloudSelected(){
        return destinationTypeGroup.getSelectedToggle()==btnCloud;}
    private void showDbOptions(){
        DatabaseBackup.DbEncryptionStatus s=DatabaseBackup.getEncryptionStatus();
        String text="""
            Status da Criptografia:
            Ativado: %s

            Arquivos:
            .enc exists: %s
            Legacy plain exists: %s
            """.formatted(s.encryptionEnabled(),s.encryptedFileExists(),s.legacyPlainExists());
        Alert alert=new Alert(Alert.AlertType.INFORMATION);
        alert.setTitle("Opções do Banco de Dados");
        alert.setHeaderText("Diagnóstico de segurança");
        TextArea area=new TextArea(text);
        area.setEditable(false);
        area.setWrapText(true);
        area.setPrefRowCount(8);
        alert.getDialogPane().setContent(area);
        alert.showAndWait();}
    private boolean confirmDisableEncryption(){
        String pass=promptPassword("Desativar criptografia","Confirme a senha do backup");
        if(pass==null)return false;
        if(Config.hasBackupPasswordHash()){
            if(!Config.verifyAndCacheBackupPassword(pass)){
                showError("Senha inválida","A senha informada não confere. Criptografia mantida.");
                return false;}}else{
            String session=Config.getBackupEncryptionPassword();
            if(session==null||session.isBlank()||!session.equals(pass)){
                showError("Senha inválida","A senha informada não confere. Criptografia mantida.");
                return false;}
            Config.setBackupEncryptionPassword(pass);}
        if(!BlobStore.verifyBackupPassword(pass)){
            showError("Falha de verificação","Não foi possível validar a senha em um teste de descriptografia.");
            return false;}
        Config.setBackupEncryptionPassword(pass);
        return true;}
    private String promptPassword(String title,String header){
        Alert alert=new Alert(Alert.AlertType.CONFIRMATION);
        alert.setTitle(title);
        alert.setHeaderText(header);
        alert.setGraphic(null);
        PasswordField field=new PasswordField();
        field.setPromptText("Senha do backup");
        VBox content=new VBox(8,field);
        content.setPadding(new Insets(6,0,0,0));
        alert.getDialogPane().setContent(content);
        try{alert.getDialogPane().getStylesheets().add(getClass().getResource("/styles.css").toExternalForm());}catch(RuntimeException ignored){}
        var okButton=alert.getDialogPane().lookupButton(ButtonType.OK);
        okButton.disableProperty().bind(field.textProperty().isEmpty());
        Optional<ButtonType> res=alert.showAndWait();
        if(res.isEmpty()||res.get()!=ButtonType.OK)return null;
        return field.getText();}
    private String promptSetPassword(){
        Alert alert=new Alert(Alert.AlertType.CONFIRMATION);
        alert.setTitle("Definir senha");
        alert.setHeaderText("Defina a senha do backup");
        alert.setGraphic(null);
        PasswordField field=new PasswordField();
        field.setPromptText("Senha do backup");
        VBox content=new VBox(8,field);
        content.setPadding(new Insets(6,0,0,0));
        alert.getDialogPane().setContent(content);
        try{alert.getDialogPane().getStylesheets().add(getClass().getResource("/styles.css").toExternalForm());}catch(Exception ignored){}
        var okButton=alert.getDialogPane().lookupButton(ButtonType.OK);
        okButton.disableProperty().bind(field.textProperty().isEmpty());
        Optional<ButtonType> res=alert.showAndWait();
        if(res.isEmpty()||res.get()!=ButtonType.OK)return null;
        return field.getText();}
    private void showError(String title,String message){
        Alert alert=new Alert(Alert.AlertType.ERROR);
        alert.setTitle(title);
        alert.setHeaderText(title);
        alert.setContentText(message);
        alert.showAndWait();}
    public Button getScanButton(){return btnScan;}
    public Button getStopButton(){return btnStop;}
    public Button getWipeButton(){return btnWipe;}
    public String getRootPathText(){return pathField.getText();}
    public String getBackupDestinationText(){return destField.getText();}
    public String getBackupEncryptionPassword(){return backupPasswordField.getText();}
    public void setScanningState(boolean isScanning){
        scanning.set(isScanning);
        refreshActionStates();}
    public void clearConsole(){consoleArea.clear();}
    public void appendLog(String message){
        Platform.runLater(()->{
            consoleArea.appendText("• "+message+"\n");
            consoleArea.positionCaret(consoleArea.getLength());});}
    public boolean isEncryptionEnabled(){
        return encryptionCheckbox.isSelected();}
    public boolean confirmWipe(){
        Alert alert=new Alert(Alert.AlertType.CONFIRMATION);
        alert.setTitle("Confirmar remoção");
        alert.setHeaderText("Apagar backups do Keeply?");
        alert.setContentText("""
            Isso apagará:
            • O histórico/banco de dados (SQLite)
            • Os binários armazenados no cofre (.keeply/storage)

            Isso NÃO apaga os arquivos originais da sua pasta de origem.
            """);
        Optional<ButtonType> res=alert.showAndWait();
        return res.isPresent()&&res.get()==ButtonType.OK;}}