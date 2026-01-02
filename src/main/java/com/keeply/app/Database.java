package com.keeply.app;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.exception.FlywayValidateException;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;

public final class Database {

    private static final Logger logger = LoggerFactory.getLogger(Database.class);

    private Database() {}

    // --- RECORDS (Data Models) ---
    public record InventoryRow(String rootPath, String pathRel, String name, long sizeBytes, long modifiedMillis,
                               long createdMillis, String status) {}
    public record ScanSummary(long scanId, String rootPath, String startedAt, String finishedAt) {}
    public record FileHistoryRow(long scanId, String rootPath, String startedAt, String finishedAt, long sizeBytes,
                                 String statusEvent, String createdAt) {}
    public record CapacityReport(String date, long totalBytes, long growthBytes) {}

    // --- CONNECTION POOL (HikariCP singleton) ---
    private static HikariDataSource dataSource;
    private static Jdbi jdbi;
    private static boolean migrated = false;

    private static synchronized void createDataSource() {
        if (dataSource != null) return;
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(Config.getDbUrl());
        config.addDataSourceProperty("cipher", "sqlcipher");
        config.addDataSourceProperty("key", Config.getSecretKey());
        config.addDataSourceProperty("legacy", "0");
        config.addDataSourceProperty("kdf_iter", "64000");

        config.setConnectionTestQuery("SELECT 1");
        config.setMaximumPoolSize(10);
        // WAL and synchronous NORMAL keep DbWriter fast on SQLite.
        config.setConnectionInitSql("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; PRAGMA busy_timeout=10000;");

        dataSource = new HikariDataSource(config);
    }

    public static synchronized void init() {
        createDataSource();
        if (jdbi == null) {
            jdbi = Jdbi.create(dataSource);
            jdbi.installPlugin(new SqlObjectPlugin());
        }
        migrateIfNeeded();
    }

    private static synchronized void migrateIfNeeded() {
        if (migrated) return;
        Flyway flyway = Flyway.configure()
                .dataSource(dataSource)
                .locations("classpath:db/migration")
                .baselineOnMigrate(true)
                .baselineVersion("0")
                .load();
        try {
            try {
                flyway.migrate();
            } catch (FlywayValidateException e) {
                logger.warn("Flyway validation failed; attempting repair.", e);
                flyway.repair();
                flyway.migrate();
            }
            migrated = true;
        } catch (Exception e) {
            logger.error("Flyway migration failed", e);
            throw new IllegalStateException("Flyway migration failed", e);
        }
    }

    public static synchronized void shutdown() {
        if (dataSource != null) {
            try { dataSource.close(); } catch (Exception ignored) {}
            dataSource = null;
        }
        jdbi = null;
        migrated = false;
    }

    public static Jdbi jdbi() {
        init();
        return jdbi;
    }

    public static final class SimplePool implements AutoCloseable {
        public SimplePool(String jdbcUrl, int poolSize) {
            // Pool size is managed by the shared Hikari pool.
        }

        public Connection borrow() throws SQLException {
            init();
            Connection c = dataSource.getConnection();
            try { c.setAutoCommit(false); } catch (Exception ignored) {}
            return c;
        }

        @Override public void close() { }
    }

    public static Connection openSingleConnection() throws SQLException {
        init();
        if (dataSource == null) throw new SQLException("Datasource not initialized");
        Connection c = dataSource.getConnection();
        try { c.setAutoCommit(false); } catch (Exception ignored) {}
        return c;
    }

    public static void safeClose(Connection c) {
        if (c == null) return;
        try { c.close(); } catch (Exception ignored) {}
    }
}
