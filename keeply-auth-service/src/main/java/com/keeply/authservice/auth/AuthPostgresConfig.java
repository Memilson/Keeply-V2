package com.keeply.authservice.auth;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;

@Configuration
public class AuthPostgresConfig {

    @Bean(name = "authDataSource")
    public DataSource authDataSource(
            @Value("${keeply.auth.pg.url:}") String url,
            @Value("${keeply.auth.pg.username:}") String username,
            @Value("${keeply.auth.pg.password:}") String password
    ) {
        if (url == null || url.isBlank()) {
            return null;
        }

        HikariConfig cfg = new HikariConfig();
        cfg.setJdbcUrl(url);
        cfg.setUsername(username);
        cfg.setPassword(password);
        cfg.setMaximumPoolSize(4);
        cfg.setMinimumIdle(1);
        cfg.setPoolName("keeply-auth-pg");
        cfg.setConnectionTimeout(10000);
        cfg.setValidationTimeout(5000);
        cfg.setConnectionTestQuery("SELECT 1");
        return new HikariDataSource(cfg);
    }
}
