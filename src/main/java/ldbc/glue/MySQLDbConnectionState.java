/*
 * Copyright © 2018 Alain Kägi
 */

package ldbc.glue;

import com.ldbc.driver.DbConnectionState;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.io.IOException;

import ldbc.utils.Db;

/**
 * The MySQLDbConnectionState class implements a connection manager
 * for this MySQL-based LDBC SNB implementation.
 */
public class MySQLDbConnectionState extends DbConnectionState {

    private HikariDataSource client;

    public MySQLDbConnectionState(String url, String user, String password) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(url);
        config.setUsername(user);
        config.setPassword(password);
        config.setAutoCommit(false);

        client = new HikariDataSource(config);
    }

    public HikariDataSource getClient() {
        return client;
    }

    @Override
    public void close() throws IOException {
    }
}
