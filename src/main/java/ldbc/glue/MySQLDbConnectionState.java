/*
 * Copyright © 2018-2019 Alain Kägi
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
        config.addDataSourceProperty("cachePrepStmts", true);
        config.addDataSourceProperty("prepStmts", true);
        config.addDataSourceProperty("prepStmtCacheSize", 250);
        config.addDataSourceProperty("dataSource.prepStmtCacheSqlLimit", 2048);
        config.addDataSourceProperty("dataSource.useServerPrepStmts", true);
        config.addDataSourceProperty("dataSource.useLocalSessionState", true);
        config.addDataSourceProperty("dataSource.rewriteBatchedStatements", true);
        config.addDataSourceProperty("dataSource.cacheResultSetMetadata", true);
        config.addDataSourceProperty("dataSource.cacheServerConfiguration", true);
        config.addDataSourceProperty("dataSource.elideSetAutoCommits", true);
        config.addDataSourceProperty("dataSource.maintainTimeStats", false);

        client = new HikariDataSource(config);
    }

    public HikariDataSource getClient() {
        return client;
    }

    @Override
    public void close() throws IOException {
    }
}
