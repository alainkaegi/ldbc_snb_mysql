/*
 * Copyright © 2018 Alain Kägi
 */

package ldbc.glue;

import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;

import java.io.IOException;

import java.sql.Connection;
import java.sql.SQLException;

//import ldbc.utils.Configuration;
import ldbc.utils.Db;

/**
 * The MySQLDbConnectionState class implements a connection manager
 * for this MySQL-based LDBC SNB implementation.
 */
public class MySQLDbConnectionState extends DbConnectionState {

    private Connection client;

    public MySQLDbConnectionState(String url, String user, String password) throws DbException {
        //Configuration config = new Configuration();

        //String url = "jdbc:mysql://localhost/" + config.database();
        try {
            client = Db.connect(url, user, password);
        }
        catch (SQLException e) {
            throw new DbException(e.getMessage());
        }
    }

    public Connection getClient() {
        return client;
    }

    @Override
    public void close() throws IOException {
    }
}
