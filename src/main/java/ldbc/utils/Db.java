/**
 * A MySQL connection utility.
 *
 * Copyright © 2017 Alain Kägi
 */

package ldbc.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class Db {

    /** Load the MySQL JDBC driver early. */
    static {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        }
        catch (ClassNotFoundException e) {
            // Never expect this class loading to fail.
        }
    }

    /** Connect to the database found at the given URL with the given user and password. */
    public static Connection connect(String url, String user, String password) throws SQLException {
        return DriverManager.getConnection(url, user, password);
    }

}
