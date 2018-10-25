/*
 * Copyright © 2017 Alain Kägi
 */

package ldbc.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * The Db class defines a static method to connect to a MySQL database.
 *
 * <p>To provide this functionality it loads the MySQL JDBC driver in
 * a static initializer.
 */
public class Db {

    // Suppress the default constructor.
    private Db() {}

    // Load the MySQL JDBC driver early.
    static {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        }
        catch (ClassNotFoundException e) {
            // Never expect this class loading to fail.
        }
    }

    /**
     * Connect to the database found at the given URL with the given
     * user and password.
     * @param url       A database URL
     * @param user      The database user on whose behalf the connection is being made
     * @param password  The user's password
     * @return a connection handle to the database
     * @throws SQLException if a database access error occurs
     */
    public static Connection connect(String url, String user, String password) throws SQLException {
        return DriverManager.getConnection(url, user, password);
    }

}
