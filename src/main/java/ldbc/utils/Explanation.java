/*
 * Copyright © 2017 Alain Kägi
 */

package ldbc.utils;

import java.io.PrintStream;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

/**
 * The Explanation class defines static functions to print out an
 * execution plan for a given query.
 *
 * <p>These definitions may only work with MySQL.
 */
public class Explanation {

    // Suppress the default constructor.
    private Explanation() {}

    /**
     * The MySQL explain command followed by a space.
     *
     * <p>This string can be simply prepended to any query before
     * submitting the entire command to get an execution plan.
     */
    public static String query = "EXPLAIN ";

    /**
     * Record separator.
     *
     * <p>Used when printing the query execution plan.
     */
    private static String separator = "\t";

    /**
     * Print the query execution plan in tab-separated lines.
     * @param o  The output stream to which the information should be printed
     * @param r  The query execution plan encoded as a result set
     * @throws SQLException if a database access error occurs
     */
    public static void print(PrintStream o, ResultSet r) throws SQLException {
        ResultSetMetaData meta = r.getMetaData();
        int columns = meta.getColumnCount();

        // Headings
        for (int i = 1; i <= columns; i++) {
            if (i > 1) o.print(separator);
            o.print(meta.getColumnName(i));
        }
        o.println();

        // Content
        while (r.next()) {
            for (int i = 1; i <= columns; i++) {
                if (i > 1) o.print(separator);
                o.print(r.getString(i));
            }
            o.println();
        }
    }

}
