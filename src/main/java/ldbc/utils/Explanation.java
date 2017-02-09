/**
 * Explanation utilities.
 *
 * Copyright © 2017 Alain Kägi
 */

package ldbc.utils;

import java.io.PrintStream;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class Explanation {

    /** The MySQL explain command followed by a space. */
    public static String query = "EXPLAIN ";

    /** Record separator. */
    private static String separator = "\t";

    /** Print a result set in tab-separated lines. */
    public static void print(PrintStream o, ResultSet rs) throws SQLException {
        ResultSetMetaData meta = rs.getMetaData();
        int columns = meta.getColumnCount();

        // Headings
        for (int i = 1; i <= columns; i++) {
            if (i > 1) o.print(separator);
            o.print(meta.getColumnName(i));
        }
        o.println();

        // Content
        while (rs.next()) {
            for (int i = 1; i <= columns; i++) {
                if (i > 1) o.print(separator);
                o.print(rs.getString(i));
            }
            o.println();
        }
    }

}
