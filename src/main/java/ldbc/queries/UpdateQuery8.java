/*
 * Copyright © 2018-2019 Alain Kägi
 */

package ldbc.queries;

import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate8AddFriendship;

import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * The UpdateQuery8 class defines the MySQL-based update query 8.
 */
public class UpdateQuery8 {

    /**
     * Add a friendship.
     * @param ds          A data source
     * @param parameters  The parameters of this transaction
     * @throws SQLException if a database access error occurs
     */
    public static void query(HikariDataSource ds, LdbcUpdate8AddFriendship parameters) throws SQLException {
        String query =
            "   INSERT INTO PersonKnowsPerson " +
            "        VALUES (?, " + // person1Id
            "                ?, " + // person2Id
            "                ?)";   // creationDate
        try (Connection c = ds.getConnection();
             PreparedStatement s = c.prepareStatement(query)) {
            s.setLong(1, parameters.person1Id());
            s.setLong(2, parameters.person2Id());
            s.setLong(3, parameters.creationDate().getTime());
            s.executeUpdate();
            s.setLong(1, parameters.person2Id());
            s.setLong(2, parameters.person1Id());
            s.setLong(3, parameters.creationDate().getTime());
            s.executeUpdate();
            c.commit();
        }
    }

}
