/*
 * Copyright © 2018-2019 Alain Kägi
 */

package ldbc.queries;

import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate3AddCommentLike;

import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * The UpdateQuery3 class defines the MySQL-based update query 3.
 */
public class UpdateQuery3 {

    /**
     * Add a 'likes' edge from a person to a comment.
     * @param ds          A data source
     * @param parameters  The parameters of this transaction
     * @throws SQLException if a database access error occurs
     */
    public static void query(HikariDataSource ds, LdbcUpdate3AddCommentLike parameters) throws SQLException {
        String query =
            "   INSERT INTO PersonLikesComment " +
            "        VALUES (?, " + // personId
            "                ?, " + // commentId
            "                ?)";   // creationDate
        try (Connection c = ds.getConnection();
             PreparedStatement s = c.prepareStatement(query)) {
            s.setLong(1, parameters.personId());
            s.setLong(2, parameters.commentId());
            s.setLong(3, parameters.creationDate().getTime());
            s.executeUpdate();
            c.commit();
        }
    }
}
