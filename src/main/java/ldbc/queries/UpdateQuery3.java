/*
 * Copyright © 2018 Alain Kägi
 */

package ldbc.queries;

import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate3AddCommentLike;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * The UpdateQuery3 class defines the MySQL-based update query 3.
 */
public class UpdateQuery3 {

    /**
     * Add a 'likes' edge from a person to a comment.
     * @param db          A database handle
     * @param parameters  The parameters of this transaction
     * @throws SQLException if a database access error occurs
     */
    public static void query(Connection db, LdbcUpdate3AddCommentLike parameters) throws SQLException {
        String query =
            "   INSERT INTO PersonLikesComment " +
            "        VALUES (?, " + // personId
            "                ?, " + // commentId
            "                ?)";   // creationDate
        PreparedStatement s = db.prepareStatement(query);
        s.setLong(1, parameters.personId());
        s.setLong(2, parameters.commentId());
        s.setLong(3, parameters.creationDate().getTime());
        s.executeUpdate();
        s.close();
    }
}
