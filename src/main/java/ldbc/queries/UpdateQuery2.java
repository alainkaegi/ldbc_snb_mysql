/*
 * Copyright © 2018 Alain Kägi
 */

package ldbc.queries;

import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate2AddPostLike;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * The UpdateQuery2 class defines the MySQL-based update query 2.
 */
public class UpdateQuery2 {

    /**
     * Add a 'likes' edge from a person to a post.
     * @param db          A database handle
     * @param parameters  The parameters of this transaction
     * @throws SQLException if a database access error occurs
     */
    public static void query(Connection db, LdbcUpdate2AddPostLike parameters) throws SQLException {
        String query =
            "   INSERT INTO PersonLikesPost " +
            "        VALUES (?, " + // personId
            "                ?, " + // postId
            "                ?)";   // creationDate
        PreparedStatement s = db.prepareStatement(query);
        s.setLong(1, parameters.personId());
        s.setLong(2, parameters.postId());
        s.setLong(3, parameters.creationDate().getTime());
        s.executeUpdate();
        s.close();
    }

}
