/*
 * Copyright © 2018-2019 Alain Kägi
 */

package ldbc.queries;

import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate2AddPostLike;

import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * The UpdateQuery2 class defines the MySQL-based update query 2.
 */
public class UpdateQuery2 {

    /**
     * Add a 'likes' edge from a person to a post.
     * @param ds          A data source
     * @param parameters  The parameters of this transaction
     * @throws SQLException if a database access error occurs
     */
    public static void query(HikariDataSource ds, LdbcUpdate2AddPostLike parameters) throws SQLException {
        String query =
            "   INSERT INTO PersonLikesPost " +
            "        VALUES (?, " + // personId
            "                ?, " + // postId
            "                ?)";   // creationDate
        try (Connection c = ds.getConnection();
             PreparedStatement s = c.prepareStatement(query)) {
            s.setLong(1, parameters.personId());
            s.setLong(2, parameters.postId());
            s.setLong(3, parameters.creationDate().getTime());
            s.executeUpdate();
            c.commit();
        }
    }

}
