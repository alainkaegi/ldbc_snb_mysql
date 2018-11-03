/*
 * Copyright © 2018 Alain Kägi
 */

package ldbc.queries;

import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate5AddForumMembership;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * The UpdateQuery5 class defines the MySQL-based update query 5.
 */
public class UpdateQuery5 {

    /**
     * Add an edge from a forum to a person..
     * @param db          A database handle
     * @param parameters  The parameters of this transaction
     * @throws SQLException if a database access error occurs
     */
    public static void query(Connection db, LdbcUpdate5AddForumMembership parameters) throws SQLException {
        String query =
            "   INSERT INTO ForumHasMemberPerson " +
            "        VALUES (?, " + // forumId
            "                ?, " + // personId
            "                ?)";   // joinDate
        PreparedStatement s = db.prepareStatement(query);
        s.setLong(1, parameters.forumId());
        s.setLong(2, parameters.personId());
        s.setLong(3, parameters.joinDate().getTime());
        s.executeUpdate();
        s.close();
    }

}
