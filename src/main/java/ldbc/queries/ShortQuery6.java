/*
 * Copyright © 2018 Alain Kägi
 */

package ldbc.queries;

import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery6MessageForumResult;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import ldbc.utils.LdbcUtils;

/**
 * The ShortQuery6 class defines the MySQL-based simple read query 6.
 */
public class ShortQuery6 {

    /**
     * Get a message's forum (sixth short read query).
     * @param db         A database handle
     * @param messageId  A message's unique identifier
     * @return the message's forum
     * @throws SQLException if a database access error occurs
     */
    public static LdbcShortQuery6MessageForumResult query(Connection db, long messageId) throws SQLException {
        LdbcShortQuery6MessageForumResult result = null;

        try {
            long parentPostId = LdbcUtils.getParentPostId(db, messageId);

            String query =
                "   SELECT Forum.id, " +
                "          Forum.title, " +
                "          Person.id, " +
                "          Person.firstName, " +
                "          Person.lastName " +
                "     FROM ForumContainerOfPost, " +
                "          ForumHasModeratorPerson, " +
                "          Forum, " +
                "          Person " +
                "    WHERE ForumContainerOfPost.postId = ? " +
                "      AND Forum.id = ForumContainerOfPost.forumId " +
                "      AND ForumHasModeratorPerson.forumId = ForumContainerOfPost.forumId " +
                "      AND Person.id = ForumHasModeratorPerson.personId";

            PreparedStatement s = db.prepareStatement(query);
            s.setLong(1, parentPostId);
            ResultSet r = s.executeQuery();
            if (r.next())
                result = new LdbcShortQuery6MessageForumResult(
                    r.getLong("Forum.id"),
                    r.getString("Forum.title"),
                    r.getLong("Person.id"),
                    r.getString("Person.firstName"),
                    r.getString("Person.lastName"));
            r.close();
            s.close();
        } finally {
            db.setAutoCommit(true);
        }

        return result;
    }

}
