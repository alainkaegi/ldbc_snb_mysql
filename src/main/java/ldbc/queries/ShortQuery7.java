/*
 * Copyright © 2018-2019 Alain Kägi
 */

package ldbc.queries;

import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery7MessageRepliesResult;

import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import java.util.List;
import java.util.ArrayList;

import ldbc.utils.LdbcUtils;

/**
 * The ShortQuery7 class defines the MySQL-based simple read query 7.
 */
public class ShortQuery7 {

    /**
     * Get a message's replies (seventh short read query).
     * @param ds         A data source
     * @param messageId  A message's unique identifier
     * @return the message's replies messages
     * @throws SQLException if a database access error occurs
     */
    public static List<LdbcShortQuery7MessageRepliesResult> query(HikariDataSource ds, long messageId) throws SQLException {
        List<LdbcShortQuery7MessageRepliesResult> results = new ArrayList<>();

        String query =
            "   SELECT Comment.id, " +
            "          Comment.content, " +
            "          Comment.imageFile, " +
            "          Comment.creationDate, " +
            "          Person.id, " +
            "          Person.firstName, " +
            "          Person.lastName, " +
            "          CASE WHEN EXISTS (SELECT PersonKnowsPerson.person2Id " +
            "                              FROM PersonKnowsPerson " +
            "                             WHERE PersonKnowsPerson.person1Id = Person.id " +
            "                               AND PersonKnowsPerson.person2Id = MessageHasCreatorPerson.personId) " +
            "               THEN 1 " + // true
            "               ELSE 0 " + // false
            "          END AS areTheyFriend " +
            "     FROM CommentReplyOfMessage, " +
            "          MessageHasCreatorPerson, " +
            "          MessageHasCreatorPerson AS CommentHasCreatorPerson, " +
            "          Message AS Comment, " +
            "          Person " +
            "    WHERE CommentReplyOfMessage.messageId = ? " +
            "      AND CommentHasCreatorPerson.messageId = CommentReplyOfMessage.commentId " +
            "      AND Person.id = CommentHasCreatorPerson.personId " +
            "      AND Comment.id = CommentReplyOfMessage.commentId " +
            "      AND MessageHasCreatorPerson.messageId = ? " +
            " ORDER BY Comment.creationDate DESC, " +
            "          Person.id";
        ResultSet r = null;
        try (Connection c = ds.getConnection();
             PreparedStatement s = c.prepareStatement(query)) {
            s.setLong(1, messageId);
            s.setLong(2, messageId);
            r = s.executeQuery();
            while (r.next()) {
                LdbcShortQuery7MessageRepliesResult result = new LdbcShortQuery7MessageRepliesResult(
                    r.getLong("Comment.id"),

                    // One or the other field must be empty.
                    r.getString("Comment.imageFile") + r.getString("Comment.content"),

                    r.getLong("Comment.creationDate"),
                    r.getLong("Person.id"),
                    r.getString("Person.firstName"),
                    r.getString("Person.lastName"),
                    r.getBoolean("areTheyFriend"));
                results.add(result);
            }
            c.commit();
        }
        finally {
            if (r != null) r.close();
        }

        return results;
    }

}
