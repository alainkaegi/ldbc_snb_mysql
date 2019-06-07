/*
 * Copyright © 2018, 2019 Alain Kägi
 */

package ldbc.queries;

import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery7MessageRepliesResult;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import ldbc.utils.LdbcUtils;

/**
 * The ShortQuery7 class defines the MySQL-based simple read query 7.
 */
public class ShortQuery7 {

    /**
     * Get a message's replies (seventh short read query).
     * @param db         A database handle
     * @param messageId  A message's unique identifier
     * @return the message's replies messages
     * @throws SQLException if a database access error occurs
     */
    public static List<LdbcShortQuery7MessageRepliesResult> query(Connection db, long messageId) throws SQLException {
        List<LdbcShortQuery7MessageRepliesResult> results = new ArrayList<>();

        String knowsQuery =
            "   SELECT PersonKnowsPerson.person2id " +
            "     FROM PersonKnowsPerson, " +
            "          MessageHasCreatorPerson " +
            "    WHERE MessageHasCreatorPerson.messageId = ?" +
            "      AND PersonKnowsPerson.person1Id = MessageHasCreatorPerson.personId";

        String mainQuery =
            "   SELECT Comment.id, " +
            "          Comment.content, " +
            "          Comment.imageFile, " +
            "          Comment.creationDate, " +
            "          Person.id, " +
            "          Person.firstName, " +
            "          Person.lastName " +
            "     FROM CommentReplyOfMessage, " +
            "          MessageHasCreatorPerson AS CommentHasCreatorPerson, " +
            "          Message AS Comment, " +
            "          Person " +
            "    WHERE CommentReplyOfMessage.messageId = ? " +
            "      AND CommentHasCreatorPerson.messageId = CommentReplyOfMessage.commentId " +
            "      AND Person.id = CommentHasCreatorPerson.personId " +
            "      AND Comment.id = CommentReplyOfMessage.commentId " +
            " ORDER BY Comment.creationDate DESC, " +
            "          Person.id";

        PreparedStatement s1 = null;
        PreparedStatement s = null;
        ResultSet r1 = null;
        ResultSet r = null;

        try {
            db.setAutoCommit(false);

            // The friends of the message's author.
            Set<Long> friendsOfAuthor = new HashSet<>();
            s1 = db.prepareStatement(knowsQuery);
            s1.setLong(1, messageId);
            r1 = s1.executeQuery();
            while (r1.next()) {
                friendsOfAuthor.add(r1.getLong("PersonKnowsPerson.person2id"));
            }

            s = db.prepareStatement(mainQuery);
            s.setLong(1, messageId);
            r = s.executeQuery();
            while (r.next()) {
                long replierId = r.getLong("Person.id");
                LdbcShortQuery7MessageRepliesResult result = new LdbcShortQuery7MessageRepliesResult(
                    r.getLong("Comment.id"),

                    // One or the other field must be empty.
                    r.getString("Comment.imageFile") + r.getString("Comment.content"),

                    r.getLong("Comment.creationDate"),
                    replierId,
                    r.getString("Person.firstName"),
                    r.getString("Person.lastName"),
                    friendsOfAuthor.contains(replierId));
                results.add(result);
            }

            db.commit();
        } finally {
            db.setAutoCommit(true);
            if (r1 != null) { r1.close(); }
            if (s1 != null) { s1.close(); }
            if (r != null) { r.close(); }
            if (s != null) { s.close(); }
        }

        return results;
    }

}
