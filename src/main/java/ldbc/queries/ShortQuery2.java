/*
 * Copyright © 2018 Alain Kägi
 */

package ldbc.queries;

import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery2PersonPostsResult;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import java.util.List;
import java.util.ArrayList;

import ldbc.utils.LdbcUtils;

/**
 * The ShortQuery2 class defines the MySQL-based simple read query 2.
 */
public class ShortQuery2 {

    /**
     * Get a person's recent messages (second short read query).
     * @param db        A database handle
     * @param personId  A person's unique identifier
     * @param limit     An upper bound on the size of results returned
     * @return the person's recent messages
     * @throws SQLException if a database access error occurs
     */
    public static List<LdbcShortQuery2PersonPostsResult> query(Connection db, long personId, int limit) throws SQLException {
        List<LdbcShortQuery2PersonPostsResult> results = new ArrayList<>();

        String query =
            "   SELECT Message.id, " +
            "          Message.imageFile, " +
            "          Message.creationDate, " +
            "          Message.content " +
            "     FROM MessageHasCreatorPerson, " +
            "          Message " +
            "    WHERE MessageHasCreatorPerson.personId = ? " +
            "      AND Message.id = MessageHasCreatorPerson.messageId " +
            " ORDER BY Message.creationDate DESC, " +
            "          Message.id DESC";
        try {
            int count = 0;
            PreparedStatement s = db.prepareStatement(query);
            s.setLong(1, personId);
            ResultSet r = s.executeQuery();
            while (r.next() && count++ < limit) {
                long messageId = r.getLong("Message.id");
                long parentPostId = LdbcUtils.getParentPostId(db, messageId);
                long parentPostAuthorId = LdbcUtils.getAuthorOf(db, parentPostId);
                LdbcShortQuery2PersonPostsResult result = new LdbcShortQuery2PersonPostsResult(
                    messageId,

                    // One or the other field must be empty.
                    r.getString("Message.imageFile") + r.getString("Message.content"),

                    r.getLong("Message.creationDate"),
                    parentPostId,
                    parentPostAuthorId,
                    LdbcUtils.getFirstName(db, parentPostAuthorId),
                    LdbcUtils.getLastName(db, parentPostAuthorId));
                results.add(result);
            }
            r.close();
            s.close();
        } finally {
            db.setAutoCommit(true);
        }

        return results;
    }

}
