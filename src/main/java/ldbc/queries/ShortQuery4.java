/*
 * Copyright © 2018 Alain Kägi
 */

package ldbc.queries;

import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery4MessageContentResult;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import ldbc.utils.LdbcUtils;

/**
 * The ShortQuery4 class defines the MySQL-based simple read query 4.
 */
public class ShortQuery4 {

    /**
     * Get a message's content (fourth short read query).
     * @param db         A database handle
     * @param messageId  A message's unique identifier
     * @return the message's content
     * @throws SQLException if a database access error occurs
     */
    public static LdbcShortQuery4MessageContentResult query(Connection db, long messageId) throws SQLException {
        LdbcShortQuery4MessageContentResult result = null;

        String query =
            "   SELECT Message.imageFile, " +
            "          Message.creationDate, " +
            "          Message.content " +
            "     FROM Message " +
            "    WHERE Message.id = ?";
        PreparedStatement s = db.prepareStatement(query);
        s.setLong(1, messageId);
        ResultSet r = s.executeQuery();
        if (r.next())
            result = new LdbcShortQuery4MessageContentResult(

                // One or the other field must be empty.
                r.getString("Message.imageFile") + r.getString("Message.content"),

                r.getLong("Message.creationDate"));
        r.close();
        s.close();

        return result;
    }

}
