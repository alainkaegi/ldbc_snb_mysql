/*
 * Copyright © 2018 Alain Kägi
 */

package ldbc.queries;

import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery5MessageCreatorResult;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import ldbc.utils.LdbcUtils;

/**
 * The ShortQuery5 class defines the MySQL-based simple read query 5.
 */
public class ShortQuery5 {

    /**
     * Get the message's creator (fifth short read query).
     * @param db         A database handle
     * @param messageId  A message's unique identifier
     * @return the message's creator
     * @throws SQLException if a database access error occurs
     */
    public static LdbcShortQuery5MessageCreatorResult query(Connection db, long messageId) throws SQLException {
        LdbcShortQuery5MessageCreatorResult result = null;

        String query =
            "   SELECT Person.id, " +
            "          Person.firstName, " +
            "          Person.lastName " +
            "     FROM MessageHasCreatorPerson, " +
            "          Person " +
            "    WHERE MessageHasCreatorPerson.messageId = ? " +
            "      AND Person.id = MessageHasCreatorPerson.personId";
        PreparedStatement s = db.prepareStatement(query);
        s.setLong(1, messageId);
        ResultSet r = s.executeQuery();
        if (r.next())
            result = new LdbcShortQuery5MessageCreatorResult(
                r.getLong("Person.id"),
                r.getString("Person.firstName"),
                r.getString("Person.lastName"));
        r.close();
        s.close();

        return result;
    }

}
