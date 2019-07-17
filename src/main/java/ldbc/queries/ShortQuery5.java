/*
 * Copyright © 2018 Alain Kägi
 */

package ldbc.queries;

import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery5MessageCreatorResult;

import com.zaxxer.hikari.HikariDataSource;

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
     * @param ds         A data source
     * @param messageId  A message's unique identifier
     * @return the message's creator
     * @throws SQLException if a database access error occurs
     */
    public static LdbcShortQuery5MessageCreatorResult query(HikariDataSource ds, long messageId) throws SQLException {
        LdbcShortQuery5MessageCreatorResult result = null;

        String query =
            "   SELECT Person.id, " +
            "          Person.firstName, " +
            "          Person.lastName " +
            "     FROM MessageHasCreatorPerson, " +
            "          Person " +
            "    WHERE MessageHasCreatorPerson.messageId = ? " +
            "      AND Person.id = MessageHasCreatorPerson.personId";
        ResultSet r = null;
        try (Connection c = ds.getConnection();
             PreparedStatement s = c.prepareStatement(query)) {
            s.setLong(1, messageId);
            r = s.executeQuery();
            if (r.next())
                result = new LdbcShortQuery5MessageCreatorResult(
                    r.getLong("Person.id"),
                    r.getString("Person.firstName"),
                    r.getString("Person.lastName"));
            c.commit();
        }
        finally {
            if (r != null) r.close();
        }

        return result;
    }

}
