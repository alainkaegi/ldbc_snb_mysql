/*
 * Copyright © 2018 Alain Kägi
 */

package ldbc.queries;

import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery3PersonFriendsResult;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import java.util.List;
import java.util.ArrayList;

import ldbc.utils.LdbcUtils;

/**
 * The ShortQuery3 class defines the MySQL-based simple read query 3.
 */
public class ShortQuery3 {

    /**
     * Get a person's recent messages (third short read query).
     * @param db        A database handle
     * @param personId  A person's unique identifier
     * @return the person's recent messages
     * @throws SQLException if a database access error occurs
     */
    public static List<LdbcShortQuery3PersonFriendsResult> query(Connection db, long personId) throws SQLException {
        List<LdbcShortQuery3PersonFriendsResult> results = new ArrayList<>();

        String query =
            "   SELECT Person.id, " +
            "          Person.firstName, " +
            "          Person.lastName, " +
            "          PersonKnowsPerson.creationDate " +
            "     FROM PersonKnowsPerson, " +
            "          Person " +
            "    WHERE PersonKnowsPerson.person1Id = ? " +
            "      AND Person.id = PersonKnowsPerson.person2Id " +
            " ORDER BY PersonKnowsPerson.creationDate DESC, " +
            "          Person.id";
        PreparedStatement s = db.prepareStatement(query);
        s.setLong(1, personId);
        ResultSet r = s.executeQuery();
        while (r.next()) {
            LdbcShortQuery3PersonFriendsResult result = new LdbcShortQuery3PersonFriendsResult(
                r.getLong("Person.id"),
                r.getString("Person.firstName"),
                r.getString("Person.lastName"),
                r.getLong("PersonKnowsPerson.creationDate"));
            results.add(result);
        }
        r.close();
        s.close();

        return results;
    }

}
