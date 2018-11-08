/*
 * Copyright © 2018 Alain Kägi
 */

package ldbc.queries;

import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery1PersonProfileResult;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * The ShortQuery1 class defines the MySQL-based simple read query 1.
 */
public class ShortQuery1 {

    /**
     * Get a person's profile (first short read query).
     * @param db        A database handle
     * @param personId  A person's unique identifier
     * @return the person's profile
     * @throws SQLException if a database access error occurs
     */
    public static LdbcShortQuery1PersonProfileResult query(Connection db, long personId) throws SQLException {
        LdbcShortQuery1PersonProfileResult result = null;

        String query =
            "   SELECT Person.firstName, " +
            "          Person.lastName, " +
            "          Person.gender, " +
            "          Person.birthday, " +
            "          Person.creationDate, " +
            "          Person.locationIP, " +
            "          Person.browserUsed, " +
            "          PersonIsLocatedInPlace.placeId " +
            "     FROM Person, " +
            "          PersonIsLocatedInPlace " +
            "    WHERE Person.id = ? " +
            "      AND PersonIsLocatedInPlace.personId = ?";
        PreparedStatement s = db.prepareStatement(query);
        s.setLong(1, personId);
        s.setLong(2, personId);
        ResultSet r = s.executeQuery();
        if (r.next())
            result = new LdbcShortQuery1PersonProfileResult(
                r.getString("Person.firstName"),
                r.getString("Person.lastName"),
                r.getLong("Person.birthday"),
                r.getString("Person.locationIP"),
                r.getString("Person.browserUsed"),
                r.getLong("PersonIsLocatedInPlace.placeId"),
                r.getString("Person.gender"),
                r.getLong("Person.creationDate"));

        r.close();
        s.close();

        return result;
    }

}
