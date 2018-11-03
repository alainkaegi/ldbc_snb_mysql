/*
 * Copyright © 2018 Alain Kägi
 */

package ldbc.queries;

import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate8AddFriendship;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * The UpdateQuery8 class defines the MySQL-based update query 8.
 */
public class UpdateQuery8 {

    /**
     * Add a friendship.
     * @param db          A database handle
     * @param parameters  The parameters of this transaction
     * @throws SQLException if a database access error occurs
     */
    public static void query(Connection db, LdbcUpdate8AddFriendship parameters) throws SQLException {
        try {
            db.setAutoCommit(false);

            String query =
                "   INSERT INTO PersonKnowsPerson " +
                "        VALUES (?, " + // person1Id
                "                ?, " + // person2Id
                "                ?)";   // creationDate
            PreparedStatement s = db.prepareStatement(query);
            s.setLong(1, parameters.person1Id());
            s.setLong(2, parameters.person2Id());
            s.setLong(3, parameters.creationDate().getTime());
            s.executeUpdate();
            s.setLong(1, parameters.person2Id());
            s.setLong(2, parameters.person1Id());
            s.setLong(3, parameters.creationDate().getTime());
            s.executeUpdate();
            s.close();
            db.commit();
        } finally {
            db.setAutoCommit(true);
        }
    }

}
