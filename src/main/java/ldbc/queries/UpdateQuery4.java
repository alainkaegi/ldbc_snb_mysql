/*
 * Copyright © 2018 Alain Kägi
 */

package ldbc.queries;

import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate4AddForum;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * The UpdateQuery4 class defines the MySQL-based update query 4.
 */
public class UpdateQuery4 {

    /**
     * Add a forum.
     * @param db          A database handle
     * @param parameters  A forum's full description
     * @throws SQLException if a database access error occurs
     */
    public static void query(Connection db, LdbcUpdate4AddForum parameters) throws SQLException {

        try {
            db.setAutoCommit(false);

            PreparedStatement s;

            String addForumQuery =
                "   INSERT INTO Forum " +
                "        VALUES (?, " + // id
                "                ?, " + // title
                "                ?)";   // creationDate
            s = db.prepareStatement(addForumQuery);
            s.setLong(1, parameters.forumId());
            s.setString(2, parameters.forumTitle());
            s.setLong(3, parameters.creationDate().getTime());
            s.executeUpdate();

            String addModeratorLinkQuery =
                "   INSERT INTO ForumHasModeratorPerson " +
                "        VALUES (?, " + // forumId
                "                ?)";   // personId
            s = db.prepareStatement(addModeratorLinkQuery);
            s.setLong(1, parameters.forumId());
            s.setLong(2, parameters.moderatorPersonId());
            s.executeUpdate();

            String addTagLinkQuery =
                "   INSERT INTO ForumHasTagTag " +
                "        VALUES (?, " + // forumId
                "                ?)";   // tagId
            s = db.prepareStatement(addTagLinkQuery);
            s.setLong(1, parameters.forumId());
            for (long tagId : parameters.tagIds()) {
                s.setLong(2, tagId);
                s.executeUpdate();
            }

            s.close();

            db.commit();
        } finally {
            db.setAutoCommit(true);
        }

    }

}
