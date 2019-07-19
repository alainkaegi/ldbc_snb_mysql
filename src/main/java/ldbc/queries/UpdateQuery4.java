/*
 * Copyright © 2018-2019 Alain Kägi
 */

package ldbc.queries;

import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate4AddForum;

import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * The UpdateQuery4 class defines the MySQL-based update query 4.
 */
public class UpdateQuery4 {

    /**
     * Add a forum.
     * @param ds          A data source
     * @param parameters  A forum's full description
     * @throws SQLException if a database access error occurs
     */
    public static void query(HikariDataSource ds, LdbcUpdate4AddForum parameters) throws SQLException {

        String addForumQuery =
            "   INSERT INTO Forum " +
            "        VALUES (?, " + // id
            "                ?, " + // title
            "                ?)";   // creationDate

        String addModeratorLinkQuery =
            "   INSERT INTO ForumHasModeratorPerson " +
            "        VALUES (?, " + // forumId
            "                ?)";   // personId

        String addTagLinkQuery =
            "   INSERT INTO ForumHasTagTag " +
            "        VALUES (?, " + // forumId
            "                ?)";   // tagId

        try (Connection c = ds.getConnection();
             PreparedStatement addForumStatement = c.prepareStatement(addForumQuery);
             PreparedStatement addModeratorLinktatement = c.prepareStatement(addModeratorLinkQuery);
             PreparedStatement addTagLinkStatement = c.prepareStatement(addTagLinkQuery)) {
            addForumStatement.setLong(1, parameters.forumId());
            addForumStatement.setString(2, parameters.forumTitle());
            addForumStatement.setLong(3, parameters.creationDate().getTime());
            addForumStatement.executeUpdate();

            addModeratorLinktatement.setLong(1, parameters.forumId());
            addModeratorLinktatement.setLong(2, parameters.moderatorPersonId());
            addModeratorLinktatement.executeUpdate();

            addTagLinkStatement.setLong(1, parameters.forumId());
            for (long tagId : parameters.tagIds()) {
                addTagLinkStatement.setLong(2, tagId);
                addTagLinkStatement.executeUpdate();
            }

            c.commit();
        }

    }

}
