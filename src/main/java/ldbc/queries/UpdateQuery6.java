/*
 * Copyright © 2018 Alain Kägi
 */

package ldbc.queries;

import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate6AddPost;

import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * The UpdateQuery6 class defines the MySQL-based update query 6.
 */
public class UpdateQuery6 {

    /**
     * Add a post.
     * @param ds          A data source
     * @param parameters  A post's full description
     * @throws SQLException if a database access error occurs
     */
    public static void query(HikariDataSource ds, LdbcUpdate6AddPost parameters) throws SQLException {

        String addPostQuery =
            "   INSERT INTO Message " +
            "        VALUES (?, " + // commentId
            "                ?, " + // imageFile
            "                ?, " + // creationDate
            "                ?, " + // locationIP
            "                ?, " + // browserUsed
            "                ?, " + // language
            "                ?, " + // content
            "                ?)";   // length

        String addAuthorLinkQuery =
            "   INSERT INTO MessageHasCreatorPerson " +
            "        VALUES (?, " + // postId
            "                ?)";   // personId

        String addForumLinkQuery =
            "   INSERT INTO ForumContainerOfPost " +
            "        VALUES (?, " + // forumId
            "                ?)";   // postId

        String addCountryLinkQuery =
            "   INSERT INTO PostIsLocatedInPlace " +
            "        VALUES (?, " + // postId
            "                ?)";   // countryId

        String addTagLinkQuery =
            "   INSERT INTO PostHasTagTag " +
            "        VALUES (?, " + // postId
            "                ?)";   // tagId

        try (Connection c = ds.getConnection();
             PreparedStatement addPostStatement = c.prepareStatement(addPostQuery);
             PreparedStatement addAuthorLinkStatement = c.prepareStatement(addAuthorLinkQuery);
             PreparedStatement addForumLinkStatement = c.prepareStatement(addForumLinkQuery);
             PreparedStatement addCountryLinkStatement = c.prepareStatement(addCountryLinkQuery);
             PreparedStatement addTagLinkStatement = c.prepareStatement(addTagLinkQuery)) {
            addPostStatement.setLong(1, parameters.postId());
            addPostStatement.setString(2, parameters.imageFile());
            addPostStatement.setLong(3, parameters.creationDate().getTime());
            addPostStatement.setString(4, parameters.locationIp());
            addPostStatement.setString(5, parameters.browserUsed());
            addPostStatement.setString(6, parameters.language());
            addPostStatement.setString(7, parameters.content());
            addPostStatement.setInt(8, parameters.length());
            addPostStatement.executeUpdate();

            addAuthorLinkStatement.setLong(1, parameters.postId());
            addAuthorLinkStatement.setLong(2, parameters.authorPersonId());
            addAuthorLinkStatement.executeUpdate();

            addForumLinkStatement.setLong(1, parameters.forumId());
            addForumLinkStatement.setLong(2, parameters.postId());
            addForumLinkStatement.executeUpdate();

            addCountryLinkStatement.setLong(1, parameters.postId());
            addCountryLinkStatement.setLong(2, parameters.countryId());
            addCountryLinkStatement.executeUpdate();

            addTagLinkStatement.setLong(1, parameters.postId());
            for (long tagId : parameters.tagIds()) {
                addTagLinkStatement.setLong(2, tagId);
                addTagLinkStatement.executeUpdate();
            }

            c.commit();
        }

    }

}
