/*
 * Copyright © 2018 Alain Kägi
 */

package ldbc.queries;

import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate6AddPost;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * The UpdateQuery6 class defines the MySQL-based update query 6.
 */
public class UpdateQuery6 {

    /**
     * Add a post.
     * @param db          A database handle
     * @param parameters  A post's full description
     * @throws SQLException if a database access error occurs
     */
    public static void query(Connection db, LdbcUpdate6AddPost parameters) throws SQLException {

        try {
            db.setAutoCommit(false);

            PreparedStatement s;

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
            s = db.prepareStatement(addPostQuery);
            s.setLong(1, parameters.postId());
            s.setString(2, parameters.imageFile());
            s.setLong(3, parameters.creationDate().getTime());
            s.setString(4, parameters.locationIp());
            s.setString(5, parameters.browserUsed());
            s.setString(6, parameters.language());
            s.setString(7, parameters.content());
            s.setInt(8, parameters.length());
            s.executeUpdate();

            String addAuthorLinkQuery =
                "   INSERT INTO MessageHasCreatorPerson " +
                "        VALUES (?, " + // postId
                "                ?)";   // personId
            s = db.prepareStatement(addAuthorLinkQuery);
            s.setLong(1, parameters.postId());
            s.setLong(2, parameters.authorPersonId());
            s.executeUpdate();

            String addForumLinkQuery =
                "   INSERT INTO ForumContainerOfPost " +
                "        VALUES (?, " + // forumId
                "                ?)";   // postId
            s = db.prepareStatement(addForumLinkQuery);
            s.setLong(1, parameters.forumId());
            s.setLong(2, parameters.postId());
            s.executeUpdate();

            String addCountryLinkQuery =
                "   INSERT INTO PostIsLocatedInPlace " +
                "        VALUES (?, " + // postId
                "                ?)";   // countryId
            s = db.prepareStatement(addCountryLinkQuery);
            s.setLong(1, parameters.postId());
            s.setLong(2, parameters.countryId());
            s.executeUpdate();

            String addTagLinkQuery =
                "   INSERT INTO PostHasTagTag " +
                "        VALUES (?, " + // postId
                "                ?)";   // tagId
            s = db.prepareStatement(addTagLinkQuery);
            s.setLong(1, parameters.postId());
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
