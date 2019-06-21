/*
 * Copyright © 2018 Alain Kägi
 */

package ldbc.queries;

import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate7AddComment;

import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * The UpdateQuery7 class defines the MySQL-based update query 7.
 */
public class UpdateQuery7 {

    /**
     * Add a comment.
     * @param ds          A data source
     * @param parameters  A comment's full description
     * @throws SQLException if a database access error occurs
     */
    public static void query(HikariDataSource ds, LdbcUpdate7AddComment parameters) throws SQLException {

        String addCommentQuery =
            "   INSERT INTO Message " +
            "        VALUES (?, " +  // commentId
            "                '', " + // no imageFile
            "                ?, " +  // creationDate
            "                ?, " +  // locationIP
            "                ?, " +  // browserUsed
            "                '', " + // no language
            "                ?, " +  // content
            "                ?)";    // length

        String addAuthorLinkQuery =
            "   INSERT INTO MessageHasCreatorPerson " +
            "        VALUES (?, " + // commentId
            "                ?)";   // personId

        String addCountryLinkQuery =
            "   INSERT INTO CommentIsLocatedInPlace " +
            "        VALUES (?, " + // commentId
            "                ?)";   // countryId

        String addReplyLinkQuery =
            "   INSERT INTO CommentReplyOfMessage " +
            "        VALUES (?, " + // commentId
            "                ?)";   // messageId

        String addTagLinkQuery =
            "   INSERT INTO CommentHasTagTag " +
            "        VALUES (?, " + // commentId
            "                ?)";   // tagId

        try (Connection c = ds.getConnection();
             PreparedStatement addCommentStatement = c.prepareStatement(addCommentQuery);
             PreparedStatement addAuthorLinkStatement = c.prepareStatement(addAuthorLinkQuery);
             PreparedStatement addCountryLinkStatement = c.prepareStatement(addCountryLinkQuery);
             PreparedStatement addReplyLinkStatement = c.prepareStatement(addReplyLinkQuery);
             PreparedStatement addTagLinkStatement = c.prepareStatement(addTagLinkQuery)) {
            addCommentStatement.setLong(1, parameters.commentId());
            addCommentStatement.setLong(2, parameters.creationDate().getTime());
            addCommentStatement.setString(3, parameters.locationIp());
            addCommentStatement.setString(4, parameters.browserUsed());
            addCommentStatement.setString(5, parameters.content());
            addCommentStatement.setInt(6, parameters.length());
            addCommentStatement.executeUpdate();

            addAuthorLinkStatement.setLong(1, parameters.commentId());
            addAuthorLinkStatement.setLong(2, parameters.authorPersonId());
            addAuthorLinkStatement.executeUpdate();

            addCountryLinkStatement.setLong(1, parameters.commentId());
            addCountryLinkStatement.setLong(2, parameters.countryId());
            addCountryLinkStatement.executeUpdate();

            long messageId = parameters.replyToPostId();
            if (messageId == -1)
                messageId = parameters.replyToCommentId();
            addReplyLinkStatement.setLong(1, parameters.commentId());
            addReplyLinkStatement.setLong(2, messageId);
            addReplyLinkStatement.executeUpdate();

            addTagLinkStatement.setLong(1, parameters.commentId());
            for (long tagId : parameters.tagIds()) {
                addTagLinkStatement.setLong(2, tagId);
                addTagLinkStatement.executeUpdate();
            }

            c.commit();
        }

    }

}
