/*
 * Copyright © 2018 Alain Kägi
 */

package ldbc.queries;

import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate7AddComment;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * The UpdateQuery7 class defines the MySQL-based update query 7.
 */
public class UpdateQuery7 {

    /**
     * Add a comment.
     * @param db          A database handle
     * @param parameters  A comment's full description
     * @throws SQLException if a database access error occurs
     */
    public static void query(Connection db, LdbcUpdate7AddComment parameters) throws SQLException {

        try {
            db.setAutoCommit(false);

            PreparedStatement s;

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
            s = db.prepareStatement(addCommentQuery);
            s.setLong(1, parameters.commentId());
            s.setLong(2, parameters.creationDate().getTime());
            s.setString(3, parameters.locationIp());
            s.setString(4, parameters.browserUsed());
            s.setString(5, parameters.content());
            s.setInt(6, parameters.length());
            s.executeUpdate();

            String addAuthorLinkQuery =
                "   INSERT INTO MessageHasCreatorPerson " +
                "        VALUES (?, " + // commentId
                "                ?)";   // personId
            s = db.prepareStatement(addAuthorLinkQuery);
            s.setLong(1, parameters.commentId());
            s.setLong(2, parameters.authorPersonId());
            s.executeUpdate();

            String addCountryLinkQuery =
                "   INSERT INTO CommentIsLocatedInPlace " +
                "        VALUES (?, " + // commentId
                "                ?)";   // countryId
            s = db.prepareStatement(addCountryLinkQuery);
            s.setLong(1, parameters.commentId());
            s.setLong(2, parameters.countryId());
            s.executeUpdate();

            String addReplyLinkQuery =
                "   INSERT INTO CommentReplyOfMessage " +
                "        VALUES (?, " + // commentId
                "                ?)";   // messageId
            long messageId = parameters.replyToPostId();
            if (messageId == -1)
                messageId = parameters.replyToCommentId();
            s = db.prepareStatement(addReplyLinkQuery);
            s.setLong(1, parameters.commentId());
            s.setLong(2, messageId);
            s.executeUpdate();

            String addTagLinkQuery =
                "   INSERT INTO CommentHasTagTag " +
                "        VALUES (?, " + // commentId
                "                ?)";   // tagId
            s = db.prepareStatement(addTagLinkQuery);
            s.setLong(1, parameters.commentId());
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
