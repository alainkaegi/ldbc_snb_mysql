/**
 * Complex read query 8.
 *
 * Copyright © 2017 Alain Kägi
 */

package ldbc.queries;

import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery8Result;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import java.text.SimpleDateFormat;

/** Eigth complex read query. */
public class Query8 implements ExecutableQuery {

    /* Static query parameters. */
    private static final String queryName = "Query8";
    private static final String queryParameterFilename = "query_8_param.txt";
    private static final String queryParameterFileLinePattern = "(\\d+)";
    private static final int queryLimit = 20;

    /** A minimal constructor. */
    private Query8() {}

    /** A main entry point to run a microbenchmark. */
    public static void main(String[] args) {
        MicroBenchmark.executeQueryWithParametersFromFile(new Query8(), queryName, queryParameterFilename, queryParameterFileLinePattern);
    }

    /**
     * Recent replies.
     * @param db        A database handle
     * @param personId  A person ID
     * @param limit     An upper bound on the size of results returned
     * @return The person's most recent replies
     * @throw SQLException if a problem occurs during the query's execution
     */
    public static List<LdbcQuery8Result> query(Connection db,
        long personId,
        int limit) throws SQLException {

        List<LdbcQuery8Result> result = new ArrayList<>();

        PreparedStatement statement = db.prepareStatement(
            "  SELECT Person.id, Person.firstName, Person.lastName, " +
            "         Message.creationDate, Message.id, Message.content " +
            "    FROM Person, Message, CommentReplyOfMessage, " +
            "         MessageHasCreatorPerson as MessageHasCreatorPerson, " +
            "         MessageHasCreatorPerson as CommentHasCreatorPerson " +
            "   WHERE ? = MessageHasCreatorPerson.personId " +
            "     AND MessageHasCreatorPerson.messageId = CommentReplyOfMessage.messageId " +
            "     AND CommentReplyOfMessage.commentId = CommentHasCreatorPerson.messageId " +
            "     AND CommentHasCreatorPerson.personId = Person.id " +
            "     AND CommentReplyOfMessage.commentId = Message.id " +
            "ORDER BY Message.creationDate DESC, Message.id " +
            "   LIMIT ?");
        statement.setLong(1, personId);
        statement.setInt(2, limit);
        ResultSet resultSet = statement.executeQuery();
        while (resultSet.next()) {
            long creatorId = resultSet.getLong("Person.id");
            String creatorFirstName = resultSet.getString("Person.firstName");
            String creatorLastName = resultSet.getString("Person.lastName");
            long messageCreationDate = resultSet.getLong("Message.creationDate");
            long messageId = resultSet.getLong("Message.id");
            String messageContent = resultSet.getString("Message.content");

            LdbcQuery8Result r = new LdbcQuery8Result(creatorId, creatorFirstName, creatorLastName, messageCreationDate, messageId, messageContent);
            result.add(r);
        }

        return result;
    }

    /**
     * Execute the query once for every query parameters.
     * @param db               A database handle
     * @param queryParameters  Stream of query input parameters
     * @param beVerbose        Print query outputs if true
     * @throw SQLException if a problem occurs during the query's execution
     */
    public void executeQuery(Connection db, QueryParameterFile queryParameters, boolean beVerbose) throws SQLException {
        while (queryParameters.nextLine()) {
            long personId = queryParameters.getLong();

            List<LdbcQuery8Result> r = query(db, personId, queryLimit);

            if (beVerbose)
                print(personId, r);
        }
    }

    /**
     * Pretty print the query results.
     * @param personId  Query 8 parameter 1
     * @param results   Query 8 results
     */
    static void print(long personId, List<LdbcQuery8Result> results) {
        System.out.println(personId);

        if (results.size() == 0) {
            System.out.println("  no matches");
            System.out.println("");
            return;
        }

        SimpleDateFormat dateTimeZFmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");
        dateTimeZFmt.setTimeZone(TimeZone.getTimeZone("UTC"));

        for (LdbcQuery8Result result : results) {
            System.out.println("  " + result.personId()
                               + ", " + result.personFirstName()
                               + " " + result.personLastName()
                               + ", " + dateTimeZFmt.format(new Date(result.commentCreationDate()))
                               + ", " + result.commentId()
                               + ", " + result.commentContent());
        }

        System.out.println("");
    }

}
