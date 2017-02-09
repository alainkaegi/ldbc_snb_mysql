/**
 * Complex read query 9.
 *
 * Copyright © 2017 Alain Kägi
 */

package ldbc.queries;

import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery9Result;

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

/** Ninth complex read query. */
public class Query9 implements ExecutableQuery {

    /* Static query parameters. */
    private static final String queryName = "Query9";
    private static final String queryParameterFilename = "query_9_param.txt";
    private static final String queryParameterFileLinePattern = "(\\d+)\\|(\\d+)";
    private static final int queryLimit = 20;

    /** A minimal constructor. */
    private Query9() {}

    /** A main entry point to run a microbenchmark. */
    public static void main(String[] args) {
        MicroBenchmark.executeQueryWithParametersFromFile(new Query9(), queryName, queryParameterFilename, queryParameterFileLinePattern);
    }

    /**
     * Recent messages by friends.
     * @param db        A database handle
     * @param personId  A person ID
     * @param date      A date in milliseconds since 1/1/1970 00:00:00 GMT
     * @param limit     An upper bound on the size of results returned
     * @return Recent posts by friends
     * @throw SQLException if a problem occurs during the query's execution
     */
    public static List<LdbcQuery9Result> query(Connection db,
        long personId,
        long date,
        int limit) throws SQLException {

        List<LdbcQuery9Result> result = new ArrayList<>();

        PreparedStatement statement = db.prepareStatement(
            "  SELECT Person.id, Person.firstName, Person.lastName, " +
            "         Message.id, Message.content, Message.imageFile, " +
            "         Message.creationDate " +
            "    FROM Person, Message, MessageHasCreatorPerson, " +
            "         (SELECT person2Id " +
            "            FROM PersonKnowsPerson " +
            "           WHERE person1Id = ? " +

            "           UNION " +

            "          SELECT k2.person2Id " +
            "            FROM PersonKnowsPerson as k1, PersonKnowsPerson as k2 " +
            "           WHERE k1.person1Id = ? " +
            "             AND k1.person2Id = k2.person1Id " +
            "             AND k2.person2Id <> ? " +
            "         ) as f " +
            "   WHERE f.person2Id = Person.id " +
            "     AND Person.id = MessageHasCreatorPerson.personId " +
            "     AND MessageHasCreatorPerson.messageId = Message.id " +
            "     AND Message.creationDate < ? " +
            "ORDER BY Message.creationDate DESC, Message.id " +
            "   LIMIT ?");
        statement.setLong(1, personId);
        statement.setLong(2, personId);
        statement.setLong(3, personId);
        statement.setLong(4, date);
        statement.setInt(5, limit);
        ResultSet resultSet = statement.executeQuery();
        while (resultSet.next()) {
            long creatorId = resultSet.getLong("Person.id");
            String creatorFirstName = resultSet.getString("Person.firstName");
            String creatorLastName = resultSet.getString("Person.lastName");
            long messageId = resultSet.getLong("Message.id");
            String messageContent = resultSet.getString("Message.content") + resultSet.getString("Message.imageFile");
            long messageCreationDate = resultSet.getLong("Message.creationDate");

            LdbcQuery9Result r = new LdbcQuery9Result(creatorId, creatorFirstName, creatorLastName, messageId, messageContent, messageCreationDate);
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
            long date = queryParameters.getLong();

            List<LdbcQuery9Result> r = query(db, personId, date, queryLimit);

            if (beVerbose)
                print(personId, new Date(date), r);
        }
    }

    /**
     * Pretty print the query results.
     * @param personId  Query 9 parameter 1
     * @param date      Query 9 parameter 2
     * @param results   Query 9 results
     */
    static void print(long personId, Date date, List<LdbcQuery9Result> results) {
        SimpleDateFormat dateTimeFmt = new SimpleDateFormat("EEE MMM dd HH:mm:ss yyyy");
        dateTimeFmt.setTimeZone(TimeZone.getTimeZone("UTC"));
        System.out.println(personId + " " + dateTimeFmt.format(date));

        if (results.size() == 0) {
            System.out.println("  no matches");
            System.out.println("");
            return;
        }

        SimpleDateFormat dateTimeZFmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");
        dateTimeZFmt.setTimeZone(TimeZone.getTimeZone("UTC"));

        for (LdbcQuery9Result result : results) {
            System.out.println("  " + result.personId()
                               + " " + result.personFirstName()
                               + " " + result.personLastName()
                               + " " + result.commentOrPostId()
                               + " " + dateTimeZFmt.format(new Date(result.commentOrPostCreationDate()))
                               + " " + result.commentOrPostContent());
        }

        System.out.println("");
    }

}
