/**
 * Complex read query 2.
 *
 * Copyright © 2017 Alain Kägi
 */

package ldbc.queries;

import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery2Result;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import java.text.SimpleDateFormat;

public class Query2 implements ExecutableQuery {

    /* Static query parameters. */
    private static final String queryName = "Query2";
    private static final String queryParameterFilename = "query_2_param.txt";
    private static final String queryParameterFileLinePattern = "(\\d+)\\|(\\d+)";

    /** A minimal constructor. */
    private Query2() {}

    /** A main entry point to run a microbenchmark. */
    public static void main(String[] args) {
        MicroBenchmark.executeQueriesWithParametersFromFile(new Query2(), queryName, queryParameterFilename, queryParameterFileLinePattern);
    }

    /**
     * Recent messages by your friends (second complex read query).
     * @param db        A database handle
     * @param personId  A person ID
     * @param date      A date in milliseconds since 1/1/1970 00:00:00 GMT
     * @param limit     An upper bound on the size of results returned
     * @return the top 'limit' recent messages posted the person's friends
     */
    public static List<LdbcQuery2Result> query(Connection db,
        long personId,
        long date,
        int limit) throws SQLException {

        List<LdbcQuery2Result> result = new ArrayList<>();

        PreparedStatement statement = db.prepareStatement(
            "  SELECT Person.id, Person.firstName, Person.lastName, " +
            "         Message.id, Message.content, Message.imageFile, " +
            "         Message.creationDate " +
            "    FROM Person, PersonKnowsPerson, " +
            "         Message, MessageHasCreatorPerson " +
            "   WHERE ? = PersonKnowsPerson.person1Id " +
            "     AND PersonKnowsPerson.person2Id = MessageHasCreatorPerson.personId " +
            "     AND MessageHasCreatorPerson.messageId = Message.id " +
            "     AND Message.creationDate <= ? " +
            "     AND PersonKnowsPerson.person2Id = Person.id " +
            "ORDER BY Message.creationDate DESC, Message.id " +
            "   LIMIT ?");
        statement.setLong(1, personId);
        statement.setLong(2, date);
        statement.setInt(3, limit);
        ResultSet resultSet = statement.executeQuery();
        while (resultSet.next()) {
            long friendId = resultSet.getLong("Person.id");
            String friendFirstName = resultSet.getString("Person.firstName");
            String friendLastName = resultSet.getString("Person.lastName");
            long messageId = resultSet.getLong("Message.id");
            String messageContent = resultSet.getString("Message.content") + resultSet.getString("Message.imageFile"); // One or the other must be empty
            long messageCreationDate = resultSet.getLong("Message.creationDate");

            LdbcQuery2Result r = new LdbcQuery2Result(friendId, friendFirstName, friendLastName, messageId, messageContent, messageCreationDate);
            result.add(r);
        }

        return result;
    }

    /**
     * Execute query 2 for microbenchmarking.
     * @param db               A database handle
     * @param queryParameters  Stream of query input parameters
     * @param beVerbose        Print query outputs if true
     */
    public void executeQueries(Connection db, QueryParameterFile queryParameters, boolean beVerbose) throws SQLException {
        while (queryParameters.nextLine()) {
            long personId = queryParameters.getLong();
            long date = queryParameters.getLong();

            List<LdbcQuery2Result> r = query(db, personId, date, 20);

            if (beVerbose)
                print(personId, new Date(date), r);
       }
    }

    /**
     * Pretty print the query 2 results.
     * @param personId  Query 2 parameter 1
     * @param date      Query 2 parameter 2
     * @param results   Query 2 results
     */
     static void print(long personId, Date date, List<LdbcQuery2Result> results) {
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

        for (LdbcQuery2Result result : results) {
            System.out.println("  " + result.personId()
                               + " " + result.personFirstName()
                               + " " + result.personLastName()
                               + " " + result.postOrCommentId()
                               + " " + dateTimeZFmt.format(new Date(result.postOrCommentCreationDate()))
                               + " " + result.postOrCommentContent());
        }
        System.out.println("");
    }

}
