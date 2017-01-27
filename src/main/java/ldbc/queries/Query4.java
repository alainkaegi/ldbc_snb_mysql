/**
 * Complex read query 4.
 *
 * Copyright © 2017 Alain Kägi
 */

package ldbc.queries;

import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery4Result;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import java.text.SimpleDateFormat;

public class Query4 implements ExecutableQuery {

    /* Static query parameters. */
    private static final String queryName = "Query4";
    private static final String queryParameterFilename = "query_4_param.txt";
    private static final String queryParameterFileLinePattern = "(\\d+)\\|(\\d+)\\|(\\d+)";

    /** A minimal constructor. */
    private Query4() {}

    /** A main entry point to run a microbenchmark. */
    public static void main(String[] args) {
        MicroBenchmark.executeQueriesWithParametersFromFile(new Query4(), queryName, queryParameterFilename, queryParameterFileLinePattern);
    }

    /**
     * New topics (fourth complex read query).
     * @param db          A database handle
     * @param personId    A person ID
     * @param startDdate  A date in milliseconds since 1/1/1970 00:00:00 GMT
     * @param duration    A duration in days
     * @param limit       An upper bound on the size of results returned
     * @return topics first created in the range provided
     */
    public static List<LdbcQuery4Result> query(Connection db,
        long personId,
        long startDate, int duration,
        int limit) throws SQLException {

        List<LdbcQuery4Result> result = new ArrayList<>();

        PreparedStatement statement = db.prepareStatement(
            "  SELECT Tag.name, COUNT(*) " +
            "    FROM Tag, PersonKnowsPerson, " +
            "         MessageHasCreatorPerson, Message, PostHasTagTag " +
            "   WHERE ? = PersonKnowsPerson.person1Id " +
            "     AND PersonKnowsPerson.person2Id = MessageHasCreatorPerson.personId " +
            "     AND MessageHasCreatorPerson.messageId = Message.id " +
            "     AND Message.id = PostHasTagTag.postId " +
            "     AND PostHasTagTag.tagId = Tag.id " +
            "     AND Message.creationDate >= ? " +
            "     AND Message.creationDate < ? + ? * 24 * 60 * 60 * 1000 " +
            "     AND NOT EXISTS " +
            "         (SELECT * " +
            "            FROM PersonKnowsPerson, " +
            "                 MessageHasCreatorPerson, Message, " +
            "                 PostHasTagTag " +
            "           WHERE ? = PersonKnowsPerson.person1Id " +
            "             AND PersonKnowsPerson.person2Id = MessageHasCreatorPerson.personId " +
            "             AND MessageHasCreatorPerson.messageId = Message.id " +
            "             AND Message.id = PostHasTagTag.postId " +
            "             AND PostHasTagTag.tagId = Tag.id " +
            "             AND Message.creationDate < ? " +
            "         ) " +
            "GROUP BY Tag.name " +
            "ORDER BY COUNT(*) DESC, Tag.name " +
            "   LIMIT ?");
        statement.setLong(1, personId);
        statement.setLong(2, startDate);
        statement.setLong(3, startDate);
        statement.setLong(4, duration);
        statement.setLong(5, personId);
        statement.setLong(6, startDate);
        statement.setInt(7, limit);
        ResultSet resultSet = statement.executeQuery();
        while (resultSet.next()) {
            String tagName = resultSet.getString("Tag.name");
            int tagCount = resultSet.getInt("COUNT(*)");

            LdbcQuery4Result r = new LdbcQuery4Result(tagName, tagCount);
            result.add(r);
        }

        return result;
    }

    /**
     * Execute query 4 for microbenchmarking.
     * @param db               A database handle
     * @param queryParameters  Stream of query input parameters
     * @param beVerbose        Print query outputs if true
     */
    public void executeQueries(Connection db, QueryParameterFile queryParameters, boolean beVerbose) throws SQLException {
        while (queryParameters.nextLine()) {
            long personId = queryParameters.getLong();
            long startDate = queryParameters.getLong();
            int duration = queryParameters.getInt();

            List<LdbcQuery4Result> r = query(db, personId, startDate, duration, 10);

            if (beVerbose)
                print(personId, startDate, duration, r);
       }
    }

    /**
     * Pretty print the query 4 results.
     * @param personId   Query 4 parameter 1
     * @param startDate  Query 4 parameter 2
     * @param duration   Query 4 parameter 3
     * @param results    Query 4 results
     */
    static void print(long personId, long startDate, int duration,
                      List<LdbcQuery4Result> results) {
        SimpleDateFormat dateTimeFmt = new SimpleDateFormat("EEE MMM dd HH:mm:ss yyyy");
        dateTimeFmt.setTimeZone(TimeZone.getTimeZone("UTC"));
        System.out.println(personId
                           + " " + dateTimeFmt.format(startDate)
                           + " " + dateTimeFmt.format(startDate + (long)duration * 24 * 60 * 60 * 1000));

        if (results.size() == 0) {
            System.out.println("  no matches");
            System.out.println("");
            return;
        }

        for (LdbcQuery4Result result : results)
            System.out.println("  " + result.tagName()
                               + " " + result.postCount());
        System.out.println("");
    }

}
