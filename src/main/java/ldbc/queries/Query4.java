/*
 * Copyright © 2017-2019 Alain Kägi
 */

package ldbc.queries;

import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery4Result;

import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import java.text.SimpleDateFormat;

import ldbc.utils.Explanation;

/**
 * The Query4 class implements an application that runs complex read
 * query 4 from the LDBC Social Network Benchmark (SNB) as a
 * microbenchmark.  As input, it expects a parameter substitution
 * parameter file as generated by the LDBC SNB data generator.
 */
public class Query4 implements ExecutableQuery {

    /* Static query parameters. */
    private static final String queryName = "Query4";
    private static final String queryParameterFilename = "interactive_4_param.txt";
    private static final String queryParameterFileLinePattern = "(\\d+)\\|(\\d+)\\|(\\d+)";
    private static final int queryLimit = 10;
    private static final String queryString =
        "   SELECT Tag.name, COUNT(*) " +
        "     FROM Tag, PersonKnowsPerson, " +
        "          MessageHasCreatorPerson, Message, PostHasTagTag " +
        "    WHERE ? = PersonKnowsPerson.person1Id " +
        "      AND PersonKnowsPerson.person2Id = MessageHasCreatorPerson.personId " +
        "      AND MessageHasCreatorPerson.messageId = Message.id " +
        "      AND Message.id = PostHasTagTag.postId " +
        "      AND PostHasTagTag.tagId = Tag.id " +
        "      AND Message.creationDate >= ? " +
        "      AND Message.creationDate < ? + ? * 24 * 60 * 60 * 1000 " +
        "      AND NOT EXISTS " +
        "          (SELECT * " +
        "             FROM PersonKnowsPerson, " +
        "                  MessageHasCreatorPerson, Message, " +
        "                  PostHasTagTag " +
        "            WHERE ? = PersonKnowsPerson.person1Id " +
        "              AND PersonKnowsPerson.person2Id = MessageHasCreatorPerson.personId " +
        "              AND MessageHasCreatorPerson.messageId = Message.id " +
        "              AND Message.id = PostHasTagTag.postId " +
        "              AND PostHasTagTag.tagId = Tag.id " +
        "              AND Message.creationDate < ? " +
        "          ) " +
        " GROUP BY Tag.name " +
        " ORDER BY COUNT(*) DESC, Tag.name " +
        "    LIMIT ?";

    /** A minimal constructor. */
    private Query4() {}

    /**
     * Run LDBC SNB complex read query 4 as a microbenchmark.
     *
     * <p>A configuration file called <tt>params.ini</tt> specifies
     * the running parameters.
     *
     * @param args  Unused
     */
    public static void main(String[] args) {
        Microbenchmark.executeQueryWithParametersFromFile(new Query4(), queryName, queryParameterFilename, queryParameterFileLinePattern);
    }

    /**
     * New topics (fourth complex read query).
     * @param ds         A data source
     * @param personId   The person's unique identifier
     * @param startDate  A date (milliseconds since the start of the epoch)
     * @param duration   A duration in days
     * @param limit      An upper bound on the number of results returned
     * @return the top 'limit' topics associated with posts created by the given person's friends in the time range provided
     * @throws SQLException if a database access error occurs
     */
    public static List<LdbcQuery4Result> query(HikariDataSource ds, long personId, long startDate, int duration, int limit) throws SQLException {
        List<LdbcQuery4Result> results = new ArrayList<>();

        ResultSet r = null;

        try (Connection c = ds.getConnection();
             PreparedStatement s = c.prepareStatement(queryString)) {
            s.setLong(1, personId);
            s.setLong(2, startDate);
            s.setLong(3, startDate);
            s.setLong(4, duration);
            s.setLong(5, personId);
            s.setLong(6, startDate);
            s.setInt(7, limit);
            r = s.executeQuery();
            while (r.next()) {
                LdbcQuery4Result result = new LdbcQuery4Result(
                    r.getString("Tag.name"),
                    r.getInt("COUNT(*)"));
                results.add(result);
            }
            c.commit();
        }
        finally {
            if (r != null) r.close();
        }

        return results;
    }

    /**
     * Explain query 4 with the given inputs.
     * @param db         A database handle
     * @param personId   The person's unique identifier
     * @param startDate  A date (milliseconds since the start of the epoch)
     * @param duration   A duration in days
     * @param limit      An upper bound on the number of results returned
     * @return information about the query execution plan
     * @throws SQLException if a database access error occurs
     */
    private static ResultSet explain(HikariDataSource db, long personId, long startDate, int duration, int limit) throws SQLException {
        Connection c = db.getConnection();
        PreparedStatement s = c.prepareStatement(Explanation.query + queryString);
        s.setLong(1, personId);
        s.setLong(2, startDate);
        s.setLong(3, startDate);
        s.setLong(4, duration);
        s.setLong(5, personId);
        s.setLong(6, startDate);
        s.setInt(7, limit);
        return s.executeQuery();
    }

    /**
     * Execute the query once for every query parameters.
     * @param db               A database handle
     * @param queryParameters  Stream of query input parameters
     * @param beVerbose        Print query outputs if true
     * @param printHeapUsage   Print heap usage if true
     * @throws SQLException if a database access error occurs
     */
    public void executeQuery(HikariDataSource db, QueryParameterFile queryParameters, boolean beVerbose, boolean printHeapUsage) throws SQLException {
        HeapUsage heapUsage = new HeapUsage();

        while (queryParameters.nextLine()) {
            long personId = queryParameters.getLong();
            long startDate = queryParameters.getLong();
            int duration = queryParameters.getInt();

            if (printHeapUsage)
                heapUsage.print(System.out);

            List<LdbcQuery4Result> results = query(db, personId, startDate, duration, queryLimit);

            if (beVerbose)
                print(personId, startDate, duration, results);
       }
    }

    /**
     * Explain the query with the first set of query parameters.
     * @param db               A database handle
     * @param queryParameters  Stream of query input parameters
     * @throws SQLException if a database access error occurs
     */
    public void explainQuery(HikariDataSource db, QueryParameterFile queryParameters) throws SQLException {
        if (queryParameters.nextLine()) {
            long personId = queryParameters.getLong();
            long startDate = queryParameters.getLong();
            int duration = queryParameters.getInt();

            ResultSet r = explain(db, personId, startDate, duration, queryLimit);

            ldbc.utils.Explanation.print(System.out, r);
        }
    }

    /**
     * Pretty print the query results.
     * @param personId   Query 4 parameter 1
     * @param startDate  Query 4 parameter 2
     * @param duration   Query 4 parameter 3
     * @param results    Query 4 results
     */
    private static void print(long personId, long startDate, int duration, List<LdbcQuery4Result> results) {
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
