/*
 * Copyright © 2018-2019 Alain Kägi
 */

package ldbc.queries;

import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery7Result;

import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TimeZone;

import java.text.SimpleDateFormat;

import ldbc.utils.LdbcUtils;

import ldbc.utils.Explanation;

/**
 * The Query7 class implements an application that runs complex read
 * query 7 from the LDBC Social Network Benchmark (SNB) as a
 * microbenchmark.  As input, it expects a parameter substitution
 * parameter file as generated by the LDBC SNB data generator.
 */
public class Query7 implements ExecutableQuery {

    /* Static query parameters. */
    private static final String queryName = "Query7";
    private static final String queryParameterFilename = "interactive_7_param.txt";
    private static final String queryParameterFileLinePattern = "(\\d+)";
    private static final int queryLimit = 20;
    // Recent likes of messages created by the start person
    // Parameter 1: person identifier
    // Parameter 2: person identifier (same as parameter 1)
    // Parameter 3: person identifier (same as parameter 1)
    private static final String queryString =
        "   SELECT Person.id AS personId, " +
        "          Person.firstName, " +
        "          Person.lastName, " +
        "          MAX(U.creationDate) AS date, " +
        "          U.messageId, " +
        "          Message.content, " +
        "          Message.imageFile, " +
        "          TRUNCATE((U.creationDate - Message.creationDate)/60000,0) AS latency, " +
        "          (   SELECT COUNT(*) " +
        "                FROM PersonKnowsPerson " +
        "               WHERE PersonKnowsPerson.person1Id = ? " +
        "                 AND PersonKnowsPerson.person2Id = personId " +
        "          ) AS isFriendOfStartPerson " +  // isFriendOfStartPerson > 0 => friends
        "     FROM (   SELECT MessageHasCreatorPerson.messageId, " +
        "                     PersonLikesPost.personId, " +
        "                     PersonLikesPost.creationDate " +
        "                FROM MessageHasCreatorPerson, " +
        "                     PersonLikesPost " +
        "               WHERE MessageHasCreatorPerson.personId = ? " +
        "                 AND PersonLikesPost.postId = MessageHasCreatorPerson.messageId " +
        "               UNION " +
        "              SELECT MessageHasCreatorPerson.messageId, " +
        "                     PersonLikesComment.personId, " +
        "                     PersonLikesComment.creationDate " +
        "                FROM MessageHasCreatorPerson, " +
        "                     PersonLikesComment " +
        "               WHERE MessageHasCreatorPerson.personId = ? " +
        "                 AND PersonLikesComment.commentId = MessageHasCreatorPerson.messageId " +
        "          ) AS U, " +
        "          Person, " +
        "          Message " +
        "    WHERE Person.id = U.personId " +
        "      AND Message.id = U.messageId " +
        " GROUP BY personId, " +
        "          Person.firstName, " +
        "          Person.lastName, " +
        "          U.messageId, " +
        "          Message.content, " +
        "          latency, " +
        "          isFriendOfStartPerson " +
        " ORDER BY date DESC";

    /** A minimal constructor. */
    private Query7() {}

    /**
     * Run LDBC SNB complex read query 7 as a microbenchmark.
     *
     * <p>A configuration file called <tt>params.ini</tt> specifies
     * the running parameters.
     *
     * @param args  Unused
     */
    public static void main(String[] args) {
        Microbenchmark.executeQueryWithParametersFromFile(new Query7(), queryName, queryParameterFilename, queryParameterFileLinePattern);
    }

    /**
     * Recent likes (seventh complex read query).
     * @param ds        A data source
     * @param personId  The person's unique identifier
     * @param limit     An upper bound on the number of results returned
     * @return the top 'limit' recent likes on the given person's messages
     * @throws SQLException if a database access error occurs
     */
    public static List<LdbcQuery7Result> query(HikariDataSource ds, long personId, int limit) throws SQLException {
        List<LdbcQuery7Result> results = new ArrayList<>();

        Set<Long> likers = new HashSet<>();

        ResultSet r = null;

        try (Connection c = ds.getConnection();
             PreparedStatement s = c.prepareStatement(queryString)) {
            s.setLong(1, personId);
            s.setLong(2, personId);
            s.setLong(3, personId);
            r = s.executeQuery();
            while (r.next() && results.size() < limit) {
                long likerId = r.getLong("personId");

                // Skip a liker we have already seen (I do not know how to
                // fold this functionality directly in the SQL query).
                if (likers.contains(likerId))
                    continue;

                likers.add(likerId);

                LdbcQuery7Result result = new LdbcQuery7Result(
                    likerId,
                    r.getString("Person.firstName"),
                    r.getString("Person.lastName"),
                    r.getLong("date"),
                    r.getLong("U.messageId"),

                    // One or the other field must be empty.
                    r.getString("Message.content") + r.getString("Message.imageFile"),

                    r.getInt("latency"),
                    r.getInt("isFriendOfStartPerson") == 0);
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
     * Explain query 7 with the given inputs.
     * @param db        A database handle
     * @param personId  The person's unique identifier
     * @param limit     An upper bound on the number of results returned
     * @return information about the query execution plan
     * @throws SQLException if a database access error occurs
     */
    private static ResultSet explain(HikariDataSource db, long personId, int limit) throws SQLException {
        Connection c = db.getConnection();
        PreparedStatement s = c.prepareStatement(Explanation.query + queryString);
        s.setLong(1, personId);
        s.setLong(2, personId);
        s.setLong(3, personId);
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

            if (printHeapUsage)
                heapUsage.print(System.out);

            List<LdbcQuery7Result> results = query(db, personId, queryLimit);

            if (beVerbose)
                print(personId, results);
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

            ResultSet r = explain(db, personId, queryLimit);

            ldbc.utils.Explanation.print(System.out, r);
        }
    }

    /**
     * Pretty print the query 7 results.
     * @param personId  Query 7 parameter 1
     * @param results   Query 7 results
     */
    private static void print(long personId, List<LdbcQuery7Result> results) {
        System.out.println(personId);

        if (results.size() == 0) {
            System.out.println("  no matches");
            System.out.println("");
            return;
        }

        SimpleDateFormat dateTimeZFmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");
        dateTimeZFmt.setTimeZone(TimeZone.getTimeZone("UTC"));

        for (LdbcQuery7Result result : results) {
            System.out.println("  " + result.personId()
                               + ", " + result.personFirstName()
                               + " " + result.personLastName()
                               + (result.isNew() ? " (new)" : "")
                               + ", " + dateTimeZFmt.format(new Date(result.likeCreationDate()))
                               + ", " + result.commentOrPostId()
                               + ", " + result.minutesLatency() + " min"
                               + ", " + result.commentOrPostContent());
        }
        System.out.println("");
    }

}
