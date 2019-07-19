/*
 * Copyright © 2017-2019 Alain Kägi
 */

package ldbc.queries;

import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery9Result;

import com.zaxxer.hikari.HikariDataSource;

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

import ldbc.utils.Explanation;

/**
 * The Query9 class implements an application that runs complex read
 * query 9 from the LDBC Social Network Benchmark (SNB) as a
 * microbenchmark.  As input, it expects a parameter substitution
 * parameter file as generated by the LDBC SNB data generator.
 */
public class Query9 implements ExecutableQuery {

    /* Static query parameters. */
    private static final String queryName = "Query9";
    private static final String queryParameterFilename = "interactive_9_param.txt";
    private static final String queryParameterFileLinePattern = "(\\d+)\\|(\\d+)";
    private static final int queryLimit = 20;
    private static final String queryString =
        "   SELECT Person.id, Person.firstName, Person.lastName, " +
        "          Message.id, Message.content, Message.imageFile, " +
        "          Message.creationDate " +
        "     FROM Person, Message, MessageHasCreatorPerson, " +
        "          (SELECT person2Id " +
        "             FROM PersonKnowsPerson " +
        "            WHERE person1Id = ? " +

        "            UNION " +

        "           SELECT k2.person2Id " +
        "             FROM PersonKnowsPerson as k1, PersonKnowsPerson as k2 " +
        "            WHERE k1.person1Id = ? " +
        "              AND k1.person2Id = k2.person1Id " +
        "              AND k2.person2Id <> ? " +
        "          ) as f " +
        "    WHERE f.person2Id = Person.id " +
        "      AND Person.id = MessageHasCreatorPerson.personId " +
        "      AND MessageHasCreatorPerson.messageId = Message.id " +
        "      AND Message.creationDate < ? " +
        " ORDER BY Message.creationDate DESC, Message.id " +
        "    LIMIT ?";

    /** A minimal constructor. */
    private Query9() {}

    /**
     * Run LDBC SNB complex read query 9 as a microbenchmark.
     *
     * <p>A configuration file called <tt>params.ini</tt> specifies
     * the running parameters.
     *
     * @param args  Unused
     */
    public static void main(String[] args) {
        Microbenchmark.executeQueryWithParametersFromFile(new Query9(), queryName, queryParameterFilename, queryParameterFileLinePattern);
    }

    /**
     * Recent messages by friends (ninth complex read query).
     * @param ds        A data source
     * @param personId  The person's unique identifier
     * @param date      A date (milliseconds since the start of the epoch)
     * @param limit     An upper bound on the number of results returned
     * @return the top 'limit' posts by friends of the given person made before the given date
     * @throws SQLException if a database access error occurs
     */
    public static List<LdbcQuery9Result> query(HikariDataSource ds, long personId, long date, int limit) throws SQLException {
        List<LdbcQuery9Result> results = new ArrayList<>();

        ResultSet r = null;

        try (Connection c = ds.getConnection();
             PreparedStatement s = c.prepareStatement(queryString)) {
            s.setLong(1, personId);
            s.setLong(2, personId);
            s.setLong(3, personId);
            s.setLong(4, date);
            s.setInt(5, limit);
            r = s.executeQuery();
            while (r.next()) {
                LdbcQuery9Result result = new LdbcQuery9Result(
                    r.getLong("Person.id"),
                    r.getString("Person.firstName"),
                    r.getString("Person.lastName"),
                    r.getLong("Message.id"),

                    // One or the other field must be empty.
                    r.getString("Message.content") + r.getString("Message.imageFile"),

                    r.getLong("Message.creationDate"));
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
     * Explain the query for the given inputs.
     * @param db        A database handle
     * @param personId  The person's unique identifier
     * @param date      A date (milliseconds since the start of the epoch)
     * @param limit     An upper bound on the number of results returned
     * @return information about the query execution plan
     * @throws SQLException if a database access error occurs
     */
    private static ResultSet explain(HikariDataSource db, long personId, long date, int limit) throws SQLException {
        Connection c = db.getConnection();
        PreparedStatement s = c.prepareStatement(Explanation.query + queryString);
        s.setLong(1, personId);
        s.setLong(2, personId);
        s.setLong(3, personId);
        s.setLong(4, date);
        s.setInt(5, limit);
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
            long date = queryParameters.getLong();

            if (printHeapUsage)
                heapUsage.print(System.out);

            List<LdbcQuery9Result> results = query(db, personId, date, queryLimit);

            if (beVerbose)
                print(personId, new Date(date), results);
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
            long date = queryParameters.getLong();

            ResultSet r = explain(db, personId, date, queryLimit);

            ldbc.utils.Explanation.print(System.out, r);
        }
    }

    /**
     * Pretty print the query 9 results.
     * @param personId  Query 9 parameter 1
     * @param date      Query 9 parameter 2
     * @param results   Query 9 results
     */
    private static void print(long personId, Date date, List<LdbcQuery9Result> results) {
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
