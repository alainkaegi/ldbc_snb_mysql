/*
 * Copyright © 2018-2019 Alain Kägi
 */

package ldbc.queries;

import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery6Result;

import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import java.util.ArrayList;
import java.util.List;

import ldbc.utils.Explanation;
import ldbc.utils.LdbcUtils;

/**
 * The Query6 class implements an application that runs complex read
 * query 6 from the LDBC Social Network Benchmark (SNB) as a
 * microbenchmark.  As input, it expects a parameter substitution
 * parameter file as generated by the LDBC SNB data generator.
 */
public class Query6 implements ExecutableQuery {

    /* Static query parameters. */
    private static final String queryName = "Query6";
    private static final String queryParameterFilename = "interactive_6_param.txt";
    private static final String queryParameterFileLinePattern = "(\\d+)\\|(.+)";
    private static final int queryLimit = 10;
    // Tags co-occurring with the given tag.
    // Parameter 1: person identifier
    // Parameter 2: person identifier (same as parameter 1)
    // Parameter 3: tag name
    // Parameter 4: tag name (same as parameter 3)
    // Parameter 5: an upper bound on the number of results returned
    private static final String queryString =
        "   SELECT Tag.name, " +
        "          COUNT(*) " +
        "     FROM (SELECT MessageHasCreatorPerson.messageId " +
        "             FROM (SELECT PersonKnowsPerson.person2Id " +
        "                     FROM PersonKnowsPerson " +
        "                    WHERE PersonKnowsPerson.person1Id = ? " +
        "                    UNION SELECT K2.person2Id " +
        "                     FROM PersonKnowsPerson AS K1, " +
        "                          PersonKnowsPerson AS K2 " +
        "                    WHERE K1.person1Id = ? " +
        "                      AND K2.person1Id = K1.person2Id " +
        "                  ) AS Friends, " +
        "                  MessageHasCreatorPerson, " +
        "                  PostHasTagTag, " +
        "                  Tag " +
        "            WHERE MessageHasCreatorPerson.personId = Friends.person2Id " +
        "              AND PostHasTagTag.postId = MessageHasCreatorPerson.messageId " +
        "              AND PostHasTagTag.tagId = Tag.id " +
        "              AND Tag.name = ? " +
        "          ) AS PostWithGivenTag, " +
        "          PostHasTagTag, " +
        "          Tag " +
        "    WHERE PostWithGivenTag.messageId = PostHasTagTag.postId " +
        "      AND Tag.id = PostHasTagTag.tagId " +
        "      AND Tag.name <> ? " +
        " GROUP BY Tag.name " +
        " ORDER BY COUNT(*) DESC, Tag.name " +
        "    LIMIT ?";

    /** A minimal constructor. */
    private Query6() {}

    /**
     * Run LDBC SNB complex read query 6 as a microbenchmark.
     *
     * <p>A configuration file called <tt>params.ini</tt> specifies
     * the running parameters.
     *
     * @param args  Unused
     */
    public static void main(String[] args) {
        Microbenchmark.executeQueryWithParametersFromFile(new Query6(), queryName, queryParameterFilename, queryParameterFileLinePattern);
    }

    /**
     * Tag co-occurrence (sixth complex read query).
     * @param ds        A data source
     * @param personId  The person's unique identifier
     * @param tag       A tag name
     * @param limit     The upper bound on the number of results returned
     * @return the top 'limit' other tags that occurs with the given tag in posts created by the given person's friends
     * @throws SQLException if a database access error occurs
     */
    public static List<LdbcQuery6Result> query(HikariDataSource ds, long personId, String tag, int limit) throws SQLException {
        List<LdbcQuery6Result> results = new ArrayList<>();

        ResultSet r = null;
        try (Connection c = ds.getConnection();
             PreparedStatement s = c.prepareStatement(queryString)) {
            s.setLong(1, personId);
            s.setLong(2, personId);
            s.setString(3, tag);
            s.setString(4, tag);
            s.setInt(5, limit);
            r = s.executeQuery();
            while (r.next()) {
                LdbcQuery6Result result = new LdbcQuery6Result(
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
     * Explain query 6 with the given inputs.
     * @param db        A database handle
     * @param personId  The person's unique identifier
     * @param tag       A tag name
     * @param limit     An upper bound on the number of results returned
     * @return information about the query execution plan
     * @throws SQLException if a database access error occurs
     */
    private static ResultSet explain(HikariDataSource db, long personId, String tag, int limit) throws SQLException {
        Connection c = db.getConnection();
        PreparedStatement s = c.prepareStatement(Explanation.query + queryString);
        s.setLong(1, personId);
        s.setLong(2, personId);
        s.setString(3, tag);
        s.setString(4, tag);
        s.setInt(5, queryLimit);
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
            String tag = queryParameters.getString();

            if (printHeapUsage)
                heapUsage.print(System.out);

            List<LdbcQuery6Result> results = query(db, personId, tag, queryLimit);

            if (beVerbose)
                print(personId, tag, results);
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
            String tag = queryParameters.getString();

            ResultSet r = explain(db, personId, tag, queryLimit);

            ldbc.utils.Explanation.print(System.out, r);
        }
    }

    /**
     * Pretty print the query 6 results.
     * @param personId  Query 6 parameter 1
     * @param tag       Query 6 parameter 2
     * @param results   Query 6 results
     */
    private static void print(long personId, String tag, List<LdbcQuery6Result> results) {
        System.out.println(personId + " " + tag);

        if (results.size() == 0) {
            System.out.println("  no matches");
            System.out.println("");
            return;
        }

        for (LdbcQuery6Result result : results)
            System.out.println("  " + result.tagName()
                               + " " + result.postCount());
        System.out.println("");
    }

}
