/*
 * Copyright © 2018 Alain Kägi
 */

package ldbc.queries;

import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery13Result;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;

/**
 * The Query13 class implements an application that runs complex read
 * query 13 from the LDBC Social Network Benchmark (SNB) as a
 * microbenchmark.  As input, it expects a parameter substitution
 * parameter file as generated by the LDBC SNB data generator.
 */
public class Query13 implements ExecutableQuery {

    /* Static query parameters. */
    private static final String queryName = "Query13";
    private static final String queryParameterFilename = "query_13_param.txt";
    private static final String queryParameterFileLinePattern = "(\\d+)\\|(\\d+)";

    /** A minimal constructor. */
    private Query13() {}

    /**
     * Run LDBC SNB complex read query 13 as a microbenchmark.
     *
     * <p>A configuration file called <tt>params.ini</tt> specifies
     * the running parameters.
     *
     * @param args  Unused
     */
    public static void main(String[] args) {
        Microbenchmark.executeQueryWithParametersFromFile(new Query13(), queryName, queryParameterFilename, queryParameterFileLinePattern);
    }

    /**
     * Single shortest path (13th complex read query).
     * @param db         A database handle
     * @param person1Id  The start person unique identifier
     * @param person2Id  The end person unique identifier
     * @return the length of single shortest path between the given persons based on the Knows relationship, -1 if no such path exists
     * @throws SQLException if a database access error occurs
     */
    public static LdbcQuery13Result query(Connection db, long person1Id, long person2Id) throws SQLException {

        int pathLength = -1;

        // Breadth-first search:
        // open: the nodes at the current distance
        // nextOpen: the nodes at the current distance + 1
        // close: the nodes we have already seen
        Queue<Long> open = new LinkedList<>();
        Queue<Long> nextOpen = new LinkedList<>();
        Set<Long> close = new HashSet<>();

        int distance = 1;
        open.add(person1Id);
        close.add(person1Id);

        String friendQuery =
            "   SELECT PersonKnowsPerson.person2Id " +
            "     FROM PersonKnowsPerson " +
            "    WHERE PersonKnowsPerson.person1Id = ?";

        // Traverse the graph induced by the Knows relationship
        // breadth first search until we find the destination node or
        // there are no more node to visit.
        try {
            db.setAutoCommit(false);

            bfs:
            while (!open.isEmpty()) {

                for (Long person : open) {

                    PreparedStatement s = db.prepareStatement(friendQuery);
                    s.setLong(1, person);
                    ResultSet r = s.executeQuery();
                    while (r.next()) {
                        long friendId = r.getLong("PersonKnowsPerson.person2Id");

                        if (close.contains(friendId))
                            continue;

                        nextOpen.add(friendId);
                        close.add(friendId);

                        if (friendId == person2Id) {
                            pathLength = distance;
                            break bfs;
                        }
                    }

                    r.close();
                    s.close();
                }

                ++distance;
                open = nextOpen;
                nextOpen = new LinkedList<>();
            }
            db.commit();
        } finally {
            db.setAutoCommit(true);
        }

        return new LdbcQuery13Result(pathLength);
    }

    /**
     * Execute the query once for every query parameters.
     * @param db               A database handle
     * @param queryParameters  Stream of query input parameters
     * @param beVerbose        Print query outputs if true
     * @param printHeapUsage   Print heap usage if true
     * @throws SQLException if a database access error occurs
     */
    public void executeQuery(Connection db, QueryParameterFile queryParameters, boolean beVerbose, boolean printHeapUsage) throws SQLException {
        HeapUsage heapUsage = new HeapUsage();

        while (queryParameters.nextLine()) {
            long person1Id = queryParameters.getLong();
            long person2Id = queryParameters.getLong();

            if (printHeapUsage)
                heapUsage.print(System.out);

            LdbcQuery13Result result = query(db, person1Id, person2Id);

            if (beVerbose)
                print(person1Id, person2Id, result);
       }
    }

    /**
     * Explain the query with the first set of query parameters.
     * @param db               A database handle
     * @param queryParameters  Stream of query input parameters
     * @throws SQLException if a database access error occurs
     */
    public void explainQuery(Connection db, QueryParameterFile queryParameters) throws SQLException {
        System.out.println("Explain is not supported for this query at this time.");
    }

    /**
     * Pretty print the query 13 results.
     * @param person1Id  Query 13 parameter 1
     * @param person2Id  Query 13 parameter 2
     * @param result    Query 13 result
     */
    private static void print(long person1Id, long person2Id, LdbcQuery13Result result) {
        System.out.println(person1Id + " " + person2Id);
        System.out.println("  " + result.shortestPathLength());
    }

}
