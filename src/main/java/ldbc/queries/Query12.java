/*
 * Copyright © 2018-2019 Alain Kägi
 */

package ldbc.queries;

import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery12Result;

import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.TreeSet;

import ldbc.helpers.Query12SortResult;
import ldbc.helpers.Query12PartialResult;

import ldbc.utils.Explanation;
import ldbc.utils.LdbcUtils;

/**
 * The Query12 class implements an application that runs complex read
 * query 12 from the LDBC Social Network Benchmark (SNB) as a
 * microbenchmark.  As input, it expects a parameter substitution
 * parameter file as generated by the LDBC SNB data generator.
 */
public class Query12 implements ExecutableQuery {

    /* Static query parameters. */
    private static final String queryName = "Query12";
    private static final String queryParameterFilename = "interactive_12_param.txt";
    private static final String queryParameterFileLinePattern = "(\\d+)\\|(.+)";
    private static final int queryLimit = 20;
    // Friends, their comments, and the comments' parent posts' tags.
    // Parameter 1: person identifier
    private static final String queryString =
        "   SELECT PersonKnowsPerson.person2Id, " +
        "          CommentReplyOfMessage.commentId, " +
        "          PostHasTagTag.tagId " +
        "     FROM PersonKnowsPerson, " +
        "          MessageHasCreatorPerson, " +
        "          CommentReplyOfMessage, " +
        "          PostHasTagTag " +
        "    WHERE PersonKnowsPerson.person1Id = ? " +
        "      AND MessageHasCreatorPerson.personId = PersonKnowsPerson.person2Id " +
        "      AND CommentReplyOfMessage.commentId = MessageHasCreatorPerson.messageId " +
        "      AND PostHasTagTag.postId = CommentReplyOfMessage.messageId";

    /** A minimal constructor. */
    private Query12() {}

    /**
     * Run LDBC SNB complex read query 12 as a microbenchmark.
     *
     * <p>A configuration file called <tt>params.ini</tt> specifies
     * the running parameters.
     *
     * @param args  Unused
     */
    public static void main(String[] args) {
        Microbenchmark.executeQueryWithParametersFromFile(new Query12(), queryName, queryParameterFilename, queryParameterFileLinePattern);
    }

    /**
     * Expert search (12th complex read query).
     * @param ds            A data source
     * @param personId      The person's unique identifier
     * @param tagClassName  The tag class's name
     * @param limit         An upper bound on the number of results returned
     * @return the top 'limit' friends of the given person who created comments in response to posts; consider only posts with tags within the given class
     * @throws SQLException if a database access error occurs
     */
    public static List<LdbcQuery12Result> query(HikariDataSource ds, long personId, String tagClassName, int limit) throws SQLException {
        List<LdbcQuery12Result> results = new ArrayList<>();

        // Create a priority queue to keep the results sorted and
        // limited to at most the requested 'limit' entries.  To make
        // this work, we inverse the sort order so we know it is safe
        // to remove the entry with the "highest" priority when the
        // queue reaches 'limit + 1' elements.
        Queue<Query12SortResult> queue = new PriorityQueue<>(limit + 1);

        Map<Long, Query12PartialResult> partials = new HashMap<>();
        Set<Long> comments = new TreeSet<>();

        ResultSet r = null;

        try (Connection c = ds.getConnection();
             PreparedStatement s = c.prepareStatement(queryString)) {

            long tagClassId = LdbcUtils.getTagClassId(c, tagClassName);

            // Execute the SQL query and update partial results as necessary.
            s.setLong(1, personId);
            r = s.executeQuery();
            while (r.next()) {
                long parentPostTagId = r.getLong("PostHasTagTag.tagId");
                long parentPostTagClassId = LdbcUtils.getTypeTagClassIdOf(c, parentPostTagId);

                // Skip if the post's tag is not in the given class.
                if (parentPostTagClassId != tagClassId
                    && !LdbcUtils.isTagClassSubclassOfTagClass(c, parentPostTagClassId, tagClassId))
                    continue;

                long friendId = r.getLong("PersonKnowsPerson.person2Id");
                long commentId = r.getLong("CommentReplyOfMessage.commentId");

                Query12PartialResult partial;
                if (partials.get(friendId) == null) {
                    partial = new Query12PartialResult(parentPostTagId);
                    partials.put(friendId, partial);
                    comments.add(commentId);
                }
                else {
                    partial = partials.get(friendId);
                    if (!comments.contains(commentId)) {
                        partial.incReplyCount();
                        comments.add(commentId);
                    }
                    partial.addTag(parentPostTagId);
                }
            }

            // Iterate over the partial results and add to the sorting queue.
            for (Map.Entry<Long, Query12PartialResult> entry : partials.entrySet()) {
                long friendId = entry.getKey();

                Query12SortResult e = new Query12SortResult(
                     friendId,
                     entry.getValue().replyCount());

                queue.add(e);

                // Eliminate the 'highest' priority entry if we have
                // reached the target number of results.
                Query12SortResult ignore;
                if (queue.size() > limit)
                    ignore = queue.poll();
            }

            // Copy the results by adding elements at the beginning of
            // the final list filling the additional fields as we go.
            while (queue.size() != 0) {
                Query12SortResult e = queue.poll(); // Dequeue.
                long friendId = e.friendId();

                // Convert tag identifiers to strings.
                Set<String> tags = new TreeSet<>();
                for (long tagId : partials.get(friendId).tags())
                    tags.add(LdbcUtils.getTagName(c, tagId));

                LdbcQuery12Result result = new LdbcQuery12Result(
                    friendId,
                    LdbcUtils.getFirstName(c, friendId),
                    LdbcUtils.getLastName(c, friendId),
                    tags,
                    e.replyCount());
                results.add(0, result); // Add at the front.
            }

            c.commit();
        } finally {
            if (r != null) r.close();
        }

        return results;
    }

    /**
     * Explain the main query for the given inputs.
     * @param db            A database handle
     * @param personId      The person's unique identifier
     * @param tagClassName  The tag class's name
     * @param limit         An upper bound on the number of results returned
     * @return the top 'limit' friends of the given person with the given first name
     * @throws SQLException if a database access error occurs
     */
    private static ResultSet explain(HikariDataSource db, long personId, String tagClassName, int limit) throws SQLException {
        Connection c = db.getConnection();
        PreparedStatement s = c.prepareStatement(Explanation.query + queryString);
        s.setLong(1, personId);
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
            String tagClassName = queryParameters.getString();

            if (printHeapUsage)
                heapUsage.print(System.out);

            List<LdbcQuery12Result> results = query(db, personId, tagClassName, queryLimit);

            if (beVerbose)
                print(personId, tagClassName, results);
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
            String tagClassName = queryParameters.getString();

            ResultSet r = explain(db, personId, tagClassName, queryLimit);

            ldbc.utils.Explanation.print(System.out, r);
        }
    }

    /**
     * Pretty print the query 12 results.
     * @param personId   Query 12 parameter 1
     * @param firstName  Query 12 parameter 2
     * @param results    Query 12 results
     */
    private static void print(long personId, String tagClassName, List<LdbcQuery12Result> results) {
        System.out.println(personId + " " + tagClassName);

        if (results.size() == 0) {
            System.out.println("  no matches");
            System.out.println("");
            return;
        }

        for (LdbcQuery12Result result : results) {
            System.out.println("  " + result.personId()
                               + ", " + result.personFirstName()
                               + " " + result.personLastName()
                               + ", " + result.replyCount());
            for (String tag : result.tagNames())
                System.out.println("    " + tag);
        }

        System.out.println("");
    }

}
