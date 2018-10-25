/*
 * Copyright © 2018 Alain Kägi
 */

package ldbc.queries;

import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery10Result;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.TimeZone;

import ldbc.helpers.Query10SortResult;

import ldbc.utils.Explanation;
import ldbc.utils.LdbcUtils;

/**
 * The Query10 class implements an application that runs complex read
 * query 10 from the LDBC Social Network Benchmark (SNB) as a
 * microbenchmark.  As input, it expects a parameter substitution
 * parameter file as generated by the LDBC SNB data generator.
 */
public class Query10 implements ExecutableQuery {

    /* Static query parameters. */
    private static final String queryName = "Query10";
    private static final String queryParameterFilename = "query_10_param.txt";
    private static final String queryParameterFileLinePattern = "(\\d+)\\|(\\d+)";
    private static final int queryLimit = 10;
    // Friends of friends and their interests.
    // Parameter 1: person identifier
    // Parameter 2: person identifier (same as parameter 1)
    // Parameter 3: person identifier (same as parameter 1)
    // Parameter 4: month
    // Parameter 5: month (same as parameter 4)
    private static final String queryString =
        "   SELECT FriendAndPost.friendId, " +
        "          FriendAndPost.postId, " +
        "          PostHasTagTag.tagId " +
        "     FROM (   SELECT Friend.id AS friendId, " +
        "                     Post.id AS postId " +
        "                FROM (SELECT K2.person2Id AS id " +
        "                        FROM PersonKnowsPerson AS K1, " +
        "                             PersonKnowsPerson AS K2, " +
        "                             Person " +
        "                       WHERE K1.person1Id = ? " +
        "                         AND K2.person1Id = K1.person2Id " +
        "                         AND K2.person2Id <> ? " +
        "                         AND K2.person2Id " +
        "                      NOT IN (SELECT PersonKnowsPerson.person2Id " +
        "                                FROM PersonKnowsPerson " +
        "                               WHERE PersonKnowsPerson.person1Id = ?) " +
        "                         AND Person.id = K2.person2Id " +
        "                         AND (month(from_unixtime(Person.birthday/1000)) = ? " +
        "                              AND day(from_unixtime(Person.birthday/1000)) >= 21 " +
        "                               OR month(from_unixtime(Person.birthday/1000)) = (? % 12) + 1 " +
        "                              AND day(from_unixtime(Person.birthday/1000)) < 22) " +
        "                     ) AS Friend " + // Friends of friends (excluding start person and immediate friends).
        "           LEFT JOIN (SELECT MessageHasCreatorPerson.personId, " + // Not all friends have created posts.
        "                             MessageHasCreatorPerson.messageId AS id " +
        "                        FROM MessageHasCreatorPerson, " +
        "                             PostIsLocatedInPlace " +
        "                       WHERE MessageHasCreatorPerson.messageId = PostIsLocatedInPlace.postId " +
        "                     ) AS Post " +
        "                  ON Post.personId = Friend.id " +
        "          ) AS FriendAndPost " +
        "LEFT JOIN PostHasTagTag " + // Not all posts have tags.
        "       ON PostHasTagTag.postId = FriendAndPost.postId " +
        " ORDER BY FriendAndPost.friendId," +
        "          FriendAndPost.postId";

    /** A minimal constructor. */
    private Query10() {}

    /**
     * Run LDBC SNB complex read query 10 as a microbenchmark.
     *
     * <p>A configuration file called <tt>params.ini</tt> specifies
     * the running parameters.
     *
     * @param args  Unused
     */
    public static void main(String[] args) {
        Microbenchmark.executeQueryWithParametersFromFile(new Query10(), queryName, queryParameterFilename, queryParameterFileLinePattern);
    }

    /**
     * Friend recommendation (tenth complex read query).
     * @param db        A database handle
     * @param personId  The person's unique identifier
     * @param month     A month (between 1 and 12 inclusive)
     * @param limit     An upper bound on the number of results returned
     * @return the top 'limit' most similar friends of the given person; consider only friends born near the given month
     * @throws SQLException if a database access error occurs
     */
    public static List<LdbcQuery10Result> query(Connection db, long personId, int month, int limit) throws SQLException {
        List<LdbcQuery10Result> results = new ArrayList<>();

        try {
            db.setAutoCommit(false);

            // Create a priority queue to keep the results sorted and
            // limited to at most the requested 'limit' entries.  To
            // make this work, we inverse the sort order so we know it
            // is safe to remove the entry with the "highest" priority
            // when the queue reaches 'limit + 1' elements.
            Queue<Query10SortResult> queue = new PriorityQueue<>(limit + 1);

            Set<Long> startPersonInterests = LdbcUtils.getTags(db, personId);

            // The SQL query returns friend, post, and tag identifiers
            // triples.  The triples are grouped by friends and then
            // by posts.  The processing loop must handle three types
            // of triples:
            //
            // 1.   X    NULL NULL   (friend who hasn't created any post)
            // 2.   X    X    NULL   (post without any tag)
            // 3.   X    X    X
            //
            // Because of the way the database engine processes left
            // joins, types 2 and 3 will contain duplicates.
            //
            // The loop creates a new result entry after it detects a
            // phase change (i.e., a new post or a new friend).
            //
            // Type 1: 0 post, commonality = 0
            // Type 2: > 0 post, some commonality
            // Type 3: > 0 post, some commonality
            long prevFriendId = -1;
            long prevPostId = -1;
            boolean matchingInterest = false;
            int common = 0;
            int uncommon = 0;

            PreparedStatement s = db.prepareStatement(queryString);
            s.setLong(1, personId);
            s.setLong(2, personId);
            s.setLong(3, personId);
            s.setInt(4, month);
            s.setInt(5, month);
            ResultSet r = s.executeQuery();
            while (r.next()) {
                long friendId = r.getLong("FriendAndPost.friendId");
                long postId = r.getLong("FriendAndPost.postId");
                long tagId = r.getLong("PostHasTagTag.tagId");

                // Processing the next batch of posts?
                if (postId != prevPostId || (prevPostId == 0 && friendId != prevFriendId)) {

                    // If so, wrap up the accounting of the previous
                    // batch of posts.  If the previous post
                    // identifier is null, then the associated friend
                    // did not create any post.
                    if (prevPostId != -1 && prevPostId != 0)
                        if (matchingInterest)
                            ++common;
                        else
                            ++uncommon;

                    // Processing the next friend?
                    if (friendId != prevFriendId) {
                        if (prevFriendId != -1)
                            add(queue, limit, prevFriendId, common - uncommon);
                        prevFriendId = friendId;
                        common = 0;
                        uncommon = 0;
                    }

                    prevPostId = postId;
                    matchingInterest = false;
                }

                if (startPersonInterests.contains(tagId))
                    matchingInterest = true;

            }

            // Process the last friend/post pair.
            if (prevFriendId != -1) {
                // If the previous post identifier is null, then the
                // associated friend did not create any post.
                if (prevPostId != 0)
                    if (matchingInterest)
                        ++common;
                    else
                        ++uncommon;
                add(queue, limit, prevFriendId, common -  uncommon);
            }

            r.close();
            s.close();

            // Add elements to the final result array in reverse order.
            while (queue.size() != 0) {
                Query10SortResult e = queue.poll(); // Dequeue.
                long friendId = e.friendId();
                LdbcQuery10Result result = new LdbcQuery10Result(
                    friendId,
                    LdbcUtils.getFirstName(db, friendId),
                    LdbcUtils.getLastName(db, friendId),
                    e.commonInterestScore(),
                    LdbcUtils.getGender(db, friendId),
                    LdbcUtils.findPlace(db, friendId));
                results.add(0, result); // Add at the front.
            }

            db.commit();
        } finally {
            db.setAutoCommit(true);
        }

        return results;
    }

    /**
     * Add a result to the temporary queue.
     * @param queue             A temporary queue of query 10 results
     * @param limit             A limit on the size of the queue
     * @param friendId          The person's unique identifier
     * @param commonalityIndex  A measure of commonality between friends
     */
    static void add(Queue<Query10SortResult> queue, int limit, long friendId, int commonality) {
        queue.add(new Query10SortResult(friendId, commonality));

        // Eliminate the 'highest' priority entry if we have reached
        // the target number of results.
        Query10SortResult ignore;
        if (queue.size() > limit)
            ignore = queue.poll();
    }

    /**
     * Explain the main query for the given inputs.
     * @param db         A database handle
     * @param personId   The person's unique identifier
     * @param month     A month (between 1 and 12 inclusive)
     * @param limit      An upper bound on the number of results returned
     * @return the top 'limit' friends of the given person with the given first name
     * @throws SQLException if a database access error occurs
     */
    private static ResultSet explain(Connection db, long personId, int month, int limit) throws SQLException {
        PreparedStatement s = db.prepareStatement(Explanation.query + queryString);
        s.setLong(1, personId);
        s.setLong(2, personId);
        s.setLong(3, personId);
        s.setInt(4, month);
        s.setInt(5, month);
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
    public void executeQuery(Connection db, QueryParameterFile queryParameters, boolean beVerbose, boolean printHeapUsage) throws SQLException {
        HeapUsage heapUsage = new HeapUsage();

        while (queryParameters.nextLine()) {
            long personId = queryParameters.getLong();
            int month = queryParameters.getInt();

            if (printHeapUsage)
                heapUsage.print(System.out);

            List<LdbcQuery10Result> results = query(db, personId, month, queryLimit);

            if (beVerbose)
                print(personId, month, results);
       }
    }

    /**
     * Explain the query with the first set of query parameters.
     * @param db               A database handle
     * @param queryParameters  Stream of query input parameters
     * @throws SQLException if a database access error occurs
     */
    public void explainQuery(Connection db, QueryParameterFile queryParameters) throws SQLException {
        if (queryParameters.nextLine()) {
            long personId = queryParameters.getLong();
            int month = queryParameters.getInt();

            ResultSet r = explain(db, personId, month, queryLimit);

            ldbc.utils.Explanation.print(System.out, r);
        }
    }

    /**
     * Pretty print the query 10 results.
     * @param personId  Query 10 parameter 1
     * @param month     Query 10 parameter 2
     * @param results   Query 10 results
     */
    private static void print(long personId, int month, List<LdbcQuery10Result> results) {
        System.out.println(personId + " " + month);

        if (results.size() == 0) {
            System.out.println("  no matches");
            System.out.println("");
            return;
        }

        for (LdbcQuery10Result result : results) {
            System.out.println("  " + result.commonInterestScore()
                               + ", " + result.personId()
                               + ", " + result.personFirstName()
                               + " " + result.personLastName()
                               + ", " + result.personGender()
                               + ", " + result.personCityName());
        }

        System.out.println("");
    }

}
