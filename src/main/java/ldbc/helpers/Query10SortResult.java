/*
 * Copyright © 2018 Alain Kägi
 */

package ldbc.helpers;

/**
 * The Query10SortResult class defines a structure to hold one query
 * 10 result temporarily.
 *
 * <p>We accumulate query 10 results temporarily in a priority queue
 * derived from this class.  We use a priority queue so we can easily
 * identify unneeded elements.
 *
 * <p>We only accumulate enough information either necessary to sort
 * elements or to retrieve additional fields later.
 */
public class Query10SortResult implements Comparable<Query10SortResult> {
    private final long friendId;
    private final int commonInterestScore;

    /**
     * Construct a Query10SortResult object.
     * @param friendId             The friend's unique identifier
     * @param commonInterestScore  The friend and start person's common interest score
     */
    public Query10SortResult(long friendId, int commonInterestScore) {
        this.friendId = friendId;
        this.commonInterestScore = commonInterestScore;
    }

    /**
     * Return the friend's identifier.
     * @return the friend's unique identifier
     */
    public long friendId() { return friendId; }

    /**
     * Return the friend and start person's common interest score.
     * @return the friend and start person's common interest score
     */
    public int commonInterestScore() { return commonInterestScore; }

    /**
     * Define a sort order for this class.
     *
     * <p>It is exactly the reverse of the intended order so we can
     * identify an unwanted element by its 'high' priority.
     *
     * @param r  result to be compared
     * @return a negative integer, zero, or a positive integer as this object is less than, equal to, or greater than the specified object
     */
    public int compareTo(Query10SortResult r) {
        int s1 = this.commonInterestScore;
        int s2 = r.commonInterestScore();
        if (s1 == s2) {
            long id1 = this.friendId;
            long id2 = r.friendId();
            return Long.compare(id2, id1);
        }
        else
            return Integer.compare(s1, s2); // descending
    }
}
