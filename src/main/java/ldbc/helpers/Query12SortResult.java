/*
 * Copyright © 2018 Alain Kägi
 */

package ldbc.helpers;

/**
 * The Query12SortResult class defines a structure to hold one query
 * 12 result temporarily.
 *
 * <p>We accumulate query 12 results temporarily in a priority queue
 * derived from this class.  We use a priority queue so we can easily
 * identify unneeded elements.
 */
public class Query12SortResult implements Comparable<Query12SortResult> {
    private final long friendId;
    private final int replyCount;

    /**
     * Construct a Query12SortResult object.
     * @param friendId    The friend's unique identifier
     * @param replyCount  The number of replies made by this friend
     */
    public Query12SortResult(long friendId, int replyCount) {
        this.friendId = friendId;
        this.replyCount = replyCount;
    }

    /**
     * Return the friend's identifier.
     * @return the friend's unique identifier
     */
    public long friendId() { return friendId; }

    /**
     * Return the friend's reply count.
     * @return the friend's number of replies
     */
    public int replyCount() { return replyCount; }

    /**
     * Define a sort order for this class.
     *
     * <p>It is exactly the reverse of the intended order so we can
     * identify an unwanted element by its 'high' priority.
     *
     * @param r  result to be compared
     * @return a negative integer, zero, or a positive integer as this object is less than, equal to, or greater than the specified object
     */
    public int compareTo(Query12SortResult r) {
        int c1 = this.replyCount;
        int c2 = r.replyCount();
        if (c1 == c2) {
            long id1 = this.friendId;
            long id2 = r.friendId();
            return Long.compare(id2, id1);
        }
        else
            return Integer.compare(c1, c2); // descending
    }
}
