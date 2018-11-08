/*
 * Copyright © 2018 Alain Kägi
 */

package ldbc.helpers;

/**
 * The Query5SortResult class defines a structure to hold one query 5
 * result temporarily.
 *
 * <p>We accumulate query 5 results temporarily in a priority queue
 * derived from this class.  We use a priority queue so we can easily
 * identify unneeded elements.
 */
public class Query5SortResult implements Comparable<Query5SortResult> {
    private final long forumId;
    private final int count;

    /**
     * Construct a Query5SortResult object.
     * @param forumId The forum's unique identifier
     * @param count   Number of posts in the associated forum
     */
    public Query5SortResult(long forumId, int count) {
        this.forumId = forumId;
        this.count = count;
    }

    /**
     * Return the forum's identifier.
     * @return the forum's unique identifier
     */
    public long forumId() { return forumId; }

    /**
     * Return the number of posts created in the associated forum.
     * @return the number of posts created in the associated forum
     */
    public int count() { return count; }

    /**
     * Define a sort order for this class.
     *
     * <p>It is exactly the reverse of the intended order so we can
     * identify an unwanted element by its 'high' priority.
     *
     * @param r  result to be compared
     * @return a negative integer, zero, or a positive integer as this object is less than, equal to, or greater than the specified object
     */
    public int compareTo(Query5SortResult r) {
        int c1 = this.count;
        int c2 = r.count();
        if (c1 == c2) {
            long id1 = this.forumId;
            long id2 = r.forumId();
            return Long.compare(id2, id1);
        }
        else
            return Integer.compare(c1, c2); // descending
    }
}
