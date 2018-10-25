/*
 * Copyright © 2018 Alain Kägi
 */

package ldbc.helpers;

/**
 * The Query3SortResult class defines a structure to hold one query 3
 * result temporarily.
 *
 * <p>We accumulate query 3 results temporarily in a priority queue
 * derived from this class.  We use a priority queue so we can easily
 * identify unneeded elements.
 */
public class Query3SortResult implements Comparable<Query3SortResult> {
    private final long friendId;
    private final String friendFirstName;
    private final String friendLastName;
    private final long xCount;
    private final long yCount;

    /**
     * Construct a Query3SortResult object.
     * @param friendId         The friend's unique identifier
     * @param friendFirstName  Their first name
     * @param friendLastName   Their last name
     * @param xCount           Number of messages from country X made by this friend
     * @param yCount           Number of messages from country Y made by this friend
     */
    public Query3SortResult(long friendId, String friendFirstName, String friendLastName, long xCount, long yCount) {
        this.friendId = friendId;
        this.friendFirstName = friendFirstName;
        this.friendLastName = friendLastName;
        this.xCount = xCount;
        this.yCount = yCount;
    }

    /**
     * Return the friend's identifier.
     * @return the friend's unique identifier
     */
    public long friendId() { return friendId; }

    /**
     * Return the friend's first name.
     * @return the friend's first name
     */
    public String friendFirstName() { return friendFirstName; }

    /**
     * Return the friend's last name.
     * @return the friend's last name
     */
    public String friendLastName() { return friendLastName; }

    /**
     * Return the number of message from country X made by this friend.
     * @return the number of message from country X made by this friend
     */
    public long xCount() { return xCount; }

    /**
     * Return the number of message from country Y made by this friend.
     * @return the number of message from country Y made by this friend
     */
    public long yCount() { return yCount; }

    /**
     * Define a sort order for this class.
     *
     * <p>It is exactly the reverse of the intended order so we can
     * identify an unwanted element by its 'high' priority.
     *
     * @param r  result to be compared
     * @return a negative integer, zero, or a positive integer as this object is less than, equal to, or greater than the specified object
     */
    public int compareTo(Query3SortResult r) {
        long c1 = this.xCount + this.yCount;
        long c2 = r.xCount() + r.yCount();
        if (c1 == c2) {
            long id1 = this.friendId;
            long id2 = r.friendId();
            return Long.compare(id2, id1);
        }
        else
            return Long.compare(c1, c2); // descending
    }
}
