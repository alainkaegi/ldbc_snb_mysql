/*
 * Copyright © 2018 Alain Kägi
 */

package ldbc.helpers;

import java.util.Set;
import java.util.TreeSet;

/**
 * A temporary holder of one query 12 partial result.
 */
public class Query12PartialResult {
    private final Set<Long> tags;
    private int replyCount;

    /**
     * Construct a Query12PartialResult object.
     *
     * <p>We create an instance of this class when we process the
     * first reply created by a particular friend.  We store the
     * associated tags in a set and seed the said set with the given
     * tag.  Also, we set the reply count to 1.
     *
     * @param tagId      A tag's unique identifier
     */
    public Query12PartialResult(long tagId) {
        this.tags = new TreeSet<>();
        this.tags.add(tagId);
        this.replyCount = 1;
    }

    /**
     * Return the number of replies made by this friend.
     * @return the number of replies made by this friend
     */
    public int replyCount() { return replyCount; }

    /**
     * Return the tags associated to posts to which this friend replied.
     * @return the set of tags associated to posts to which this friend replied
     */
    public Set<Long> tags() { return tags; }

    /**
     * Increment the number of replies made by this friend.
     */
    public void incReplyCount() { ++this.replyCount; }

    /**
     * Add a tag to those associated to posts to which this friend replied.
     * @param tagId  The additional tag
     */
    public void addTag(long tagId) { this.tags.add(tagId); }
}
