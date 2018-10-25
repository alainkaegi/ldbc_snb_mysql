/*
 * Copyright © 2018 Alain Kägi
 */

package ldbc.helpers;

/**
 * The Query3Counts class defines a structure to keep tracks of
 * friends having visited countries X and Y.
 */
public class Query3Counts {
    private int countryXCount;
    private int countryYCount;

    /**
     * Construct a Query3Counts object.
     * @param xCount  Number of messages created in country X
     * @param yCount  Number of messages created in country Y
     */
    public Query3Counts(int xCount, int yCount) {
        this.countryXCount = xCount;
        this.countryYCount = yCount;
    }

    /** Increment the number of messages created in country X. */
    public void incXCount() { ++this.countryXCount; }

    /** Increment the number of messages created in country Y. */
    public void incYCount() { ++this.countryYCount; }

    /**
     * Get the number of messages created in country X.
     * @return the number of messages created in country X.
     */
    public int getXCount() { return this.countryXCount; }

    /**
     * Get the number of messages created in country Y.
     * @return the number of messages created in country Y.
     */
    public int getYCount() { return this.countryYCount; }
}
