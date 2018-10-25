/*
 * Copyright © 2017-2018 Alain Kägi
 */

package ldbc.queries;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Date;
import java.util.regex.MatchResult;
import java.util.regex.Pattern;
import java.util.Scanner;

/**
 * The QueryParameterFile class define functions to read a query
 * substitution parameter file used for microbenchmarking.
 *
 * A query substitution parameter file consists of lines separated by
 * newlines.  The first line is a header which is ignored.  The other
 * lines all follow a pattern defined by a regular expression passed
 * in to the constructor.
 *
 * The accessor functions return valid data only if the caller has
 * first called nextLine() and that function returned true.  The
 * accessor functions are not idempotent.  The caller must be aware of
 * the file structure (it defines the query substitution parameter
 * line pattern).  In other words, the caller must invoke an accessor
 * function only only once per item on each line and must invoke the
 * accessor function of the proper type for the next parameter to be
 * read.
 */
class QueryParameterFile {

    private String queryParameterFilename;
    private Scanner queryParameterFileScanner;
    private Pattern queryParameterLinePattern;
    private MatchResult currentMatch;
    private int currentGroup;

    /**
     * Construct a query substitution parameter file object.
     * @param queryParameterFilename     Name of the file containing a set of substitution parameters
     * @param queryParameterLinePattern  Regular expression describing a line of the parameter file
     * @throws QueryParameterFileNotFoundException if the parameter file is nout found
     */
    public QueryParameterFile(String queryParameterFilename, String queryParameterLinePattern) throws QueryParameterFileNotFoundException {
        this.queryParameterFilename = queryParameterFilename;
        this.queryParameterFileScanner = openFileAndSkipHeader(queryParameterFilename);
        this.queryParameterLinePattern = Pattern.compile(queryParameterLinePattern);
    }

    /**
     * Read the next line of query substitution parameters.
     * @return false if the scanner has reached the end of file
     */
    public boolean nextLine() {
        if (queryParameterFileScanner.findInLine(queryParameterLinePattern) == null)
            return false;
        currentMatch = queryParameterFileScanner.match();
        if (queryParameterFileScanner.hasNextLine()) queryParameterFileScanner.nextLine();
        currentGroup = 1;
        return true;
    }

    /**
     * Scan the next token of the parameter file as an int
     * @return the scanned int
     */
    public int getInt() {
        return Integer.valueOf(currentMatch.group(currentGroup++));
    }

    /**
     * Scan the next token of the parameter file as a long
     * @return the scanned long
     */
    public long getLong() {
        return Long.valueOf(currentMatch.group(currentGroup++));
    }

    /**
     * Scan the next token of the parameter file as a long interpreted as a Date
     * @return the scanned Date
     */
    public Date getDate() {
        return new Date(Long.valueOf(currentMatch.group(currentGroup++)));
    }

    /**
     * Scan the next token of the parameter file as a String
     * @return the scanned String
     */
    public String getString() {
        return currentMatch.group(currentGroup++);
    }

    private Scanner openFileAndSkipHeader(String filename) throws QueryParameterFileNotFoundException {
        try {
            Scanner scanner = new Scanner(new File(filename));
            scanner.nextLine();
            return scanner;
        }
        catch (FileNotFoundException e) {
            throw new QueryParameterFileNotFoundException(queryParameterFilename + ": No such query parameter input file");
        }
    }

    /** Define an exception to be thrown when the query input parameter file is not found. */
    public class QueryParameterFileNotFoundException extends Exception {

        public QueryParameterFileNotFoundException(String message) { super(message); }
        public QueryParameterFileNotFoundException(String message, Throwable throwable) { super(message, throwable); }

    }

}
