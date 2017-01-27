/**
 * A wrapper to execute an LDBC SNB query on a MySQL database.
 *
 * Copyright © 2017 Alain Kägi
 */

package ldbc.queries;

import java.sql.Connection;
import java.sql.SQLException;

import ldbc.utils.Db;
import ldbc.utils.Configuration;

/** A helper class to run query microbenchmarks. */
public class MicroBenchmark {

    /**
     * Execute a query, once per parameter line read from a file.
     * @param query  Essentially a handle to a per-query defined executeQueries function
     * @param queryName  The name of the query; used for printing messages
     * @param queryParameterFilename  The source of the input query parameters
     * @param queryParameterFileLinePattern  A regular expression describing one line of parameter input
     * This is the high-level function for running a microbenchmark on
     * a given query.  It handles configuration, initialization, and
     * error handling.
     */
    public static void executeQueriesWithParametersFromFile(ExecutableQuery query, String queryName, String queryParameterFilename, String queryParameterFileLinePattern) {

        try {

            Configuration config = new Configuration();

            String url = "jdbc:mysql://localhost/" + config.database();
            String parameterFQN = config.parameterFilesDirectory() + "/" + queryParameterFilename;

            try {

                Connection db = Db.connect(url, config.user(), config.password());
                doExecuteQueriesWithParametersFromFile(query, db, parameterFQN, queryParameterFileLinePattern, config.measureLatency(), config.beVerbose());

            }
            catch (QueryParameterFile.QueryParameterFileNotFoundException e) {
                System.err.println(queryName + ": " + e.getMessage());
                System.exit(1);
            }
            catch (SQLException e) {
                System.err.println(queryName + ": " + config.database() + ": " + e.getMessage());
                System.exit(1);
            }

        }
        catch (Configuration.ConfigurationFileNotFoundException e) {
            System.err.println(queryName + ": " + e.getMessage());
            System.exit(1);
        }
        catch (Configuration.ConfigurationIOException e) {
            System.err.println(queryName + ": " + e.getMessage());
            System.exit(1);
        }
        catch (Configuration.MissingConfigurationException e) {
            System.err.println(queryName + ": " + e.getMessage());
            System.exit(1);
        }

    }

    /**
     * Execute a query, once per parameter line read from a file.
     * @param query  Essentially a handle to a per-query defined executeQueries function
     * @param db
     * @param queryParameterFilename  The source of the input query parameters
     * @param queryParameterFileLinePattern  A regular expression describing one line of parameter input
     * This is the lower-level function.  We time the run here.
     */
    private static void doExecuteQueriesWithParametersFromFile(ExecutableQuery query, Connection db, String queryParameterFilename, String queryParameterFileLinePattern, boolean measureLatency, boolean beVerbose) throws QueryParameterFile.QueryParameterFileNotFoundException, SQLException {
        QueryParameterFile queryParameters = new QueryParameterFile(queryParameterFilename, queryParameterFileLinePattern);

        Timer timer = new Timer();
        timer.start();

        query.executeQueries(db, queryParameters, beVerbose);

        timer.stop();

        if (measureLatency)
            timer.print();
    }

}
