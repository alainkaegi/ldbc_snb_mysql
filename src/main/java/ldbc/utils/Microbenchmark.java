/*
 * Copyright © 2017-2018 Alain Kägi
 */

package ldbc.queries;

import com.zaxxer.hikari.HikariDataSource;

import java.sql.SQLException;

import ldbc.glue.MySQLDbConnectionState;
import ldbc.utils.Db;
import ldbc.utils.Configuration;

/**
 * The Microbenchmark class defines static functions to help run the
 * various LDBC Social Network Benchmark (SNB) queries as
 * microbenchmarks.
 */
public class Microbenchmark {

    // Suppress the default constructor.
    private Microbenchmark() {}

    /**
     * Execute a query, once per parameter line read from a file.
     *
     * <p>This is the high-level function for running a microbenchmark on
     * a given query.  It handles configuration, initialization, and
     * error handling.
     * @param query  A function that executes queries with input from the substitution parameters
     * @param queryName  The name of the query; used for printing messages
     * @param queryParameterFilename  The name of the file (relative to the directory specified in the configuration file) holding the input query substitution parameters
     * @param queryParameterFileLinePattern  A regular expression describing one line of parameter input
     */
    public static void executeQueryWithParametersFromFile(ExecutableQuery query, String queryName, String queryParameterFilename, String queryParameterFileLinePattern) {

        try {
            Configuration config = new Configuration();

            String url = "jdbc:mysql://" + config.host() + ":" + config.port() + "/" + config.database();
            String parameterFQN = config.parameterFilesDirectory() + "/" + queryParameterFilename;

            MySQLDbConnectionState state = new MySQLDbConnectionState(url, config.user(), config.password());

            HikariDataSource ds = state.getClient();

            try {
                if (config.explain())
                    doExplainQueryWithParametersFromFile(query, ds, parameterFQN, queryParameterFileLinePattern);
                else
                    doExecuteQueryWithParametersFromFile(query, ds, parameterFQN, queryParameterFileLinePattern, config.measureLatency(), config.printHeapUsage(), config.beVerbose());

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
     * Do execute a query, once per parameter line read from a file.
     * @param query  A function that executes queries with input from the substitution parameters
     * @param ds  A database source
     * @param queryParameterQFilename  The qualified location of the input query subsitution parameters
     * @param queryParameterFileLinePattern  A regular expression describing one line of substitution parameter input
     * @param measureLatency  Report execution time if requested
     * @param printHeapUsage  Print heap usage if requested
     * @param beVerbose  Output the result of each query if requested
     * @throws QueryParameterFileNotFoundException if the parameter file is not found
     * @throws SQLException if a database access error occurs
     * This is the lower-level function.  We time the run here.
     */
    private static void doExecuteQueryWithParametersFromFile(ExecutableQuery query, HikariDataSource ds, String queryParameterQFilename, String queryParameterFileLinePattern, boolean measureLatency, boolean printHeapUsage, boolean beVerbose) throws QueryParameterFile.QueryParameterFileNotFoundException, SQLException {
        QueryParameterFile queryParameters = new QueryParameterFile(queryParameterQFilename, queryParameterFileLinePattern);

        Timer timer = new Timer();
        timer.start();

        query.executeQuery(ds, queryParameters, beVerbose, printHeapUsage);

        timer.stop();

        if (measureLatency)
            timer.print(System.out);

    }

    /**
     * Do explain a query using the first parameter line read from a file.
     * @param query  A function that prints the query execution plan
     * @param ds  A database source
     * @param queryParameterQFilename  The qualified location of the input query substitution parameters
     * @param queryParameterFileLinePattern  A regular expression describing one line of substitution parameter input
     * @throws QueryParameterFileNotFoundException if the parameter file is not found
     * @throws SQLException if a database access error occurs
     */
    private static void doExplainQueryWithParametersFromFile(ExecutableQuery query, HikariDataSource ds, String queryParameterQFilename, String queryParameterFileLinePattern) throws QueryParameterFile.QueryParameterFileNotFoundException, SQLException {
        QueryParameterFile queryParameters = new QueryParameterFile(queryParameterQFilename, queryParameterFileLinePattern);

        query.explainQuery(ds, queryParameters);
    }

}
