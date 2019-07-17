/*
 * Copyright © 2017-2019 Alain Kägi
 */

package ldbc.utils;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

/**
 * This class defines a configuration framework to run microbenchmarks
 * derived from the LDBC Social Network Benchmark (SNB).
 *
 * <p>A configuration file named <tt>params.ini</tt> must exist in the
 * root directory of this project.  Its structure must adhere to the
 * standard defined in <tt>java.util.Properties</tt>.  It must define
 * a "database" URL, a database "user" and her "password", a
 * "parameterFilesDirectory" holding query input parameter files, and
 * a "datasetDirectory" containing the merged generated dataset in the
 * CSV format.  Optional parameters are "beVerbose", "measureLatency",
 * "printHeapUsage", and "explain".
 */
public class Configuration {

    private static final String configFilename = "params.ini";

    // Optional properties.
    private boolean beVerbose;
    private boolean measureLatency;
    private boolean printHeapUsage;
    private boolean explain;

    // Required properties.
    private String host;
    private String port;
    private String database;
    private String user;
    private String password;
    private String parameterFilesDirectory;
    private String datasetDirectory;

    /**
     * Construct a configuration object.
     * @throws ConfigurationFileNotFoundException if params.ini is not found
     * @throws ConfigurationIOException if a problem occurs while reading params.ini
     * @throws MissingConfigurationException if a required property if missing
     */
    public Configuration() throws ConfigurationFileNotFoundException, ConfigurationIOException, MissingConfigurationException {
        try {
            Properties config = new Properties();
            config.load(new FileInputStream(configFilename));
            beVerbose = config.getProperty("beVerbose", "false").equals("true");
            measureLatency = config.getProperty("measureLatency", "false").equals("true");
            printHeapUsage = config.getProperty("printHeapUsage", "false").equals("true");
            explain = config.getProperty("explain", "false").equals("true");
            if ((host = config.getProperty("host")) == null) throw new MissingConfigurationException(configFilename + ": host: No such field defined");
            if ((port = config.getProperty("port")) == null) throw new MissingConfigurationException(configFilename + ": port: No such field defined");
            if ((database = config.getProperty("database")) == null) throw new MissingConfigurationException(configFilename + ": database: No such field defined");
            if ((user = config.getProperty("user")) == null) throw new MissingConfigurationException(configFilename + ": user: No such field defined");
            if ((password = config.getProperty("password")) == null) throw new MissingConfigurationException(configFilename + ": password: No such field defined");
            if ((parameterFilesDirectory = config.getProperty("parameterFilesDirectory")) == null) throw new MissingConfigurationException(configFilename + ": parameterFilesDirectory: No such field defined");
            if ((datasetDirectory = config.getProperty("datasetDirectory")) == null) throw new MissingConfigurationException(configFilename + ": datasetDirectory: No such field defined");
        }
        catch (FileNotFoundException e) {
            throw new ConfigurationFileNotFoundException(configFilename + ": No such file");
        }
        catch (IOException e) {
            throw new ConfigurationIOException(configFilename + ": IO exception while reading");
        }
    }

    /**
     * Should we be verbose?
     * @return true if we must be verbose
     */
    public boolean beVerbose() { return beVerbose; }

    /**
     * Should we measure execution times?
     * @return true if we must measure execution times
     */
    public boolean measureLatency() { return measureLatency; }

    /**
     * Should we report a query execution plan?
     * @return true if we must report a query execution plan
     */
    public boolean explain() { return explain; }

    /**
     * Should we report heap usage?
     * @return true if we must report heap usage
     */
    public boolean printHeapUsage() { return printHeapUsage; }

    /**
     * A host for the database.
     * @return a name or IP address for the host
     */
    public String host() { return host; }

    /**
     * A port to access the database.
     * @return a port number to access the database
     */
    public String port() { return port; }

    /**
     * A name for the database.
     * @return a name for the database
     */
    public String database() { return database; }

    /**
     * The name of a user with sufficient privileges to access and to modify the associated database.
     * @return a username to access the associated database
     */
    public String user() { return user; }

    /**
     * The password of the user to access the database.
     * @return the password associated with the username
     */
    public String password() { return password; }

    /**
     * The location of the substitution parameter files to microbenchmark the database queries defined in the LDBC Social Network Benchmark (SNB).
     * @return the location of the LDBC SNB substitution parameters
     */
    public String parameterFilesDirectory() { return parameterFilesDirectory; }

    /**
     * The location of the (merged) dataset with which to load a database to be used with the LDBC Social Network Benchmark (SNB).
     * @return the location of the LDBC SNB dataset to seed the database
     */
    public String datasetDirectory() { return datasetDirectory; }

    /** Define an exception to be thrown when the configuration file is not found. */
    public class ConfigurationFileNotFoundException extends Exception {

        public ConfigurationFileNotFoundException(String message) { super(message); }
        public ConfigurationFileNotFoundException(String message, Throwable throwable) { super(message, throwable); }

    }

    /** Define an exception to be thrown if a problem occurs while reading the configuration file. */
    public class ConfigurationIOException extends Exception {

        public ConfigurationIOException(String message) { super(message); }
        public ConfigurationIOException(String message, Throwable throwable) { super(message, throwable); }

    }

    /** Define an exception to be thrown if a required configuration item is missing. */
    public class MissingConfigurationException extends Exception {

        public MissingConfigurationException(String message) { super(message); }
        public MissingConfigurationException(String message, Throwable throwable) { super(message, throwable); }

    }

}
