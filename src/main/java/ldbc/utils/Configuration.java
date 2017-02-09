/**
 * A configuration utility for running this LDBC SNB benchmark.
 *
 * Copyright © 2017 Alain Kägi
 */

package ldbc.utils;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

/**
 * Define a configuration framework for microbenchmarking queries.
 *
 * A configuration file whose name is defined below must exist in the
 * root directory of this project.  Its structure must adhere to the
 * standard defined in java.util.Properties.  It must define a
 * "database" URL, a database "user" and her "password", a
 * "parameterFilesDirectory" holding query input parameter files, and
 * a "datasetDirectory" containing the merged generated dataset in
 * the CVS format.  Optional parameters are "measureLatency",
 * "beVerbose", and "explain".
 */
public class Configuration {

    private static final String configFilename = "params.ini";

    private boolean measureLatency;
    private boolean beVerbose;
    private boolean explain;
    private String database;
    private String user;
    private String password;
    private String parameterFilesDirectory;
    private String datasetDirectory;

    public Configuration() throws ConfigurationFileNotFoundException, ConfigurationIOException, MissingConfigurationException {
        try {
            Properties config = new Properties();
            config.load(new FileInputStream(configFilename));
            measureLatency = config.getProperty("measureLatency", "false").equals("true");
            beVerbose = config.getProperty("beVerbose", "false").equals("true");
            explain = config.getProperty("explain", "false").equals("true");
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

    public boolean beVerbose() { return beVerbose; }
    public boolean measureLatency() { return measureLatency; }
    public boolean explain() { return explain; }
    public String database() { return database; }
    public String user() { return user; }
    public String password() { return password; }
    public String parameterFilesDirectory() { return parameterFilesDirectory; }
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
