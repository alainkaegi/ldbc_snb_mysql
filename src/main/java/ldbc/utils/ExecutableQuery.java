/*
 * Copyright © 2017 Alain Kägi
 */

package ldbc.queries;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * The ExecutableQuery interface defines functions that a query must
 * implement so we can microbenchmark it.
 */
interface ExecutableQuery {

    void executeQuery(Connection db, QueryParameterFile queryParameters, boolean beVerbose, boolean printHeapUsage) throws SQLException;
    void explainQuery(Connection db, QueryParameterFile queryParameters) throws SQLException;

}
