/*
 * Copyright © 2017-2019 Alain Kägi
 */

package ldbc.queries;

import com.zaxxer.hikari.HikariDataSource;

import java.sql.SQLException;

/**
 * The ExecutableQuery interface defines functions that a query must
 * implement so we can microbenchmark it.
 */
interface ExecutableQuery {

    void executeQuery(HikariDataSource ds, QueryParameterFile queryParameters, boolean beVerbose, boolean printHeapUsage) throws SQLException;
    void explainQuery(HikariDataSource ds, QueryParameterFile queryParameters) throws SQLException;

}
