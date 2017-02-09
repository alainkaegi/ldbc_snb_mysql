/**
 * A common interface to execute queries.
 *
 * Copyright © 2017 Alain Kägi
 */

package ldbc.queries;

import java.sql.Connection;
import java.sql.SQLException;

/** A query must define the function defined in this interface to fit in this microbenchmarking framework. */
interface ExecutableQuery {

    void executeQuery(Connection db, QueryParameterFile queryParameters, boolean beVerbose) throws SQLException;
    void explainQuery(Connection db, QueryParameterFile queryParameters) throws SQLException;

}
