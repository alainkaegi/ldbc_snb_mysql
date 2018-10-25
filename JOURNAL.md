2018-09-29

Running complex query 1 as a microbenchmark eventually throws an
OutOfMemoryError exception: the program runs out of heap space.  If I
print heap usage after the processing of each line of the parameter
file, I note that said usage always strictly increases.  However, if
query 1 releases (calling the close() method), as soon as possible,
Statement and ResultSet instances used in its main body, it no longer
runs out of space and usage seesaws, as you would expect.  As a
precaution, LdbcUtils functions now also call the close() method as
quickly as possible although it does not appear necessary to do so (at
least from the view point of query 1).
