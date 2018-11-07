These source files represents a MySQL-based implementation of a subset
of the LDBC Social Network Benchmark's Interactive Workload.  This
distribution draws heavily from the Virtuoso SQL query implementation
shown in appendix A.2 of the LDBC report specifying the benchmark [1].
It borrows also the print functions found in the Neo4j LDBC SNB sister
implementation [2].

Manifest
========

- README.md: this file
- COPYING: license
- build.gradle: build/run script
- src: Java source files

Expectations
============

This document assumes the reader is familiar with the LDBC Social
Network Benchmark.  Good sources of information include the LDBC
Social Network Benchmark specification [1] and the LDBC SNB Data
Generator project hosted at GitHub, Inc. [3].

Running mode
============

This distribution supports only one running mode for now.  You can run
the complex queries individually or in what I call standalone mode.
That mode might be useful for debugging or microbenchmarking.  In that
mode a query runs on a single thread and processes input arguments as
quickly as possible.  To run a complex query in standalone
mode require a substitution parameter-like file (the same as the ones
produced by the LDBC data generator with the PARAM_GENERATION
environment variable set to 1).  See below for details.

Requirements
============

- The sister, Neo4j-based, LDBC SNB implementation (for the script directory only)
- gradle version 2.10 or later
- java 1.7 or later
- mysql version 5.5.29 or later


Instructions
============

I tested these instructions on Ubuntu running Xenial Xerus (16.04).

These instructions assume that you cloned the various source packages
mentioned here (data generator, my Neo4j's LDBC SNB implementation,
and this project) in the same directory.  They also assume that in
most cases each project generates data in their own subdirectories.
These assumptions not need to be the case; it should be
straightforward to choose a different naming discipline.

Set up MySQL
------------
- sudo usermod -a -G mysql $USER
  (must log out and back in to take effect)
- sudo chmod g+rwx /var/lib/mysql-files/
  (I had to repeat this step on occasion; perhaps after a software update?)
- add these lines to /etc/mysql/my.cnf:
```
[mysqld]
character-set-server = utf8
character-set-filesystem = utf8
default_time_zone = "+00:00"
```
- restart the mysql server
- CREATE USER 'ldbc'@'localhost' IDENTIFIED BY '<password>';
  (note the password for later use in these instructions)
- CREATE DATABASE ldbc;
- GRANT ALL PRIVILEGES ON ldbc.* TO 'ldbc'@'localhost';
- GRANT FILE ON *.* TO 'ldbc'@'localhost';

Generate the Dataset
--------------------

Generate a dataset using the LDBC SNB data generator [3] with the
scale factor of your choice, the PARAM_GENERATION environment variable
set to 1, and most other parameters left to their default values.  In
the end the goal is to have ../ldbc_snb_datagen/social_network/
holding the CSV files to seed the benchmark graph database and
../ldbc_snb_datagen/substitution_parameters/ containing files used as
input to the LDBC benchmark or to the complex queries run in
standalone mode.  You may have to issue a command like bin/hdfs dfs
-get social_network/ ../ldbc_snb_datagen/social_network/ to retrieve
your data.

Prepare the Dataset
-------------------

The data generator mentioned in the previous section may split
entities across multiple files.  Use concatenation script distributed
with my Neo4j-based LDBC SNB implementation [2] to merge the results
(e.g., ../ldbc_snb_neo4j/scripts/cat.sh
../ldbc_snb_datagen/social_network/
../ldbc_snb_datagen/social_network_merged/).

Edit params.ini
---------------

Start with the params.ini.sample provided to create your own
params.ini.  The important parameters are a password to be used by the
MySQL 'ldbc' user to access the database, the location of the
substitution parameter files, and the location of the merged dataset.

Load the Dataset
----------------

gradle load

Run a Complex Query
-------------------

gradle queryX, where X is between 1 and 14 inclusive.

Validation
==========

I validated this implementation following the directions given with
the LDBC SNB interactive validation project [4].  These steps are very
similar to those I followed when validating my Neo4j LDBC SNB
implementation [2].

1 Clone the project LDBC SNB interactive validation project in
  ../ldbc_snb_interactive_validation/

2 Untar the content of
  ldbc_snb_interactive_validation/neo4j/neo4j--validation_set.tar.gz
  in, say, ../ldbc_snb_interactive_validation/neo4j/e/

3 Concatenate the CSV content of
  ldbc_snb_interactive_validation/neo4j/e/social_network/string_date/
  into
  ../ldbc_snb_interactive_validation/neo4j/e/social_network/string_date_merged/
  (e.g., ./scripts/cat.sh
  ../ldbc_snb_interactive_validation/neo4j/e/social_network/string_date/
  ../ldbc_snb_interactive_validation/neo4j/e/social_network/string_date_merged/)

4 Load the merged CSV files into the MySQL ldbc database; use the same
  params.ini file as for microbenchmarking except for changing the
  'datasetDirectory' parameter to
  '../ldbc_snb_interactive_validation/neo4j/e/social_network/string_date_merged'

5 Copy the property file provided with that project and name it
  validation.properties (e.g., cp
  ../ldbc_snb_interactive_validation/neo4j/readwrite_neo4j--ldbc_driver_config--db_validation.properties
  ./validation.properties); edit the LDBC driver configuration section
  of validation.properties; set 'database' to 'ldbc.glue.MySQLDb',
  'validate_database' to
  '../ldbc_snb_interactive_validation/neo4j/e/validation_params.csv',
  and 'ldbc.snb.interactive.parameters_dir' to
  '../ldbc_snb_interactive_validation/neo4j/e/substitution_parameters/';
  add fields 'url', 'user' and 'password'; set 'url' to
  'jdbc:mysql://localhost/ldbc', and set 'user' and 'password' to the
  same values specified in params.ini

6 Issue the command 'gradle validate'

7 Important: you must reload the validation database from scratch
  (step 4) every time you run validation

Known Issues
============

The sort order for complex query 4 and 6 is slightly different than my
Neo4j-based LDBC SNB implementation.

The LDBC data generator produces strings longer than promised by the
benchmark specification [1].  In particular I have found that some
forum titles exceed size 40, some message content exceed 2000, some
place names exceed size 40, some tag names exceed size 40, some email
addresses exceed size 40, and organization's names exceed 40.  I have
used sizes 100, 2100, 100, 100, 80, and 150 respectively.

References
==========

[1] Arnau Prat (Editor).  LDBC Social Network Benchmark (SNB), v0.2.2
    First Public Draft, release 0.2.2, last retrieved December 17,
    2015.

[2] Alain Kägi.  An Implementation of the LDBC SNB
    Benchmark. https://github.com/alainkaegi/ldbc_snb_neo4j.  Last
    retrieved October 11, 2016.

[3] LDBC SNB Data Generator.
    https://github.com/ldbc/ldbc_snb_datagen, last retrieved March
    18th, 2016, with, then, latest commit 2645cc0.
