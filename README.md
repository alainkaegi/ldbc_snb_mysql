These source files represents a MySQL-based implementation of the LDBC
Social Network Benchmark's (SNB) Interactive Workload.  This
distribution draws from the Virtuoso SQL query implementation shown in
appendix A.2 of the LDBC report specifying the benchmark [1].  It
borrows also the print functions found in the Neo4j LDBC SNB sister
implementation [2].

Manifest
========

- `README.md`: this file
- `COPYING`: license
- `build.gradle`: build/run script
- `src`: Java source files
- `params.ini.sample`: sample configuration file

Expectations
============

This document assumes the reader is familiar with the LDBC Social
Network Benchmark.  Good sources of information include the LDBC
Social Network Benchmark specification [1] and the LDBC SNB Data
Generator project hosted at GitHub, Inc. [3].

Running modes
=============

This distribution supports three running modes: (1) you can run the
complex queries individually or in what I call standalone mode, (2)
run the official LDBC benchmark, or (3) run validation.

Standalone
----------

The standalone run mode might be useful for debugging or
microbenchmarking.  In that mode a query runs on a single thread and
processes input arguments as quickly as possible.  To run a complex
query in standalone mode require a substitution parameter-like file
(the same as the ones produced by the LDBC data generator with the
`PARAM_GENERATION` environment variable set to 1).  See below for
details.

Benchmark
---------

The benchmark run mode executes the official LDBC SNB benchmark.

Validation
----------

The LDBC SNB benchmark comes with instructions to validate a candidate
implementation.

Requirements
============

- The LDBC data generator [3]
- The LDBC benchmark driver [4]
- The LDBC validation project [5]
- The sister, Neo4j-based, LDBC SNB implementation [2] (for the script directory only)
- gradle version 3.4.1 or later
- java 1.8 or later
- mysql version 5.7.24 or later
- maven 3.5.2 or later
- hadoop 2.6.0 or later

Instructions
============

I tested these instructions on Ubuntu running Xenial Xerus (16.04).

These instructions assume that you cloned the various source packages
mentioned here (data generator [3], benchmark driver [4], benchmark
validation [5], my Neo4j's LDBC SNB implementation [2], and this
project) in the same directory.  They also assume that in most cases
each project generates data in their own subdirectories.  These
assumptions need not be met; it should be straightforward to choose a
different naming discipline.

Set up MySQL
------------

These steps assume that you have root access on your system and some
version of MySQL installed with access to MySQL's root user.  In
principle we use these root accesses only once to configure the system
and set up a less privileged user called 'ldbc' to run benchmarks or
validation.

- `sudo usermod -a -G mysql $USER`
  (must log out and back in to take effect)
- `sudo chmod g+rwx /var/lib/mysql-files/`
  (I had to repeat this step on occasion; perhaps after a software update?)
- add these lines to `/etc/mysql/my.cnf`:
```
[mysqld]
character-set-server = utf8
character-set-filesystem = utf8
collation-server = utf8_bin
default_time_zone = "+00:00"
```
- `sudo service mysql restart`
- enter the MySQL monitor (`mysql -u root -p`; you will be prompted
  for the MySQL root password) to create the user and password that we
  will use throughout these instructions.

Once in the MySQL monitor, enter these commands:

- `CREATE USER 'ldbc'@'localhost' IDENTIFIED BY '<password>';`
  (note the password for later use in these instructions)
- `CREATE DATABASE ldbc;`
- `GRANT ALL PRIVILEGES ON ldbc.* TO 'ldbc'@'localhost';`
- `GRANT FILE ON *.* TO 'ldbc'@'localhost';`

Generate the Dataset
--------------------

The LDBC validation project [5] comes with its own pre-generated
dataset.  For all other cases, you must first generate some data.

Generate a dataset using the LDBC SNB data generator [3] with the
scale factor of your choice, the `PARAM_GENERATION` environment
variable set to 1, and most other parameters left to their default
values.  In the end the goal is to have
`../ldbc_snb_datagen/social_network/` holding the CSV files to seed
the MySQL database and `../ldbc_snb_datagen/substitution_parameters/`
containing files used as input to the complex queries run in
standalone mode or to the LDBC benchmark.  You may have to issue a
command like `bin/hdfs dfs -get social_network/
../ldbc_snb_datagen/social_network/` to retrieve your data.

Prepare the Dataset
-------------------

The dataset may come with entities split across multiple files (either
a dataset you have generated or that has been supplied).  Use the
concatenation script distributed with my Neo4j-based LDBC SNB
implementation [2] to merge the results (e.g.,
`../ldbc_snb_neo4j/scripts/cat.sh ../ldbc_snb_datagen/social_network/
../ldbc_snb_datagen/social_network_merged/`).

Edit params.ini
---------------

This file is used for loading the database from a dataset (for running
either a microbenchmark, i.e., a complex read query, the LDBC
benchmark, or validation.  Start with the `params.ini.sample` provided
to create your own (i.e., `cp params.ini.sample params.ini`).  The
important parameters are a password to be used by the MySQL 'ldbc'
user to access the database (see the 'Set up MySQL' section above),
the location of the substitution parameter files, and the location of
the merged dataset.

Load the Dataset
----------------

Once `params.ini` is ready and pointing to the correct merged dataset,
you are ready to load said dataset into the database:

```
gradle load
```

This command destroy the previous content of the database and replaces
it with the content of the provided dataset.

Configure the LDBC Driver
-------------------------

The LDBC driver must be configured for either a benchmark run or
validation through files `ldbc.properties` or `validation.properties`,
respectively.  A good starting point for these files are
`../ldbc_driver/workloads/ldbc/snb/interactive/ldbc_snb_interactive_SF-0001.properties`
(or one of its sister files, depending on your choice of scale factor)
and
`../ldbc_snb_interactive_validation/neo4j/readwrite_neo4j--ldbc_driver_config--db_validation.properties`, respectively.  Copy these files into this
directory and rename them `ldbc.properties` and `validation.properties`.
These files have a number of fields in common.  In particular, they
both must specify these fields:

- 'database': change it to `ldbc.glue.MySQLDb`
- 'url': and and set it to `jdbc:mysql://localhost/ldbc`
- 'user': add and set it to `ldbc`
- 'password': add and set it to the value you picked when configuring MySQL (see above)

Run a Complex Query
-------------------

Once you have generated a dataset and loaded it into a database, you
are ready to run a complex read query:

```
gradle queryX
```

where X is between 1 and 14 inclusive.

Run the Benchmark
-----------------

Once you have generated a dataset and loaded it into a database, you
are also ready to run the LDBC benchmark.

Copy the property file corresponding to the chosen scale factor and
name it `ldbc.properties` (e.g., `cp
../ldbc_driver/workloads/ldbc/snb/interactive/ldbc_snb_interactive_SF-0001.properties
./ldbc.properties`).  Edit the file according to the instructions in
the 'Configure the LDBC Driver' section above.  Also set
'ldbc.snb.interactive.parameters_dir' to the query parameter files
produced by the data generation phase (e.g.,
`../ldbc_snb_datagen/substitution_parameters/`), set
'ldbc.snb.interactive.updates_dir' to the directory containing the
update stream files produced by the data generation phase (e.g.,
`../ldbc_snb_datagen/social_network/`), add 'operation_count' and set
it to the desired value.

Copy the update stream property file produced by the data generation
process (e.g, `cp
../ldbc_data_gen/social_network/updateStream.properties .`).

Now you can run the LDBC benchmark:

```
gradle ldbc
```

Important: you may need to reload the database before rerunning the
benchmark.

Run Validation
--------------

I validated this implementation following the directions given with
the LDBC SNB interactive validation project [5].  These steps are very
similar to those I followed when validating my Neo4j LDBC SNB
implementation [2].

Untar the content of
`ldbc_snb_interactive_validation/neo4j/neo4j--validation_set.tar.gz` in,
say, `../ldbc_snb_interactive_validation/neo4j/e/`.

Prepare and load the dataset as described in the 'Prepare the Dataset'
and 'Load the Dataset' sections using the CSV files in the
`string_date` subdirectory (`../ldbc_snb_neo4j/scripts/cat.sh
../ldbc_snb_interactive_validation/neo4j/e/social_network/string_date/
../ldbc_snb_interactive_validation/neo4j/e/social_network/string_date_merged/`
and set the 'datasetDirectory' parameter in `params.ini` to
`../ldbc_snb_interactive_validation/neo4j/e/social_network/string_date_merged`).

Copy the property file provided with that validation project and name
it validation.properties (e.g., `cp
../ldbc_snb_interactive_validation/neo4j/readwrite_neo4j--ldbc_driver_config--db_validation.properties
./validation.properties`).  Edit the LDBC driver configuration section
as described in the 'Configure the LDBC Driver' section.  Also set
'validate_database' to
`../ldbc_snb_interactive_validation/neo4j/e/validation_params.csv`,
and 'ldbc.snb.interactive.parameters_dir' to
'../ldbc_snb_interactive_validation/neo4j/e/substitution_parameters/'.

Now you are ready to run validation:

```
gradle validate
```

Important: you must reload the validation database from scratch every
time you run validation

Known Issues
============

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

[2] Alain Kägi.  A Neo4j-based Implementation of the LDBC SNB
    Benchmark. https://github.com/alainkaegi/ldbc_snb_neo4j.  Last
    retrieved October 11, 2016.

[3] LDBC SNB Data Generator.
    https://github.com/ldbc/ldbc_snb_datagen, last retrieved March
    18th, 2016, with, then, latest commit 2645cc0.

[4] LDBC SNB Benchmark Driver.  https://github.com/ldbc/ldbc_driver,
    last retrieved March 30th, 2016, version 0.2, commit 55f7ac0.

[5] LDBC SNB Interactive Validation Project.
    https://github.com/ldbc/ldbc_snb_interactive_validation, last
    retrieved April 5th, 2016, with then, the latest commit 03c34c0.
