# Univeral Relational JDBC driver

A driver that can do remote connection to the Relational Database OR
start an in-process Relational dependent on JDBC URL passed.

This module differs from fdb-relational-jdbc in that it includes jdbc and server
dependencies so it can standup an inprocess server if the JDBC URL specifies
it: i.e. `jdbc:relational:///__SYS`, a URL w/o a server or port specified.

For example, if you put the universal jdbc driver under `sqlline` target directory
so it is available on the `sqlline` `CLASSPATH`, you can do as follows:
```
$ ./bin/sqlline -u jdbc:relational:///__SYS?schema=CATALOG -n "" -p "" --verbose
issuing: !connect -p user "" -p password "" "jdbc:relational:///__SYS?schema=CATALOG"
Connecting to jdbc:relational:///__SYS?schema=CATALOG
Connected to: Relational Database (version 23.03.22.4a5e609-SNAPSHOT)
Driver: Relational JDBC Driver (version 23.03.22.4a5e609-SNAPSHOT)
Autocommit status: true
Transaction isolation: TRANSACTION_REPEATABLE_READ
sqlline version 1.11.0
0: jdbc:relational:///__SYS> select * from databases ;
+-------------+
| DATABASE_ID |
+-------------+
| /FRL/YCSB   |
| /__SYS      |
+-------------+
2 rows selected (0.4 seconds)
0: jdbc:relational:///__SYS> select * from schemas ;
+-------------+-------------+------------------+------------------+
| DATABASE_ID | SCHEMA_NAME |  TEMPLATE_NAME   | TEMPLATE_VERSION |
+-------------+-------------+------------------+------------------+
| /FRL/YCSB   | YCSB        | YCSB_TEMPLATE    | 1                |
| /__SYS      | CATALOG     | CATALOG_TEMPLATE | 1                |
+-------------+-------------+------------------+------------------+
2 rows selected (0.031 seconds)
0: jdbc:relational:///__SYS>
```
And so on.

## Building the JDBC Driver 'Fat Jar'
To build the fat JDBC Driver jar with all dependencies included, invoke the 'shadowJar' task as in:
```
 ./gradlew -p fdb-relational-jdbc shadowJar
```
This will build a `fdb-relational-jdbc` jar with a `driver.jar` suffix into the `fdb-relational-jdbc/.dist/` directory.

The jar will have the classes from fdb-relational-jdbc, fdb-relational-api, fdb-relational-grpc,
and then dependencies such as grpc, netty, and protobuf all built in as well
as fdb-relational-server and fdb-relational-core included (so as to support inprocess access).
