# Univeral Relational JDBC driver

Can do inprocess connection to the Relational database or remote access.
This module differs from fdb-relational-jdbc in that it includes jdbc and server
dependencies so it can launch inprocess server if the JDBC URL specifies
inprocess access (`jdbc:relational:///__SYS`).

## Building the JDBC Driver 'Fat Jar'
To build the fat JDBC Driver jar with all dependencies included, invoke the 'shadowJar' task as in:
```
 ./gradlew -p fdb-relational-jdbc shadowJar
```
This will build a `fdb-relational-jdbc` jar with a `driver.jar` suffix into the `fdb-relational-jdbc/.dist/` directory.

The jar will have the classes from fdb-relational-jdbc, fdb-relational-api, fdb-relational-grpc,
and then dependencies such as grpc, netty, and protobuf all built in as well
as fdb-relational-server and fdb-relational-core included (so as to support inprocess access).
