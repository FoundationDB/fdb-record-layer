# Relational-Core

## <a name="embedded"></a>The Embedded JDBC Driver: `url=jdbc:embed:/__SYS?schema=CATALOG`
Relational core bundles a JDBC Driver that offers direct-connect on a Relational instance.
The Driver class name, `com.apple.foundationdb.relational.jdbc.JDBCRelationalDriver`, is registered with
[Service Loader](https://docs.oracle.com/javase/8/docs/api/java/util/ServiceLoader.html).

JDBC URLs have a `jdbc:embed:` prefix. An URL that connects to the Relational
system database, `__SYS`, using the system database `CATALOG` would look like this:
```
jdbc:embed:/__SYS?schema=CATALOG
```
Connecting to the `/FRL/YCSB` database using the `ycsb` schema:
```
jdbc:embed:/FRL/YCSB?schema=YCSB
```

There is no one 'fat' convenience driver jar that bundles all dependencies built
currently. Access to the fdb client native library complicates our being able to
deliver a single `driver` jar (TODO).

The [fdb-relational-cli](../fdb-relational-cli/README.md) comes with the embedded Driver bundled.

Relational also bundles a JDBC client Driver that talks GRPC to a Relational
Server/Service. See [Relational JDBC client Driver](../fdb-relational-jdbc/README.md)
for more.