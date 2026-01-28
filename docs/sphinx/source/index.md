# FoundationDB Record Layer
  
Welcome to the documentation for the FoundationDB Record Layer.

The Record Layer is a Java library providing a record-oriented store on top of
[FoundationDB](https://www.foundationdb.org), supporting structured records with
fields and types, schema evolution, complex primary and secondary indexes, 
and declarative query execution.  The Record Layer also includes features not 
typically found in a traditional relational database, such as support for 
complex nested data types, indexes on the commit-time of records, and indexes 
and queries that span different types of records.

The Record Layer is stateless, so scaling up is as simple as launching more instances.
It is designed for massive multi-tenancy, encapsulating and isolating all of a tenant's
state, and providing the ability to tightly constrain and balance resource consumption
across users in a predictable fashion, even in the face of a wide variety of workloads
and activity. Together, these properties enable the Record Layer to scale elastically
as the demands of its clients grow.

Sitting on top of FoundationDB, the Record Layer inherits its strong ACID semantics,
reliability, and performance in a distributed setting.

## Documentation

```{toctree}
:maxdepth: 1
Overview and Examples <Overview>
GettingStarted
JDBC Guide <jdbc/index>
Building <Building>
SchemaEvolution
Extending
Versioning Guide <Versioning>
Coding Best Practices <Coding_Best_Practices>
ReleaseNotes
Frequently Asked Questions <FAQ>
SQL Reference <SQL_Reference>
API <api/index>
Contributing <https://github.com/FoundationDB/fdb-record-layer/blob/main/CONTRIBUTING.md>
Code of Conduct <https://github.com/FoundationDB/fdb-record-layer/blob/main/CODE_OF_CONDUCT.md>
License <https://github.com/FoundationDB/fdb-record-layer/blob/main/LICENSE>
```
