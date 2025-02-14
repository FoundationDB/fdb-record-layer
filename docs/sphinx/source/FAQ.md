# FoundationDB Record Layer FAQ

Welcome to FoundationDB Record Layer Frequently Asked Questions (FAQ)! 

## About the Record Layer

### What is the Record Layer?

The Record Layer is a Java API that provides a structured, record-oriented store 
on top of the core FoundationDB store. For feature highlights, see the
[Record Layer overview](Overview.md). See the [getting started guide](GettingStarted.md)
for a worked-out example of how to begin developing an application that uses the
Record Layer.

### Is it relational? 

Strictly, yes, however it may feel quite different from your traditional
RDBMS.  

The Record Layer defines a concept of a record store. A record store
is an extent in which records of all types in the metadata may be 
stored (i.e., they are co-mingled together), unlike a relational 
database table in which it can only store records (rows) of a single 
schema (see [Are record types tables?](#are-record-types-tables)).  
Indexes may be defined that can span multiple record types (for 
example, indexing field "X" in record types "A" and "B" as a 
single index). 

Additionally the Record Layer allows for nested relationships such
as a field that is, itself, an array of records, and provides the
ability to define indexes across these sorts of relationships.

### Why are queries so restrictive?

If you peruse the questions in [Queries](#queries) section of this FAQ, you'll
note that many of the things you might be accustomed to from other
databases are not available in the Record Layer, such as arbitrary 
aggregation, sorting, and complex joins.

Among the driving design principles of the Record Layer are support for
potentially millions of record stores (possibly sharing a common schema), 
predictable scalability, predictable resource consumption, and stateless 
execution. As such, features in which resources, such as memory, may
not be predictably consumed or may even be unbounded are avoided,
excepting the cases in which the functionality can be supported in a resource 
constrained fashion by the natural ordering of the primary key or 
available indexes.

The Record Layer is, however, certainly intended as a foundation upon which
such features can be built!

## Data modeling

### Record types

#### Are record types tables?

Record types should not be blindly treated like a table in a relational 
database system.  As record types are co-mingled in the same extent,
a query filtered against a single record type may require traversing all of the 
other record types in order to satisfy the query. This effect can 
be mitigated, however, by creating an index on a record type by its
primary key.

Similarly, inserting two records that have the same primary key definition
and the same primary key values will result in the second record replacing
the first. This is true even if the records are of different types with 
different field names.

#### Is there some way to make record types behave more like tables?

A primary key definition can be prefixed with a special record type key.
The Record Layer will assign a unique key value for each record type. In addition to avoiding
the problem of collisions with the rest of the primary key values, the Record Layer
leverages the guarantee that records of a given type are stored together.

* The planner can scan when there is no appropriate index or when the filters match more of the primary key
without skipping other types.
* The online index builder does not need to scan as many records.
* When deciding whether to build a new index right away, the count of records by type can be used.
* store.deleteRecordsWhere can be restricted to a single record type and still efficient.

The overhead of this is usually two bytes in every key (record or index entry).

### Indexes

#### Are auto-incremented primary keys supported?

Is it possible to define a primary key such that the value of the key is 
automatically assigned by the Record Layer as an auto-incrementing value?

The short answer is: No. It is the responsibility of the client to assign 
a suitably unique value (or values, in the case of a composite key) for the record. 

Long answer: The Record Layer does not currently offer any kind of auto-generated primary key.
There are a few reasons for this, but the most pressing is scalability. 
FoundationDB fundamentally doesn't offer a way to get a low-contention incrementing 
counter to produce a stream of integers, monotonically increasing by 1. There are ways of 
getting such a stream, but they will almost certainly serialize inserts to a record store (so 
you'll be able to commit one record at a time globally). This is actually acceptable to 
some use cases (for example, a use case that is sharded into many record stores may be okay 
if access to each individual record store is serialized like this, as updates to each 
record store are independent and can be done concurrently).

Another reason to be weary of auto-generated keys is that it is hard to make a system 
which is idempotent and also uses auto-generated keys for inserts. In particular, this is important 
in handling certain failure scenarios in FoundationDB. In particular, see: 
[the developer's guide](https://apple.github.io/foundationdb/developer-guide.html#developer-guide-unknown-results).

That being said, there are strategies for medium-contention auto-incrementing keys. For example
you can use a "MAX_EVER" index on the primary key, and then read the index (at snapshot isolation 
level) when you need to generate a new key, add a random value to it, and then use this 
as the new key. This will produce a key higher than any other key used so far, so it is guaranteed 
when it inserts that the key it's writing to will be blank unless there is a concurrent insert 
to the same spot, in which case the transaction will fail with a conflict. By adjusting the random 
amount you can alter the likelihood of conflicts at the cost of gaps in the keys that are
generated.

There's a second approach that is more theoretical which is to use 
[versionstamp operations](https://apple.github.io/foundationdb/api-python.html#fdb.Transaction.set_versionstamped_key) 
to get a sequence of monotonically increasing keys from the database. We already do that for certain 
index types to enable things like an index for changes to a record store, but we don't allow such 
expressions in the primary key.

But all of this assumes you care about ordering. If you don't care about ordering, then you 
could generate a UUID. 

## Queries

### Is SQL supported?

No. Currently the Record Layer has no query language, however it does expose a query API 
and a query planner. In the future it is possible that the Record Layer may develop a
formal query language, but it is unlikely that such a language would closely resemble 
the SQL standard.

### Are joins supported?

Query and planning suport for joins is not supported.  However, 
it is possible to programatically implement a join by scanning the outer record type and
using `flatMapPipelined` to retrieve inner records. Alternatively, you could define an
index that spans multiple record types, logically co-locating the records on the join
field(s), and then perform a scan of the index to implement the join. 

### Are sorts supported?

Yes, but only in cases in which the sort order requested can be supported by the
natural ordering of the primary keys or by indexes. The Record Layer has no facility 
for in-memory sorts or sorts that spill to disk (see 
[Why are queries so restrictive?](#why-are-queries-so-restrictive)).

### Are aggregation and grouping supported?

Yes, but only in cases in which an aggregate index has been defined to support
the operation. The query planner has only limited support for the use of these
indexes at the moment.

## Transactions

The core behavior of transactions is dictated by FoundationDB's behavior and functionality
and some of these questions and answers are really specific to FoundationDB, but are 
listed here simply because they come up often.

### What does `not_committed` (error 1020) mean? 

Unlike traditional databases, FoundationDB does not use locking to control concurrent
modification of data, instead it uses [optimistic concurrency control](https://en.wikipedia.org/wiki/Optimistic_concurrency_control),
in which, upon commit, the transaction is validated as to whether or not another transaction
modified data upon which your transaction was dependent.  The `not_committed` error indicates
that there was a conflict with another transaction and, thus, not committed. This will
typically indicate that the work should be retried.

### What does `transaction_too_old` (error 1007) mean?

FoundationDB allows transactions to remain open for a [limited period of time](https://apple.github.io/foundationdb/known-limitations.html#long-running-transactions).
This means that your transaction has expired and may indicate that you need to decompose
your work into a number of smaller transactions. All streaming operations in the Record Layer
support **continuations** (an opaque representation of the current location within the stream),
so restarting the same operation with a new transaction and a continuation can allow the
operation to continue where it left off.

### What does `transaction_too_large` (error 2101) mean?

FoundationDB restricts transactions to modifying a [limited amount of data](https://apple.github.io/foundationdb/known-limitations.html#large-transactions).
This means that your transaction has exceeded that limit and may indicate that you need
to decompose your work into a number of smaller transactions.
