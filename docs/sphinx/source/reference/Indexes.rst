=======
Indexes
=======

.. _index_definition:

Defining indexes
################

It is possible to define indexes in the Relational Layer as materialized views of :doc:`SELECT <sql_commands/DQL/SELECT>`
statements.To be used in an index, the query must able to be incrementally updated. That is, when a row on an indexed
table is inserted, updated, or deleted, it must be possible to construct a finite set up modifications to the index
structure to reflect the new query results.

In practice, that means that indexes are translated into a Record Layer key expression and index type. Indexes currently
only support indexes on a single table. When row is modified, the key expression generates zero or more tuples based
on the index data, and then it stores them in a format optimized for fast lookups.

The simplest indexes are simple projections with an `ORDER BY` clause. They follow this form:

.. code-block:: sql

    CREATE INDEX <indexName> AS
        SELECT (field1, field2, ... fieldN, fieldN+1, ... fieldM)
        FROM <tableName> ORDER BY ( field1, field2, ... fieldN )

Where:

* :sql:`indexName` is the name of the index.
* :sql:`tableName` is the name of the table the index is defined on.
* :sql:`field1, field2, ... fieldN` are the indexed fields.
* :sql:`fieldN+1, fieldN+1, ... fieldM` optional list of non-indexed fields to be attached to each tuple to reduce the chance of fetch (index lookup).

For example, let us say we have the following table:

.. code-block:: sql

    CREATE TABLE employee(id INT64, fname STRING, lname STRING, PRIMARY KEY (id))

We define the following index:

.. code-block:: sql

    CREATE INDEX fnameIdx AS SELECT fname, lname FROM employee ORDER BY fname

This index will contain two-tuples of `(fname, lname)`, but ordered only by `fname`. In the Record Layer, this
corresponds to a `VALUE` index with the `fname` field indexed in the key and the `lname` field indexed in the value.
This index can be used to optimize queries like:

.. code-block:: sql

   -- Simple index scan
   SELECT fname, lname FROM employee ORDER BY fname ASC;
   -- Simple index scan, but in reverse
   SELECT fname, lname FROM employee ORDER BY fname DESC;
   -- Limited scan of the index to single fname value
   SELECT lname FROM employee WHERE fname = ?;
   -- Limited scan of the index to range of fname values
   SELECT fname, lname FROM employee WHERE fname > ? AND fname < ?;
   -- Limited scan of the index to single fname value with additional lname predicate
   SELECT lname FROM employee WHERE fname = ? AND lname LIKE 'a%';

In each case, the fact that index entries are ordered by `fname` is leveraged, either to satisfy the sort or to limit
the scan to a smaller range of index entries. The `lname` field can then be returned to the user without having to
perform an additional lookup of the underlying row.

Index Syntax Alternatives
##########################

The Relational Layer supports two equivalent syntaxes for creating indexes:

1. **INDEX AS SELECT** - A query-based syntax (shown above) inspired by materialized views
2. **INDEX ON** - A traditional columnar syntax that creates indexes on tables or views

Both syntaxes produce identical index structures and have the same capabilities. The choice between them is primarily
a matter of style and organizational preference.

INDEX ON Syntax
***************

The ``INDEX ON`` syntax provides a more traditional approach to index creation, specifying columns directly rather
than through a SELECT query:

.. code-block:: sql

    CREATE INDEX <indexName> ON <source>(<columns>) [INCLUDE(<valueColumns>)] [OPTIONS(...)]

Where:

* :sql:`indexName` is the name of the index
* :sql:`source` is a table or view name
* :sql:`columns` specifies the index key columns with optional ordering
* :sql:`INCLUDE` clause (optional) adds covered columns stored as values
* :sql:`OPTIONS` clause (optional) specifies index-specific options

Using the same employee table from above, we can create an equivalent index using the INDEX ON syntax:

.. code-block:: sql

    CREATE INDEX fnameIdx ON employee(fname) INCLUDE(lname)

This creates the same index structure as the INDEX AS SELECT example - a VALUE index with ``fname`` in the key
and ``lname`` as a covered value.

Syntax Comparison
*****************

These two approaches create identical indexes:

**INDEX AS SELECT:**

.. code-block:: sql

    CREATE INDEX fnameIdx AS
        SELECT fname, lname
        FROM employee
        ORDER BY fname

**INDEX ON:**

.. code-block:: sql

    CREATE INDEX fnameIdx ON employee(fname) INCLUDE(lname)

Column Ordering and NULL Handling
##################################

When creating indexes using either syntax, you can control how values are sorted in the index through ordering
clauses and NULL semantics.

Sorting Criteria
****************

Each key column in an INDEX ON definition supports explicit sort order:

* :sql:`ASC` (ascending) - Values sorted from smallest to largest (default if not specified)
* :sql:`DESC` (descending) - Values sorted from largest to smallest

For INDEX AS SELECT, the sort order is specified in the ORDER BY clause.

NULL Semantics
**************

You can control where NULL values appear in the sort order:

* :sql:`NULLS FIRST` - NULL values appear before non-NULL values
* :sql:`NULLS LAST` - NULL values appear after non-NULL values

**Default NULL behavior:**

* For ``ASC`` ordering: ``NULLS FIRST`` is the default
* For ``DESC`` ordering: ``NULLS LAST`` is the default

Ordering Syntax Examples
*************************

The ordering clause for each column in INDEX ON can take several forms:

1. Sort order only: ``columnName ASC`` or ``columnName DESC``
2. Sort order with null semantics: ``columnName ASC NULLS LAST`` or ``columnName DESC NULLS FIRST``
3. Null semantics only: ``columnName NULLS FIRST`` (uses default ASC ordering)

Examples:

.. code-block:: sql

    -- Ascending order with nulls last
    CREATE INDEX idx_rating ON products(rating ASC NULLS LAST)

    -- Descending order with nulls first
    CREATE INDEX idx_price ON products(price DESC NULLS FIRST)

    -- Specify only null semantics (ascending is implicit)
    CREATE INDEX idx_stock ON products(stock NULLS LAST)

    -- Mixed ordering across multiple columns
    CREATE INDEX idx_complex ON products(
        category ASC NULLS FIRST,
        price DESC NULLS LAST,
        name ASC
    )

For INDEX AS SELECT syntax, the same ordering is specified in the ORDER BY clause:

.. code-block:: sql

    CREATE INDEX idx_rating AS
        SELECT rating
        FROM products
        ORDER BY rating ASC NULLS LAST

Partitioning for Vector Indexes
################################

Vector indexes support an optional ``PARTITION BY`` clause that allows organizing vectors by category or tenant.
This clause is **only applicable to vector indexes** created with the ``VECTOR INDEX`` syntax and is not supported
for regular value indexes.

Partitioning helps improve query performance for vector similarity searches by limiting the search space to relevant
partitions:

.. code-block:: sql

    CREATE VECTOR INDEX idx_embedding USING HNSW ON products(embedding)
        PARTITION BY(category)

In this example, vectors are partitioned by product category, so similarity searches can be scoped to specific
categories for better performance.

**Important:** The ``PARTITION BY`` clause cannot be used with regular (non-vector) indexes created using either
the INDEX AS SELECT or INDEX ON syntax.

Indexes on nested fields
########################

The Relational Layer also offers a special version of indexes that can be used to describe how to index nested fields and
:sql:`ARRAY` fields. They are defined using SQL, and they follow a strict set of rules, but before we get into the details, let
us introduce them first by the following simple example. Suppose we have a table :sql:`restaurant` that is defined like this:

.. code-block:: sql

    CREATE TYPE AS STRUCT restaurant_review (reviewer STRING, rating INT64);
    CREATE TABLE restaurant (
        rest_no INT64, name STRING, reviews restaurant_review ARRAY, PRIMARY KEY(rest_no)
    );

Let us say we have too many queries involving restaurant ratings, so it makes sense to have an index defined on :sql:`rating`.
We can define an index to do exactly that:

.. code-block:: sql

    CREATE INDEX mv AS
        SELECT SQ.rating from restaurant AS RR, (select rating from RR.reviews) SQ;

At first glance, it looks like the query is performing a join between :sql:`restaurant` and a subquery. However, if we
take a closer look, we see that the table we select from in the subquery is nothing but the nested repeated field in
:sql:`restaurant`. We use the alias :sql:`RR` to link the nested repeated field and its parent together.


Index rules
###########

Defining a index must adhere to these rules:

* It must involve a single table only, correlated joins that navigate from one nested repeated field to another is possible.
* Predicates are not allowed in any (sub)query.
* Each subquery can have a single _source_ and an arbitrary number of other nested subqueries. A source is effectively a
  nested-repeated field. In the example above, :sql:`RR.reviews` is the source of the containing subquery.
* The parent query must have the table itself as a source.
* Propagation of selected fields from subqueries must follow the same order and clustering. Interleaving is not supported. For example, this is illegal:

    .. code-block:: sql

        SELECT T1.a, T2.c, T1.b FROM T t, (SELECT a, b FROM t.X) T1, (SELECT c FROM t.Y) T2

  However, this is legal:

    .. code-block:: sql

        SELECT T1.a, T1.a, T2.c FROM T t, (SELECT a, b FROM t.X) T1, (SELECT c FROM t.Y) T2

Implementation details
######################

Indexes have a close relationship with Record Layer key expressions and index types. When the user defines a index
we create logical plan, analyze it, and then pass it to a special visitor that iterates over its nodes generating a
semantically equivalent key expression. Since SQL it a rich language, it is possible to define a index that
can not be translated to a key expression. Therefore, we define the list of rules above to limit, as much as possible,
the possibilities of defining an invalid index.

If the index can be translated into a key expression, the generated key expression will have a structure
that resembles the structure of the SQL statement:

* Projected fields :sql:`f1`, :sql:`f2`, ... :sql:`fn` in (sub)queries maps to a :sql:`concat(field(f1), field(f2), ... field(fn))`.
* Projected nested fields (:sql:`f1`, :sql:`f2`, ... :sql:`fn`) from a repeated field :sql:`rf`, i.e. :sql:`select f1, f2, ... fn, ... from FOO.rf`
  maps to :sql:`field(rf, FAN_OUT).nest(field(f1), (field(f2), ..., field(fn)))`.

See Also
########

* :doc:`CREATE INDEX <sql_commands/DDL/CREATE/INDEX>` - Complete CREATE INDEX command reference with detailed syntax and examples
