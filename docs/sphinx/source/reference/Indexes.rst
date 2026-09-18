=======
Indexes
=======

.. _index_definition:

Defining indexes
################

It is possible to define indexes in the Relational Layer as materialized views of :doc:`SELECT <sql_commands/DQL/SELECT>`
statements. To be used in an index, the query must be able to be incrementally updated. That is, when a row on an indexed
table is inserted, updated, or deleted, it must be possible to construct a finite set of modifications to the index
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

Index syntax alternatives
#########################

The Relational Layer supports two equivalent syntaxes for creating indexes:

1. **INDEX AS SELECT** - A query-based syntax (shown above) inspired by materialized views
2. **INDEX ON** - A traditional columnar syntax that creates indexes on tables or views

Both syntaxes produce identical index structures and have the same capabilities. The choice between them is primarily
a matter of style and organizational preference.

``INDEX ON`` syntax
*******************

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

Syntax comparison
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

Column ordering and ``NULL`` handling
#####################################

When creating indexes using either syntax, you can control how values are sorted in the index through ordering
clauses and NULL semantics.

Sorting criteria
****************

Each key column in an INDEX ON definition supports explicit sort order:

* :sql:`ASC` (ascending) - Values sorted from smallest to largest (default if not specified)
* :sql:`DESC` (descending) - Values sorted from largest to smallest

For INDEX AS SELECT, the sort order is specified in the ORDER BY clause.

``NULL`` semantics
******************

You can control where NULL values appear in the sort order:

* :sql:`NULLS FIRST` - NULL values appear before non-NULL values
* :sql:`NULLS LAST` - NULL values appear after non-NULL values

**Default NULL behavior:**

* For ``ASC`` ordering: ``NULLS FIRST`` is the default
* For ``DESC`` ordering: ``NULLS LAST`` is the default

Ordering syntax examples
************************

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

Indexes on unnested ``ARRAY`` fields
####################################

If the table underlying the index contains an ``ARRAY`` column, it is possible to define an index on the result of unnesting the array. (This is subject to certain constraints, as described under :ref:`index_rules`.)

Let us consider a simple example. Suppose we have a table ``restaurant``:

.. code-block:: sql

    CREATE TYPE AS STRUCT restaurant_review (reviewer STRING, rating INT64);

    CREATE TABLE restaurant (
        rest_no INT64,
        name STRING,
        reviews restaurant_review ARRAY,
        PRIMARY KEY(rest_no)
    );

To optimize queries involving restaurant ratings, it may make sense to have an index defined on the unnested ``rating`` array. To do so, use the constructs described under :doc:`Unnesting`:

.. code-block:: sql

    CREATE INDEX idx_review_rating AS
        SELECT review.rating FROM restaurant AS RR, RR.reviews AS review;

As a (less terse) alternative, the same index can also be written with an explicit subquery as follows:

.. code-block:: sql

    CREATE INDEX idx_review_rating AS
        SELECT SQ.rating FROM restaurant AS RR, (SELECT rating FROM RR.reviews) SQ;

Both forms produce the same underlying key expression, ``field("reviews", FAN_OUT).nest(field("rating"))``. For each row of ``restaurant``, the index emits one entry per element of ``reviews``, keyed on the ``rating`` of the element. The general translation rule is described under :ref:`Implementation details <implementation_details>` below.

Partitioning for vector indexes
###############################

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

.. _index_rules:

Index rules
###########

Defining an index must adhere to these rules:

* The index body must involve only a single base table. Unnestings may be chained, however: an unnesting of a
  nested repeated field may itself contain a further unnesting of another nested repeated field within it.
* Predicates are not allowed in any (sub)query.
* Each subquery can have a single _source_ and an arbitrary number of other nested subqueries. A source is effectively a
  nested-repeated field. In the examples above, :sql:`RR.reviews` is such a source.
* The parent query must have the table itself as a source.
* Propagation of selected fields from subqueries must follow the same order and clustering. Interleaving is not supported. For example, this is illegal:

    .. code-block:: sql

        SELECT T1.a, T2.c, T1.b FROM T t, (SELECT a, b FROM t.X) T1, (SELECT c FROM t.Y) T2

  However, this is legal:

    .. code-block:: sql

        SELECT T1.a, T1.a, T2.c FROM T t, (SELECT a, b FROM t.X) T1, (SELECT c FROM t.Y) T2

.. _implementation_details:

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

See also
########

* :doc:`CREATE INDEX <sql_commands/DDL/CREATE/INDEX>` - Complete CREATE INDEX command reference with detailed syntax and examples
* :doc:`Unnesting` - Array unnesting (used in the body of nested-field indexes)
