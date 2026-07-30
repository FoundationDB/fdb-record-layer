===================
CREATE STORED QUERY
===================

.. _create_stored_query:

Registers a named query inside a schema template. The query's plan is generated ahead of time
("warmed") when the engine starts up, so that matching queries issued at runtime reuse the cached
plan instead of being planned from scratch.

Stored queries are declared as part of a :ref:`schema template <schema_template>`, alongside tables
and indexes. They are not invoked by name; instead, a stored query pre-populates the plan cache, and
any runtime query whose canonical form matches the stored body transparently hits that cached plan.

Syntax
======

.. raw:: html
    :file: STORED_QUERY.diagram.svg

.. code-block:: sql

    CREATE STORED QUERY query_name
        [ DECLARE
              FUNCTION function_name ( [IN] parameter_name data_type [DEFAULT default_value], ... )
                  AS ( query );
              ...
        ]
        AS query

Parameters
==========

``query_name``
    The name of the stored query, unique within the schema template. The name identifies the stored
    query in metadata; it is not used to invoke the query.

``DECLARE`` block
    Optional. Declares one or more transaction-local functions that the stored query body may call,
    using the same syntax as :ref:`CREATE TEMPORARY FUNCTION <create_temporary_function>`. Multiple
    functions are separated by semicolons.

``query``
    The body of the stored query — any SELECT statement (including CTEs, recursive CTEs, and joins).
    The body may contain concrete literals.

Examples
========

Setup
-----

.. code-block:: sql

    CREATE SCHEMA TEMPLATE example_template
        CREATE TABLE t1(id BIGINT, col1 BIGINT, PRIMARY KEY(id))
        CREATE INDEX i1 AS SELECT col1 FROM t1
        CREATE STORED QUERY by_col1 AS SELECT * FROM t1 WHERE col1 = 10

Stored query with a concrete literal
------------------------------------

``by_col1`` warms a plan for ``col1 = 10``. Because literal values are stripped during planning, any
runtime query of the same shape reuses it regardless of the constant:

.. code-block:: sql

    SELECT * FROM t1 WHERE col1 = 42    -- reuses the warmed plan

Stored query with declared functions
-------------------------------------

A ``DECLARE`` block makes transaction-local functions available to the body:

.. code-block:: sql

    CREATE STORED QUERY by_helper
        DECLARE
            FUNCTION recent(IN threshold BIGINT) AS (SELECT * FROM t1 WHERE col1 > threshold)
    AS
        SELECT * FROM recent(10)

Important notes
===============

Warm-up timing
--------------

Stored queries are warmed once, when the engine is constructed, against the schema templates already
present in the catalog. A schema template created after the engine is up is warmed the next time an
engine is constructed against that catalog.

Relationship to the plan cache
------------------------------

A stored query does not create a distinct, separately invokable object. It pre-populates the plan
cache: the warmed plan is keyed by the canonical (literal-stripped) form of the body plus its plan
constraints, so a runtime query with the same canonical form and compatible bound values hits it.

See Also
========

* :ref:`CREATE SCHEMA TEMPLATE <schema_template>` - The template that stored queries are declared in
* :ref:`CREATE TEMPORARY FUNCTION <create_temporary_function>` - Same function syntax as the ``DECLARE`` block
