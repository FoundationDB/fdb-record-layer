===================
CREATE STORED QUERY
===================

.. _create_stored_query:

Registers a named query inside a schema template. Its plan is generated ahead of time ("warmed") when the engine starts up, so that matching queries issued at runtime reuse the cached plan instead of being planned from scratch.

Stored queries are declared as part of a :ref:`schema template <schema_template>`, alongside tables and indexes. A stored query is not invoked by name; instead it pre-populates the plan cache. The warmed plan is keyed by the canonical (literal-stripped) form of the body, the temporary functions in scope, and its plan constraints, so any runtime query with the same canonical form, equivalent temporary functions, and compatible bound values transparently hits it.

Warming happens once, when the engine is constructed: each stored query is planned against the schema template it belongs to. Only schema templates already present in the catalog at that moment are warmed — a schema template created after the engine is up is warmed the next time an engine is constructed against that catalog.

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
    The name of the stored query, unique within the schema template. The name identifies the stored query in metadata; it is not used to invoke the query.

``DECLARE`` block
    Optional. Declares one or more transaction-local functions that the stored query body may call, using the same syntax as :ref:`CREATE TEMPORARY FUNCTION <create_temporary_function>`. Multiple functions are separated by semicolons.

``query``
    The body of the stored query — any SELECT statement (including CTEs, recursive CTEs, and joins). The body may contain concrete literals.

Examples
========

Declare a stored query as part of a schema template:

.. code-block:: sql

    CREATE SCHEMA TEMPLATE example_template
        CREATE TABLE t1(id BIGINT, col1 BIGINT, PRIMARY KEY(id))
        CREATE INDEX i1 AS SELECT col1 FROM t1
        CREATE STORED QUERY by_col1 AS SELECT * FROM t1 WHERE col1 = 10

``by_col1`` warms a plan for ``col1 = 10``. Because literal values are stripped during planning, any runtime query of the same shape reuses it regardless of the constant:

.. code-block:: sql

    SELECT * FROM t1 WHERE col1 = 42    -- reuses the warmed plan

A ``DECLARE`` block makes transaction-local functions available to the body:

.. code-block:: sql

    CREATE STORED QUERY by_helper
        DECLARE
            FUNCTION recent(IN threshold BIGINT) AS (SELECT * FROM t1 WHERE col1 > threshold)
    AS
        SELECT * FROM recent(10)

The declared function is transaction-local: it exists only for warming the stored query and is not visible to arbitrary runtime queries. A runtime query therefore cannot reference ``recent`` directly — and it does not need to. Because the temporary functions in scope are part of the plan-cache key (alongside the canonical query text), a runtime session reuses the warmed plan by installing an *equivalent* temporary function and issuing the same query:

.. code-block:: sql

    CREATE TEMPORARY FUNCTION recent(IN threshold BIGINT) ON COMMIT DROP FUNCTION
        AS SELECT * FROM t1 WHERE col1 > threshold;

    SELECT * FROM recent(20)    -- reuses the warmed plan

The function definition and the invocation must match what was declared in the stored query (the invocation's literal is stripped, so any argument value reuses the plan). This lets an application share one pre-warmed plan across sessions that each re-declare the same ad-hoc function.

See Also
========

* :ref:`CREATE SCHEMA TEMPLATE <schema_template>` - The template that stored queries are declared in
* :ref:`CREATE TEMPORARY FUNCTION <create_temporary_function>` - Same function syntax as the ``DECLARE`` block
