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
    The body may contain concrete literals, and/or **inline typed parameters** (see below).

Inline typed parameters (``?{type}``)
=====================================

A stored query body can use an **inline typed parameter** ``?{type}`` in place of a concrete literal,
for example ``col1 > ?{bigint}``. The parameter declares only a type, not a value, so the stored
query is planned **value-free** at warmup: the resulting plan is independent of any particular value
and is reused by a runtime query that binds a value of the same type at the same position.

.. code-block:: sql

    CREATE STORED QUERY by_col1 AS SELECT * FROM t1 WHERE col1 > ?{bigint}

At runtime, an ordinary parameterized query reuses the warmed plan:

.. code-block:: sql

    SELECT * FROM t1 WHERE col1 > ?    -- bound to a BIGINT value; hits the warmed plan

Supported types are the primitive types: ``BOOLEAN``, ``INTEGER``, ``BIGINT``, ``FLOAT``,
``DOUBLE``, ``STRING``, ``BYTES``.

Notes and limitations
----------------------

- **The runtime value's type must match the declared type exactly.** A stored query declared
  ``?{bigint}`` is reused only by a runtime value bound as a ``BIGINT`` (e.g. JDBC ``setLong``); a
  value of another type (e.g. an ``INTEGER``) does not match and is planned separately.
- **Non-null only.** An inline typed parameter carries an ``IS NOT NULL`` constraint. Queries that
  must distinguish ``NULL`` should be authored as separate stored queries with concrete ``NULL`` /
  non-``NULL`` literals.
- **Values that drive plan choice must stay concrete.** A parameter whose value the planner needs to
  choose an access path — most notably a value compared against a **filtered/range index** — cannot
  be planned value-free. Such a stored query is skipped at warmup (and simply planned normally at
  runtime, when the value is present). Use a concrete literal for those cases.
- A single body may freely mix concrete literals and ``?{type}`` parameters; keep a value concrete
  where it genuinely drives plan choice, and use ``?{type}`` where it does not.

Examples
========

Setup
-----

.. code-block:: sql

    CREATE SCHEMA TEMPLATE example_template
        CREATE TABLE t1(id BIGINT, col1 BIGINT, PRIMARY KEY(id))
        CREATE INDEX i1 AS SELECT col1 FROM t1
        CREATE STORED QUERY by_col1_literal AS SELECT * FROM t1 WHERE col1 = 10
        CREATE STORED QUERY by_col1_typed AS SELECT * FROM t1 WHERE col1 > ?{bigint}

Stored query with a concrete literal
------------------------------------

``by_col1_literal`` warms a plan for ``col1 = 10``. Because literal values are stripped during
planning, any runtime query of the same shape reuses it regardless of the constant:

.. code-block:: sql

    SELECT * FROM t1 WHERE col1 = 42    -- reuses the warmed plan

Stored query with an inline typed parameter
-------------------------------------------

``by_col1_typed`` warms a single value-free plan that serves any ``BIGINT`` value:

.. code-block:: sql

    SELECT * FROM t1 WHERE col1 > ?     -- bound to any BIGINT; reuses the warmed plan

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
