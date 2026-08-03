===================
CREATE STORED QUERY
===================

.. _create_stored_query:

Registers a named query inside a schema template so its plan is generated ahead of time ("warmed"), letting matching queries issued at runtime reuse the cached plan instead of being planned from scratch. A stored query is not invoked by name, it exists only to pre-populate the plan cache.

The plan cache is local to each engine instance and starts empty. When a fresh instance is constructed it opens the catalog once and, for every schema template present that declares stored queries, plans each stored query offline and stores the resulting plan in that instance's cache. Templates created after the instance is up are warmed by the next fresh instance.

A runtime query hits a warmed plan when its canonical (literal-stripped) form, the temporary functions in scope, and its plan constraints all match — so bound values may differ from those the stored query was written with.

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
    The body of the stored query — any SELECT statement (including CTEs, recursive CTEs, and joins). The body may contain concrete literals, and/or **inline typed parameters** (see below).

Inline typed parameters (``?{type}``)
=====================================

A stored query body can use an **inline typed parameter** ``?{type}`` in place of a concrete literal, for example ``col1 > ?{bigint}``. The parameter declares only a type, not a value, so the stored query is planned **value-free** at warm-up: the resulting plan is independent of any particular value and is reused by a runtime query that binds a value of the same type at the same position.

.. code-block:: sql

    CREATE STORED QUERY by_col1 AS SELECT * FROM t1 WHERE col1 > ?{bigint}

At runtime, an ordinary parameterized query reuses the warmed plan:

.. code-block:: sql

    SELECT * FROM t1 WHERE col1 > ?    -- bound to a BIGINT value; hits the warmed plan: RelationalPreparedStatement.setLong(1, 20L);

Supported types are the primitive types ``BOOLEAN``, ``INTEGER``, ``BIGINT``, ``FLOAT``, ``DOUBLE``, ``STRING``, ``BYTES``, and ``UUID``, plus the special ``NULL`` (see below).

Notes and limitations
---------------------

- **The runtime value's type must match the declared type exactly.** A stored query declared ``?{bigint}`` is reused only by a runtime value bound as a ``BIGINT`` (e.g. JDBC ``setLong``); a value of another type (e.g. an ``INTEGER``) does not match and is planned separately.
- **Non-null by default.** An ordinary inline typed parameter carries an ``IS NOT NULL`` constraint. To warm the plan for a ``NULL`` binding (e.g. JDBC ``setNull``), declare the parameter as ``?{null}``: it is planned value-free with an ``IS NULL`` constraint and reused by a runtime ``NULL`` value at the same position. ``?{null}`` and a typed non-null parameter warm separate plans.
- **Values that drive plan choice must stay concrete.** A parameter whose value the planner needs to choose an access path — most notably a value compared against a **filtered/range index** — cannot be planned value-free. Such a stored query is skipped at warm-up (and simply planned normally at runtime, when the value is present). Use a concrete literal for those cases.
- A single body may freely mix concrete literals and ``?{type}`` parameters; keep a value concrete where it genuinely drives plan choice, and use ``?{type}`` where it does not.

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

A body may instead use an inline typed parameter (see above), warming a single value-free plan reused by any value of that type:

.. code-block:: sql

    CREATE STORED QUERY by_col1_typed AS SELECT * FROM t1 WHERE col1 > ?{bigint}
    -- runtime: SELECT * FROM t1 WHERE col1 > ?  (bound to a BIGINT) reuses the warmed plan

A ``DECLARE`` block declares transaction-local functions the body can call. A declared function is the warm-up counterpart of a runtime :ref:`CREATE TEMPORARY FUNCTION <create_temporary_function>` — same function, supplied while warming instead of in a live session:

.. code-block:: sql

    CREATE STORED QUERY by_helper
        DECLARE
            FUNCTION recent(IN threshold BIGINT) AS (SELECT * FROM t1 WHERE col1 > threshold)
    AS
        SELECT * FROM recent(10)

Temporary functions in scope are part of the plan-cache key, so a runtime query reuses this warmed plan only if it has declared the same temporary function. The runtime session installs an equivalent ``CREATE TEMPORARY FUNCTION`` and issues the same query:

.. code-block:: sql

    CREATE TEMPORARY FUNCTION recent(IN threshold BIGINT) ON COMMIT DROP FUNCTION
        AS SELECT * FROM t1 WHERE col1 > threshold;

    SELECT * FROM recent(20)    -- reuses the warmed plan

The function definition must match the one declared in the stored query; the invocation's literal is stripped, so any argument value reuses the plan.

See Also
========

* :ref:`CREATE SCHEMA TEMPLATE <schema_template>` - The template that stored queries are declared in
* :ref:`CREATE TEMPORARY FUNCTION <create_temporary_function>` - Same function syntax as the ``DECLARE`` block
