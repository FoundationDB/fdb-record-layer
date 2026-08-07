===================
CREATE STORED QUERY
===================

.. _create_stored_query:

Registers a named query inside a schema template so its plan is generated ahead of time ("warmed"), letting matching queries issued at runtime reuse the cached plan instead of being planned from scratch. A stored query is not invoked by name, it exists only to pre-populate the plan cache.

A stored query body parameterizes the values it should serve at runtime with **inline typed parameters** ``?{type}`` (e.g. ``col1 > ?{bigint}``): each declares a type but no value, so the query is planned **value-free** and one cached plan is reused by any runtime query that binds a value of that type at the same position. A concrete literal is also accepted in a body, but that is a corner case (see below).

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
    The body of the stored query — any SELECT statement (including CTEs, recursive CTEs, and joins). Its runtime-varying values are written as **inline typed parameters** ``?{type}`` (see below); a concrete literal may be used instead where the value must stay fixed at plan time.

Inline typed parameters (``?{type}``)
=====================================

An inline typed parameter ``?{type}`` stands in for a value in the body, for example ``col1 > ?{bigint}``. It declares only a type, not a value, so the stored query is planned **value-free** at warm-up: the resulting plan is independent of any particular value and is reused by a runtime query that binds a value of the same type at the same position.

.. code-block:: sql

    CREATE STORED QUERY by_col1 AS SELECT * FROM t1 WHERE col1 > ?{bigint}

At runtime, an ordinary parameterized query reuses the warmed plan:

.. code-block:: sql

    SELECT * FROM t1 WHERE col1 > ?    -- bound to a BIGINT (e.g. setLong(1, 20L)); hits the warmed plan

Supported types are the primitive types ``BOOLEAN``, ``INTEGER``, ``BIGINT``, ``FLOAT``, ``DOUBLE``, ``STRING``, ``BYTES``, and ``UUID``, plus the special ``NULL`` (see below).

Notes and limitations
---------------------

- **The runtime value's type must match the declared type exactly.** A stored query declared ``?{bigint}`` is reused only by a runtime value bound as a ``BIGINT`` (e.g. JDBC ``setLong``); a value of another type (e.g. an ``INTEGER``) does not match and is planned separately.
- **Non-null by default.** An ordinary inline typed parameter carries an ``IS NOT NULL`` constraint. To warm the plan for a ``NULL`` binding (e.g. JDBC ``setNull``), declare the parameter as ``?{null}``: it is planned value-free with an ``IS NULL`` constraint and reused by a runtime ``NULL`` value at the same position. ``?{null}`` and a typed non-null parameter warm separate plans.
- **Boolean and ``NULL`` values become part of the plan constraint.** A boolean value folds into an ``IS TRUE`` / ``IS FALSE`` constraint and a ``NULL`` into an ``IS NULL`` / ``IS NOT NULL`` constraint — either can change the chosen plan significantly (e.g. flip a branch or prune it), so a warmed plan is valid only for that specific value. Write these as concrete literals directly in the body and, when both cases are needed, enumerate them as separate stored queries (one with ``TRUE`` and one with ``FALSE``, or one with ``NULL`` and one without) rather than parameterizing with ``?{boolean}``.
- **A value that drives plan choice must stay a concrete literal.** ``?{type}`` cannot be used where the planner needs the actual value to choose an access path — most notably a value compared against a **filtered/range index**. Such a value-free stored query is skipped at warm-up (and simply planned normally at runtime, when the value is present); use a concrete literal instead (see the corner case below).

Examples
========

Declare a stored query with an inline typed parameter as part of a schema template:

.. code-block:: sql

    CREATE SCHEMA TEMPLATE example_template
        CREATE TABLE t1(id BIGINT, col1 BIGINT, PRIMARY KEY(id))
        CREATE INDEX i1 AS SELECT col1 FROM t1
        CREATE STORED QUERY by_col1 AS SELECT * FROM t1 WHERE col1 > ?{bigint}

``by_col1`` warms one value-free plan. Any runtime query of the same shape, binding a ``BIGINT``, reuses it:

.. code-block:: sql

    SELECT * FROM t1 WHERE col1 > ?    -- setLong(1, 20L) → reuses the warmed plan

Parameters may be used freely, including inside a ``DECLARE`` block. A declared function is the warm-up counterpart of a runtime :ref:`CREATE TEMPORARY FUNCTION <create_temporary_function>` — same function, supplied while warming instead of in a live session:

.. code-block:: sql

    CREATE STORED QUERY by_helper
        DECLARE
            FUNCTION above(IN threshold BIGINT) AS (SELECT * FROM t1 WHERE col1 > threshold)
    AS
        SELECT * FROM above(?{bigint})

Temporary functions in scope are part of the plan-cache key, so a runtime query reuses this warmed plan only if it has declared the same temporary function. The runtime session installs an equivalent ``CREATE TEMPORARY FUNCTION`` and issues the same query:

.. code-block:: sql

    CREATE TEMPORARY FUNCTION above(IN threshold BIGINT) ON COMMIT DROP FUNCTION
        AS SELECT * FROM t1 WHERE col1 > threshold;

    SELECT * FROM above(?)    -- setLong(1, 20L) → reuses the warmed plan

Warming the ``NULL`` case
-------------------------

When a parameter's null-ness changes the result, warm each case as its own stored query — one with ``?{type}`` (non-null) and one with ``?{null}`` — because the value folds into the plan constraint. For a body that treats ``NULL`` as "match everything":

.. code-block:: sql

    CREATE STORED QUERY by_val
        DECLARE
            FUNCTION by_val(IN p BIGINT) AS (SELECT * FROM t1 WHERE p IS NULL OR col1 = p)
    AS
        SELECT id FROM by_val(?{bigint})

    CREATE STORED QUERY by_val_null
        DECLARE
            FUNCTION by_val(IN p BIGINT) AS (SELECT * FROM t1 WHERE p IS NULL OR col1 = p)
    AS
        SELECT id FROM by_val(?{null})

These warm two separate plans (``IS NOT NULL`` vs ``IS NULL``). A runtime session re-declares ``by_val`` and binds the parameter; each binding hits the matching plan:

.. code-block:: sql

    SELECT id FROM by_val(?)    -- setLong(1, 20L) → IS NOT NULL plan: rows where col1 = 20
    SELECT id FROM by_val(?)    -- setNull(1, BIGINT) → IS NULL plan: every row

Corner case: concrete literals
------------------------------

A body may use a concrete literal instead of a ``?{type}`` parameter. This is needed when the value must stay fixed at plan time — most importantly when it is compared against a **filtered/range index**, because the planner has to check that the value falls inside the index's predicate to select that index. A ``?{type}`` here has no value to check, so the stored query would be skipped at warm-up; a concrete literal lets it warm normally:

.. code-block:: sql

    CREATE SCHEMA TEMPLATE example_template
        CREATE TABLE t1(id BIGINT, col1 BIGINT, PRIMARY KEY(id))
        -- filtered index: only rows with col1 > 42 are indexed
        CREATE INDEX hot AS SELECT col1 FROM t1 WHERE col1 > 42 ORDER BY col1
        -- concrete literal 50 lets the planner prove 50 > 42, so the plan uses the "hot" index
        CREATE STORED QUERY hot_col1 AS SELECT * FROM t1 WHERE col1 > 50

The index's filter goes into the plan constraint, so the warmed plan works for any literal that satisfies that constraint:

.. code-block:: sql

    SELECT * FROM t1 WHERE col1 > 60    -- 60 > 42, reuses the warmed index-scan plan

Writing ``col1 > ?{bigint}`` instead would leave the planner unable to prove the value falls in ``col1 > 42``, so that stored query is skipped at warm-up (it still plans normally at runtime, once a value is bound).

See Also
========

* :ref:`CREATE SCHEMA TEMPLATE <schema_template>` - The template that stored queries are declared in
* :ref:`CREATE TEMPORARY FUNCTION <create_temporary_function>` - Same function syntax as the ``DECLARE`` block
