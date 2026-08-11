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

    CREATE STORED QUERY query_name [ ( parameter_name data_type, ... ) ]
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

``( parameter_name data_type, ... )`` (signature)
    Optional. A list of typed named parameters. Each parameter is referenced by name — as a bare identifier, with no ``?`` prefix — anywhere in the declared function bodies or the stored query body. At warm-up each is planned *value-free* from its declared type; at runtime the client re-issues the equivalent SQL with the reference written as a named parameter ``?parameter_name`` and binds it by name (see the signature example below). A parameter type must be primitive, or the keyword ``NULL`` to declare the parameter as *exactly null* — see :ref:`the null-parameter example <stored_query_null_parameter>`.

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

Parameterizing with a signature
-------------------------------

A stored query signature declares typed named parameters that are used as bare identifiers throughout the declared function bodies and the query body. This warms the plan for *any* runtime value of the declared type, without writing a concrete literal:

.. code-block:: sql

    CREATE STORED QUERY by_sig(param_a BIGINT, param_b BIGINT)
        DECLARE
            FUNCTION f1(IN p BIGINT) AS (SELECT * FROM t1 WHERE (p IS NULL OR col1 = p) AND col2 = param_a)
    AS
        SELECT id FROM f1(param_b)

Here ``param_a`` is captured inside ``f1``'s body (it is not ``f1``'s own parameter ``p``), and ``param_b`` is passed as ``f1``'s argument. Internally each signature parameter becomes a named parameter ``?parameter_name``. At runtime the client re-issues the equivalent SQL — writing each reference as ``?parameter_name`` — and binds the values by name:

.. code-block:: sql

    CREATE TEMPORARY FUNCTION f1(IN p BIGINT) ON COMMIT DROP FUNCTION
        AS SELECT * FROM t1 WHERE (p IS NULL OR col1 = p) AND col2 = ?param_a;   -- bind param_a by name

    SELECT id FROM f1(?param_b)                                                  -- bind param_b by name, reuses the warmed plan

Notes:

* Signature parameter types must be **primitive** (or ``NULL`` — see below).
* A reference must use the parameter's **declared spelling** — matching is case-sensitive, because a signature parameter becomes a named parameter, which is always case-sensitive. A reference written in a different case is treated as an ordinary column.
* A typed parameter warms the plan for a **non-NULL** value of that type. Binding it to ``NULL`` does not reuse that plan. To pre-warm the null case, declare the parameter as exactly null (below).

.. _stored_query_null_parameter:

Null parameters
---------------

Declaring a signature parameter as ``NULL`` marks it as *exactly null* — the warmed plan is specialized for that parameter being NULL, which the planner can optimize (for example, folding ``param IS NULL`` to true and dropping the corresponding index probe). This is a distinct plan from the typed (non-null) one, so the value case and the null case are two separate stored queries:

.. code-block:: sql

    -- value case: param_b is a bigint
    CREATE STORED QUERY sq(param_a BIGINT, param_b BIGINT)
        DECLARE FUNCTION f1(IN p BIGINT) AS (SELECT * FROM t1 WHERE (p IS NULL OR col1 = p) AND col2 = param_a)
    AS SELECT id FROM f1(param_b)

    -- null case: param_b is exactly null (an optimized plan)
    CREATE STORED QUERY sq_bnull(param_a BIGINT, param_b NULL)
        DECLARE FUNCTION f1(IN p BIGINT) AS (SELECT * FROM t1 WHERE (p IS NULL OR col1 = p) AND col2 = param_a)
    AS SELECT id FROM f1(param_b)

At runtime the client binds the null parameter with ``setNull`` to reuse the null-specialized plan:

.. code-block:: sql

    CREATE TEMPORARY FUNCTION f1(IN p BIGINT) ON COMMIT DROP FUNCTION
        AS SELECT * FROM t1 WHERE (p IS NULL OR col1 = p) AND col2 = ?param_a;

    SELECT id FROM f1(?param_b)    -- setLong(param_a, ...), setNull(param_b) → reuses the null-specialized plan

See Also
========

* :ref:`CREATE SCHEMA TEMPLATE <schema_template>` - The template that stored queries are declared in
* :ref:`CREATE TEMPORARY FUNCTION <create_temporary_function>` - Same function syntax as the ``DECLARE`` block
