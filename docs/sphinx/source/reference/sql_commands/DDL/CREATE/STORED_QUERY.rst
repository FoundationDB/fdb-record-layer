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
        [ ( parameter_name data_type [ NOT NULL ], ... )
          PREPARE FOR (
              ( parameter_name { IS [ NOT ] NULL | = { TRUE | FALSE } }, ... ),
              ...
          )
        ]
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

signature
    Optional. Declares typed named parameters, which the body refers to by name. See `Signature`_ below. A signature requires a ``PREPARE FOR`` block, and a ``PREPARE FOR`` block requires a signature.

``PREPARE FOR`` block
    Required whenever a signature is present. Lists the combinations of parameter states that each get their own warmed plan. Every case must pin every declared parameter. See `Prepared cases`_ below.

``DECLARE`` block
    Optional. Declares one or more transaction-local functions that the stored query body may call, using the same syntax as :ref:`CREATE TEMPORARY FUNCTION <create_temporary_function>`. Multiple functions are separated by semicolons.

``query``
    The body of the stored query — any SELECT statement (including CTEs, recursive CTEs, and joins). The body may contain concrete literals.

Signature
=========

A stored query may declare parameters, which is how it stands in for a runtime query that binds values rather than writing them as literals:

.. code-block:: sql

    CREATE STORED QUERY by_col1(param_a BIGINT)
        PREPARE FOR ((param_a IS NOT NULL))
        AS SELECT * FROM t1 WHERE col1 = param_a

In the body a parameter is written as a **bare identifier**, with no ``?`` — unlike the runtime statement it stands for, where the same reference is ``?param_a``. It becomes exactly that internally, so the stored form above is equivalent to ``SELECT * FROM t1 WHERE col1 = ?PARAM_A``. A parameter may also be referred to inside a declared function's body.

A parameter is **nullable by default**, as a column is. Write ``NOT NULL`` to declare that it never receives a null.

Naming
------

A parameter name is an ordinary identifier: unquoted it is upper-cased, quoted it keeps its spelling, and the connection option ``CASE_SENSITIVE_IDENTIFIERS`` decides which rule applies. A prepared parameter name, on the other hand, is never normalized — neither the ``?name`` in the query text nor the name passed to ``setLong``, ``setNull`` and the rest.

The two are compared as raw strings, so the name a client uses is the declared identifier **after** normalization:

.. code-block:: sql

    CREATE STORED QUERY by_zone("CK___zone_key" BIGINT, adopter_a INTEGER)
        PREPARE FOR (("CK___zone_key" IS NOT NULL, adopter_a IS NOT NULL))
        AS SELECT * FROM t1 WHERE zone_key = "CK___zone_key" AND adopter = adopter_a

``"CK___zone_key"`` is quoted, so it keeps its spelling and a client binds ``?CK___zone_key``. ``adopter_a`` is not, so it becomes ``ADOPTER_A`` and a client binds ``?ADOPTER_A``. Quote a parameter — in the signature, in the prepared cases, and in every reference in the body — whenever the client's spelling is not already upper case.

A parameter name must not collide with a parameter of a declared function, since inside that function's body the two references would be indistinguishable.

Prepared cases
==============

Most plans depend only on the type of a value, so one plan serves every value of that type. A few depend on the value itself: a ``NULL``, or a boolean ``TRUE`` or ``FALSE``, lets the planner fold predicates away, drop index probes and delete whole branches. ``PREPARE FOR`` lists the combinations worth their own plan, and each is warmed separately.

Four states can be written for a parameter:

.. list-table::
    :header-rows: 1
    :widths: 20 80

    * - written
      - warmed for
    * - ``p IS NOT NULL``
      - any non-null value of ``p``'s declared type
    * - ``p IS NULL``
      - a null binding only, with the predicate folded away at plan time
    * - ``p = TRUE``
      - a ``true`` binding only
    * - ``p = FALSE``
      - a ``false`` binding only

``= TRUE`` and ``= FALSE`` require the parameter to be declared ``BOOLEAN``. ``IS NULL`` cannot be written for a parameter declared ``NOT NULL``, since that would contradict the declaration.

Every parameter declared nullable — the default — must be pinned in every case:

.. code-block:: sql

    CREATE STORED QUERY by_zone("CK___zone_key" BIGINT, zone_wide BOOLEAN)
        PREPARE FOR (
            ("CK___zone_key" IS NULL,     zone_wide = FALSE),
            ("CK___zone_key" IS NOT NULL, zone_wide = FALSE)
        )
        AS SELECT * FROM t1 WHERE zone_key = "CK___zone_key" AND wide = zone_wide

Two plans are warmed here. The first has ``"CK___zone_key"`` bound to null while planning, so the planner folds that predicate; the second leaves it without a value and only knows it is not null, so the plan keeps the predicate and can use a zone index.

Because every parameter ends up with a definite state, no two cases can overlap, and there is no rule about which one wins. A binding no case matches — ``zone_wide`` bound to ``true`` above — is not an error: the runtime misses the cache, plans the query with the values in hand and returns correct rows. Only the warm-up is missed. Add a case for a combination that turns out to be hot.

Leaving a nullable parameter unpinned is rejected. Its plan would have to be built with no value and a nullable type, and no single plan is correct for both a null and a non-null binding.

A parameter declared ``NOT NULL`` may be left out, since ``IS NOT NULL`` is the only state its declaration allows and writing it says nothing new. It is recorded as if written, so what is warmed does not depend on whether the author spelled it out:

.. code-block:: sql

    CREATE STORED QUERY by_zone("CK___zone_key" BIGINT NOT NULL, adopter_a INTEGER)
        PREPARE FOR ((adopter_a IS NOT NULL))
        AS SELECT * FROM t1 WHERE zone_key = "CK___zone_key" AND adopter = adopter_a

Examples
========

Declare a stored query as part of a schema template:

.. code-block:: sql

    CREATE SCHEMA TEMPLATE example_template
        CREATE TABLE t1(id BIGINT, col1 BIGINT, PRIMARY KEY(id))
        CREATE INDEX i1 AS SELECT col1 FROM t1
        CREATE STORED QUERY by_col1 AS SELECT * FROM t1 WHERE col1 = 10

``by_col1`` declares no parameters, so it needs no ``PREPARE FOR``. It warms a plan for ``col1 = 10``. Because literal values are stripped during planning, any runtime query of the same shape reuses it regardless of the constant:

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

A signature and a ``DECLARE`` block combine, and a parameter may be captured inside a function's body. ``PREPARE FOR`` comes before ``DECLARE``:

.. code-block:: sql

    CREATE STORED QUERY by_fn(param_a BIGINT, param_b BIGINT)
        PREPARE FOR ((param_a IS NOT NULL, param_b IS NOT NULL))
        DECLARE
            FUNCTION f1(IN p BIGINT) AS (SELECT * FROM t1 WHERE col1 = p AND col2 = param_a)
        AS
            SELECT id FROM f1(param_b)

Here ``param_a`` is captured by ``f1``'s body while ``param_b`` is passed as its argument. The runtime counterpart installs the same temporary function and issues the same query, binding ``?PARAM_A`` and ``?PARAM_B``. The declared function is planned together with the body, once per case, since a captured parameter changes the function's plan too.

See Also
========

* :ref:`CREATE SCHEMA TEMPLATE <schema_template>` - The template that stored queries are declared in
* :ref:`CREATE TEMPORARY FUNCTION <create_temporary_function>` - Same function syntax as the ``DECLARE`` block
