=========================
SET TRANSACTION VARIABLE
=========================

.. _set_transaction_variable:

Binds a named value to the current transaction. The value can then be read in any SQL statement
executed within that transaction using :ref:`GET_VARIABLE(name) <get_variable>`.

Syntax
======

.. raw:: html
    :file: SET_TRANSACTION_VARIABLE.diagram.svg

Parameters
==========

``name``
    The variable name. Case-insensitive. Must be a valid SQL identifier.

``constant``
    A literal constant value. Expressions are not supported. The type is inferred from the
    literal (e.g. ``42`` is ``BIGINT``, ``'text'`` is ``STRING``, ``NULL`` has no type until the
    variable is next set to a concrete value).

Lifecycle
=========

Variables are scoped to the current transaction:

- A variable set with ``SET TRANSACTION VARIABLE`` is visible to all statements in the same
  transaction.
- When the transaction commits or is aborted, all variables are discarded.
- Setting the same variable twice overwrites the previous value — including changing its type
  (e.g. from ``BIGINT`` to ``STRING``, or from ``NULL`` to a concrete type). This is intentional:
  variables are dynamically typed, and re-binding a variable to a different type is expected to
  work, not be rejected.

Using ``GET_VARIABLE(name)`` in queries
========================================

``GET_VARIABLE(name)`` can appear anywhere a column reference or literal is valid: ``WHERE``
clauses, ``SELECT`` lists, function arguments, subqueries, and function bodies.

.. code-block:: sql

    SET TRANSACTION VARIABLE min_salary = 100000

    SELECT id, name, salary
    FROM employees
    WHERE salary >= GET_VARIABLE(min_salary)

Reading an unset variable
--------------------------

Calling ``GET_VARIABLE(name)`` for a variable that has never been set in the current transaction
raises ``UNDEFINED_PARAMETER``. This is distinct from a variable explicitly set to ``NULL``:
reading a ``NULL``-valued variable succeeds and yields ``NULL``.

.. code-block:: sql

    SET TRANSACTION VARIABLE x = NULL

    -- Succeeds: x was set (to NULL). val = NULL never matches, so no rows are returned.
    SELECT pk FROM tbl WHERE val = GET_VARIABLE(x)

    -- Fails with UNDEFINED_PARAMETER: y was never set.
    SELECT pk FROM tbl WHERE val = GET_VARIABLE(y)

Using variables in function bodies
===================================

Both permanent and temporary functions can reference ``GET_VARIABLE(name)``. The variable is
resolved at query execution time from the calling transaction's variable state.

.. code-block:: sql

    CREATE FUNCTION high_earners(IN dept STRING)
    AS SELECT id, name, salary
       FROM employees
       WHERE department = dept AND salary >= GET_VARIABLE(min_salary)

    SET TRANSACTION VARIABLE min_salary = 110000

    SELECT * FROM high_earners('Engineering')

The function body sees ``GET_VARIABLE(min_salary)`` from the transaction that called it, not from
the transaction that created the function.

Independence from ``?param``
=============================

Transaction variables and ``?name`` (named prepared statement parameters) are independent
namespaces. A query may use both ``GET_VARIABLE(x)`` and ``?x`` with the same name; they resolve
to different values.

.. code-block:: sql

    SET TRANSACTION VARIABLE x = 1

    -- GET_VARIABLE(x) resolves to 1; ?x is supplied separately as a prepared parameter
    SELECT pk FROM tbl WHERE pk = GET_VARIABLE(x) OR pk = ?x

Continuations
=============

When a query is executed, the current value of each referenced variable is captured and baked
into the continuation bytes. If the variable is later changed with ``SET TRANSACTION VARIABLE``
and the continuation is resumed, the original captured value is used — not the new one.

.. code-block:: sql

    SET TRANSACTION VARIABLE min_pk = 2
    -- Execute query, get page 1 continuation

    SET TRANSACTION VARIABLE min_pk = 99
    -- Resuming the continuation still uses min_pk = 2

Plan caching and type changes
==============================

The compiled plan cache accounts for a variable's current type, not just its name. Re-setting a
variable to a value of the same type reuses the previously compiled plan; re-setting it to a
value of a *different* type (including a transition to or from ``NULL``) always compiles a fresh
plan for that type, rather than reusing one shaped for the old type.

.. code-block:: sql

    SET TRANSACTION VARIABLE x = 10
    SELECT pk FROM tbl WHERE val = GET_VARIABLE(x)   -- plan compiled for BIGINT

    SET TRANSACTION VARIABLE x = 'ten'
    SELECT pk FROM tbl WHERE val = GET_VARIABLE(x)   -- different type: compiled fresh, not reused

Examples
========

Setup
-----

.. code-block:: sql

    CREATE TABLE employees(
        id BIGINT,
        name STRING,
        department STRING,
        salary BIGINT,
        PRIMARY KEY(id))

    INSERT INTO employees VALUES
        (1, 'Alice', 'Engineering', 100000),
        (2, 'Bob', 'Engineering', 110000),
        (3, 'Carol', 'Engineering', 150000),
        (4, 'Dave', 'Sales', 80000),
        (5, 'Eve', 'Sales', 120000)

Filtering with a transaction variable
--------------------------------------

.. code-block:: sql

    SET TRANSACTION VARIABLE dept = 'Engineering'

    SELECT id, name FROM employees WHERE department = GET_VARIABLE(dept)

.. list-table::
    :header-rows: 1

    * - :sql:`id`
      - :sql:`name`
    * - :json:`1`
      - :json:`"Alice"`
    * - :json:`2`
      - :json:`"Bob"`
    * - :json:`3`
      - :json:`"Carol"`

Reusing a variable across multiple statements
---------------------------------------------

Within a single transaction, one ``SET TRANSACTION VARIABLE`` applies to all subsequent
statements:

.. code-block:: sql

    SET TRANSACTION VARIABLE threshold = 100000

    SELECT id, name FROM employees WHERE salary > GET_VARIABLE(threshold)
    -- Returns Bob, Carol, Eve

    SELECT COUNT(*) AS cnt FROM employees WHERE salary <= GET_VARIABLE(threshold)
    -- Returns 2 (Alice, Dave)

Overwriting a variable
-----------------------

.. code-block:: sql

    SET TRANSACTION VARIABLE x = 1
    SELECT GET_VARIABLE(x)   -- returns 1

    SET TRANSACTION VARIABLE x = 42
    SELECT GET_VARIABLE(x)   -- returns 42

See Also
========

* :ref:`GET_VARIABLE(name) <get_variable>` - Reading a transaction variable's current value
* :ref:`CREATE FUNCTION <create_function>` - Defining reusable SQL functions that can reference transaction variables
