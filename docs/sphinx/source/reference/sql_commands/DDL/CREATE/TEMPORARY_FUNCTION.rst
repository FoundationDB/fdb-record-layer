==========================
CREATE TEMPORARY FUNCTION
==========================

.. _create_temporary_function:

Creates a transaction-scoped user-defined function that is automatically dropped when the transaction commits.

Syntax
======

.. raw:: html
    :file: TEMPORARY_FUNCTION.diagram.svg

.. raw:: html
    :file: FUNCTION_params.diagram.svg

.. code-block:: sql

    CREATE [OR REPLACE] TEMPORARY FUNCTION function_name (
        [IN] parameter_name data_type [DEFAULT default_value], ...
    ) ON COMMIT DROP FUNCTION AS query

Parameters
==========

``OR REPLACE``
    If specified, replaces an existing temporary function with the same name in the current transaction. Without this clause, creating a function with a name that already exists raises an error.

``function_name``
    The name of the temporary function to create. Scoped to the current transaction.

``parameter_name``
    The name of a function parameter. Parameters can be referenced in the function body using their names.

``data_type``
    The SQL data type of the parameter (e.g., ``BIGINT``, ``STRING``, ``INTEGER``).

``default_value``
    Optional default value for the parameter. If provided, the parameter becomes optional when calling the function.

``query``
    The SQL SELECT statement that defines the function body.

Returns
=======

Returns the result of executing the function's query with the provided parameter values.

Examples
========

Setup
-----

For these examples, assume we have a ``restaurants`` table:

.. code-block:: sql

    CREATE TABLE restaurants(
        rest_no BIGINT,
        name STRING,
        city STRING,
        PRIMARY KEY(rest_no))

    INSERT INTO restaurants VALUES
        (1001, 'The Burger Place', 'New York'),
        (1002, 'Pizza Palace',     'New York'),
        (2001, 'Sushi World',      'San Francisco'),
        (2002, 'Taco Town',        'San Francisco')

Basic Temporary Function
------------------------

Create a temporary function scoped to the current transaction:

.. code-block:: sql

    CREATE TEMPORARY FUNCTION high_no_restaurants()
    ON COMMIT DROP FUNCTION AS
        SELECT * FROM restaurants WHERE rest_no > 1000

Call the function like a regular table-valued function:

.. code-block:: sql

    SELECT * FROM high_no_restaurants()

The function is automatically dropped when the transaction commits.

Replacing an Existing Temporary Function
-----------------------------------------

Use ``OR REPLACE`` to redefine a temporary function within the same transaction:

.. code-block:: sql

    CREATE OR REPLACE TEMPORARY FUNCTION high_no_restaurants()
    ON COMMIT DROP FUNCTION AS
        SELECT * FROM restaurants WHERE rest_no > 2000

Temporary Function with Parameters
------------------------------------

Temporary functions support the same parameter syntax as permanent functions:

.. code-block:: sql

    CREATE TEMPORARY FUNCTION restaurants_in_city(IN target_city STRING)
    ON COMMIT DROP FUNCTION AS
        SELECT rest_no, name FROM restaurants WHERE city = target_city

.. code-block:: sql

    SELECT * FROM restaurants_in_city('New York')

.. list-table::
    :header-rows: 1

    * - :sql:`rest_no`
      - :sql:`name`
    * - :json:`1001`
      - :json:`"The Burger Place"`
    * - :json:`1002`
      - :json:`"Pizza Palace"`

Dropping Early
--------------

A temporary function can be dropped explicitly before the transaction commits using :ref:`DROP TEMPORARY FUNCTION <drop_temporary_function>`:

.. code-block:: sql

    DROP TEMPORARY FUNCTION high_no_restaurants

Important Notes
===============

Transaction Scope
-----------------

Temporary functions exist only for the duration of the transaction in which they are created. They are automatically dropped on commit and are not visible to other transactions or after the transaction ends.

Relationship to Permanent Functions
-------------------------------------

Temporary functions are distinct from permanent functions created with :ref:`CREATE FUNCTION <create_function>`. Permanent functions are stored at the schema template level and persist across transactions. Creating a temporary function with the same name as an existing permanent function raises a ``DUPLICATE_FUNCTION`` error.

Parameter Syntax
----------------

Temporary functions share the same parameter declaration syntax as permanent functions. See :ref:`CREATE FUNCTION <create_function>` for full details on positional parameters, named parameters, and default values.

See Also
========

* :ref:`DROP TEMPORARY FUNCTION <drop_temporary_function>` - Explicitly drop a temporary function
* :ref:`CREATE FUNCTION <create_function>` - Create a permanent function
