============
GET_VARIABLE
============

.. _get_variable:

Returns the current value of a named :ref:`transaction variable <set_transaction_variable>`.

Syntax
======

.. raw:: html
    :file: get_variable.diagram.svg

Parameters
==========

``GET_VARIABLE(name)``
    ``name`` is the variable name, written as a bare identifier — the same spelling used on the
    ``SET TRANSACTION VARIABLE`` side, not a string literal.

Returns
=======

The value most recently bound to ``name`` with :ref:`SET TRANSACTION VARIABLE <set_transaction_variable>`
in the current transaction, with the type inferred from that binding.

If ``name`` was set to ``NULL``, ``GET_VARIABLE(name)`` returns ``NULL``. If ``name`` was never
set in the current transaction, ``GET_VARIABLE(name)`` raises ``UNDEFINED_PARAMETER`` — this is
a distinct case from being set to ``NULL``.

Examples
========

Setup
-----

.. code-block:: sql

    CREATE TABLE employees(
        id BIGINT,
        name STRING,
        salary BIGINT,
        PRIMARY KEY(id))

    INSERT INTO employees VALUES
        (1, 'Alice', 100000),
        (2, 'Bob', 110000)

Reading a transaction variable
-------------------------------

.. code-block:: sql

    SET TRANSACTION VARIABLE min_salary = 105000

    SELECT name FROM employees WHERE salary >= GET_VARIABLE(min_salary)

.. list-table::
    :header-rows: 1

    * - :sql:`name`
    * - :json:`"Bob"`

Unset variable raises an error
-------------------------------

.. code-block:: sql

    SELECT name FROM employees WHERE salary >= GET_VARIABLE(never_set)
    -- Error: no value found for variable never_set
    -- Error Code: 42F02 (UNDEFINED_PARAMETER)

See Also
========

* :ref:`SET TRANSACTION VARIABLE <set_transaction_variable>` - Binding a value for the current transaction
