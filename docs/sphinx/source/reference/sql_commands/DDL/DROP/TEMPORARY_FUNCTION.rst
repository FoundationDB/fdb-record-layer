========================
DROP TEMPORARY FUNCTION
========================

.. _drop_temporary_function:

Drops a temporary function within the current transaction.

Syntax
======

.. raw:: html
    :file: TEMPORARY_FUNCTION.diagram.svg

.. code-block:: sql

    DROP TEMPORARY FUNCTION [IF EXISTS] function_name

Parameters
==========

``IF EXISTS``
    If specified, the statement is a no-op when the named temporary function does not exist. Without this clause, dropping a non-existent function raises an error.

``function_name``
    The name of the temporary function to drop. Must be a temporary function created in the current transaction.

Examples
========

Drop an existing temporary function:

.. code-block:: sql

    DROP TEMPORARY FUNCTION my_temp_func

Drop a temporary function without error if it does not exist:

.. code-block:: sql

    DROP TEMPORARY FUNCTION IF EXISTS my_temp_func

Important Notes
===============

Temporary functions are automatically dropped when the transaction commits. ``DROP TEMPORARY FUNCTION`` allows earlier removal within the same transaction — for example, to free the name for redefinition without using ``OR REPLACE``.

See Also
========

* :ref:`CREATE TEMPORARY FUNCTION <create_temporary_function>` - Create a transaction-scoped function
* :ref:`CREATE FUNCTION <create_function>` - Create a permanent function
