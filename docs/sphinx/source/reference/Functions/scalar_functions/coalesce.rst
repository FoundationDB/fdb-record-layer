========
COALESCE
========

.. _coalesce:

Returns the first non-NULL value from a list of expressions.

Syntax
======

.. raw:: html
    :file: coalesce.diagram.svg

Parameters
==========

``COALESCE(expression1, expression2, ...)``
    Evaluates expressions from left to right and returns the first non-NULL value. Requires at least two expressions.

Returns
=======

Returns the first non-NULL value with the same type as the input expressions. If all values are NULL, returns NULL. All expressions must be of compatible types.

Supported Types
================

``COALESCE`` supports all data types:

* Primitive types: ``STRING``, ``BOOLEAN``, ``DOUBLE``, ``FLOAT``, ``INTEGER``, ``BIGINT``, ``BYTES``
* Complex types: ``ARRAY``, ``STRUCT``

Examples
========

Setup
-----

For these examples, assume we have a ``contacts`` table:

.. code-block:: sql

    CREATE TABLE contacts(
        id BIGINT,
        name STRING,
        email STRING,
        phone STRING,
        address STRING,
        PRIMARY KEY(id))

    INSERT INTO contacts VALUES
        (1, 'Alice', 'alice@example.com', NULL, NULL),
        (2, 'Bob', NULL, '555-0123', NULL),
        (3, 'Charlie', NULL, NULL, '123 Main St'),
        (4, 'David', NULL, NULL, NULL)

COALESCE - Find First Available Contact Method
------------------------------------------------

Get the first available contact method for each person:

.. code-block:: sql

    SELECT name, COALESCE(email, phone, address, 'No contact info') AS contact_method
    FROM contacts

.. list-table::
    :header-rows: 1

    * - :sql:`name`
      - :sql:`contact_method`
    * - :json:`"Alice"`
      - :json:`"alice@example.com"`
    * - :json:`"Bob"`
      - :json:`"555-0123"`
    * - :json:`"Charlie"`
      - :json:`"123 Main St"`
    * - :json:`"David"`
      - :json:`"No contact info"`

COALESCE with Default Values
------------------------------

Provide default values for NULL fields:

.. code-block:: sql

    SELECT name,
           COALESCE(email, 'no-email@example.com') AS email,
           COALESCE(phone, 'Unknown') AS phone
    FROM contacts

.. list-table::
    :header-rows: 1

    * - :sql:`name`
      - :sql:`email`
      - :sql:`phone`
    * - :json:`"Alice"`
      - :json:`"alice@example.com"`
      - :json:`"Unknown"`
    * - :json:`"Bob"`
      - :json:`"no-email@example.com"`
      - :json:`"555-0123"`
    * - :json:`"Charlie"`
      - :json:`"no-email@example.com"`
      - :json:`"Unknown"`
    * - :json:`"David"`
      - :json:`"no-email@example.com"`
      - :json:`"Unknown"`

COALESCE in UPDATE Statements
-------------------------------

Use COALESCE to update only NULL values:

.. code-block:: sql

    UPDATE contacts
    SET email = COALESCE(email, 'default@example.com')
    WHERE id = 2

This sets the email to ``'default@example.com'`` only if it was NULL, otherwise keeps the existing value.

Important Notes
===============

* ``COALESCE`` returns the first non-NULL value from left to right
* If all values are NULL, ``COALESCE`` returns NULL
* All expressions must be of compatible types
* The function requires at least two arguments
* ``COALESCE`` supports all data types, including complex types like ``STRUCT`` and ``ARRAY``
* Commonly used for providing default values when NULL is encountered
