===================
CREATE TYPE AS ENUM
===================

Clause in a :ref:`schema template definition <create-schema-template>` to create an enum type.

Syntax
======

.. raw:: html
    :file: ENUM.diagram.svg


Parameters
==========

``enumName``
    The name of the ``ENUM`` type

``stringLiteral``
    A ``STRING`` representation of an ``ENUM`` value

Example
=======

.. code-block:: sql

    CREATE SCHEMA TEMPLATE TEMP
        CREATE TYPE AS ENUM color ('blue', 'red', 'yellow')
        CREATE TABLE dot(X INTEGER, Y INTEGER, C color, PRIMARY KEY(X, Y));

    -- On a schema that uses the above schema template
    INSERT INTO dot VALUES
        (1, 1, 'blue'),
        (1, 2, 'yellow'),
        (2, 2, 'red'),
        (3, 2, 'red');

    SELECT * FROM dot;

    SELECT * FROM dot WHERE C = 'red';

.. TODO: file tickets because the above queries do not work