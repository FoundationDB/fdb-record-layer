====
LIKE
====

.. _like:

Tests whether a string matches a pattern using wildcards.

Syntax
======

.. raw:: html
    :file: LIKE.diagram.svg

The LIKE operator is used in WHERE clauses:

.. code-block:: sql

    SELECT column1, column2
    FROM table_name
    WHERE column1 LIKE 'pattern%'

Parameters
==========

``expression [NOT] LIKE pattern [ESCAPE escape_char]``
    Tests whether a string expression matches a pattern containing wildcards.

``expression``
    The string value to test. Must be of type STRING.

``pattern``
    A string literal containing the pattern to match. Supports two wildcards:

    - ``%`` - Matches zero or more characters
    - ``_`` - Matches exactly one character

``NOT`` (optional)
    Negates the result - returns true if the expression does **not** match the pattern.

``ESCAPE escape_char`` (optional)
    Specifies a single-character escape sequence to treat wildcard characters (``%`` or ``_``) as literals in the pattern.

Returns
=======

Returns:
- ``TRUE`` if the expression matches the pattern
- ``FALSE`` if the expression does not match the pattern
- ``NULL`` if either the expression or pattern is NULL

Examples
========

Setup
-----

For these examples, assume we have a ``products`` table:

.. code-block:: sql

    CREATE TABLE products(
        id BIGINT,
        name STRING,
        category STRING,
        PRIMARY KEY(id))

    INSERT INTO products VALUES
        (1, 'apple', 'fruit'),
        (2, 'application', 'software'),
        (3, 'appliance', 'hardware'),
        (4, 'banana', 'fruit'),
        (5, 'bench', 'furniture'),
        (6, 'canal', 'infrastructure'),
        (7, 'cabal', 'organization')

Prefix Matching with %
----------------------

Find all products whose names start with "app":

.. code-block:: sql

    SELECT name
    FROM products
    WHERE name LIKE 'app%'

.. list-table::
    :header-rows: 1

    * - :sql:`name`
    * - :json:`"apple"`
    * - :json:`"application"`
    * - :json:`"appliance"`

Suffix Matching with %
----------------------

Find all products whose names end with "tion":

.. code-block:: sql

    SELECT name
    FROM products
    WHERE name LIKE '%tion'

.. list-table::
    :header-rows: 1

    * - :sql:`name`
    * - :json:`"application"`

Substring Matching with %
-------------------------

Find all products whose names contain "an":

.. code-block:: sql

    SELECT name
    FROM products
    WHERE name LIKE '%an%'

.. list-table::
    :header-rows: 1

    * - :sql:`name`
    * - :json:`"banana"`
    * - :json:`"canal"`
    * - :json:`"appliance"`
    * - :json:`"application"`

Single Character Matching with _
---------------------------------

Exact pattern - "c", any char, then "nal":

.. code-block:: sql

    SELECT name
    FROM products
    WHERE name LIKE 'c_nal'

.. list-table::
    :header-rows: 1

    * - :sql:`name`
    * - :json:`"canal"`

Combining % and _
-----------------

Match patterns with multiple wildcards:

.. code-block:: sql

    SELECT name
    FROM products
    WHERE name LIKE '_a%'

.. list-table::
    :header-rows: 1

    * - :sql:`name`
    * - :json:`"banana"`
    * - :json:`"canal"`
    * - :json:`"cabal"`

This matches any name where the second character is 'a'.

NOT LIKE
--------

Find products that don't match a pattern:

.. code-block:: sql

    SELECT name
    FROM products
    WHERE name NOT LIKE 'app%'

.. list-table::
    :header-rows: 1

    * - :sql:`name`
    * - :json:`"banana"`
    * - :json:`"bench"`
    * - :json:`"canal"`
    * - :json:`"cabal"`

ESCAPE Clause
-------------

To search for literal ``%`` or ``_`` characters, use the ESCAPE clause:

.. code-block:: sql

    CREATE TABLE files(
        id BIGINT,
        filename STRING,
        PRIMARY KEY(id))

    INSERT INTO files VALUES
        (1, 'report_2024.pdf'),
        (2, 'data%summary.txt'),
        (3, 'test_file.csv')

    -- Find files with literal underscore before a digit
    SELECT filename
    FROM files
    WHERE filename LIKE '%\\_%' ESCAPE '\\'

.. list-table::
    :header-rows: 1

    * - :sql:`filename`
    * - :json:`"report_2024.pdf"`
    * - :json:`"test_file.csv"`

With the ESCAPE clause, ``\\_`` matches a literal underscore character.

Important Notes
===============

Case Sensitivity
----------------

LIKE comparisons are **case-sensitive**. ``'ABC' LIKE 'abc'`` returns ``FALSE``.

To perform case-insensitive matching, convert both sides to the same case:

.. code-block:: sql

    WHERE LOWER(name) LIKE LOWER('APP%')

NULL Handling
-------------

If either the expression or pattern is NULL, LIKE returns NULL:

.. code-block:: sql

    WHERE NULL LIKE 'pattern'     -- Returns NULL
    WHERE name LIKE NULL           -- Returns NULL

Performance Considerations
--------------------------

**Leading wildcards** (e.g., ``'%pattern'`` or ``'%pattern%'``) prevent the use of index scans and may result in full table scans. For optimal performance, avoid leading wildcards when possible.

Patterns like ``'prefix%'`` (no leading wildcard) can utilize indexes for efficient lookups.

Wildcard Summary
----------------

.. list-table::
    :header-rows: 1

    * - Wildcard
      - Meaning
      - Example
      - Matches
    * - ``%``
      - Zero or more characters
      - ``'app%'``
      - ``'app'``, ``'apple'``, ``'application'``
    * - ``_``
      - Exactly one character
      - ``'c_t'``
      - ``'cat'``, ``'cot'``, ``'cut'``
    * - ``\\%`` (with ESCAPE)
      - Literal ``%``
      - ``'50\\%' ESCAPE '\\'``
      - ``'50%'``
    * - ``\\_`` (with ESCAPE)
      - Literal ``_``
      - ``'test\\_file' ESCAPE '\\'``
      - ``'test_file'``

Supported Types
===============

LIKE only works with STRING types. Attempting to use LIKE with other types (INTEGER, BIGINT, BYTES, etc.) will result in a type error.

See Also
========

* :ref:`Comparison Operators <comparison-operators>` - Other comparison operations
