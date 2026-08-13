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
    * - :json:`"appliance"`
    * - :json:`"application"`
    * - :json:`"banana"`
    * - :json:`"canal"`

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

    -- Find files with literal underscore
    SELECT filename
    FROM files
    WHERE filename LIKE '%\_%' ESCAPE '\'

.. list-table::
    :header-rows: 1

    * - :sql:`filename`
    * - :json:`"report_2024.pdf"`
    * - :json:`"test_file.csv"`

With the ESCAPE clause, ``\_`` matches a literal underscore character.

Note that if the ESCAPE clause is set, then any special character following the escape character is
treated as a literal. This means that, if the escape character is set to ``\`` as above, that
``\_`` matches the literal underscore, ``\%`` matches the literal percent sign, and
``\\`` matches the (single) literal backslash. It is an error (code: 22025 `invalid_escape_sequence`)
to follow the escape character with a non-special character. It is also an error to try and use
either ``%`` or ``_`` as the escape character (code: 2200B `escape_character_conflict`) or to
use an empty or multi-character escape sequence (code: 22019 `invalid_escape_character`).

Using the example schema above, if we insert the following values:

.. code-block:: sql

    INSERT INTO files VALUES
        (4, 'final_report.pdf'),
        (5, 'final-report.pdf'),
        (6, 'final\-report.pdf')

We get different result sets with different escape values:

.. code-block:: sql

    SELECT filename
    FROM files
    WHERE filename LIKE 'final_report.pdf'

.. list-table::
    :header-rows: 1

    * - :sql:`filename`
    * - :json:`"final_report.pdf"`
    * - :json:`"final-report.pdf"`

.. code-block:: sql

    SELECT filename
    FROM files
    WHERE filename LIKE 'final\_report.pdf' ESCAPE '\'

.. list-table::
    :header-rows: 1

    * - :sql:`filename`
    * - :json:`"final_report.pdf"`

.. code-block:: sql

    SELECT filename
    FROM files
    WHERE filename LIKE 'final\\_report.pdf' ESCAPE '\'

.. list-table::
    :header-rows: 1

    * - :sql:`filename`
    * - :json:`"final\-report.pdf"`

While these examples all throw errors:

.. code-block:: sql

    SELECT filename
    FROM files
    WHERE filename LIKE 'final%_report.pdf' ESCAPE '%'

.. list-table::
    :header-rows: 2

    * - :sql:`code`
    * - :sql:`error`
    * - :sql:`2200B`
    * - :json:`escape_character_conflict`

.. code-block:: sql

    SELECT filename
    FROM files
    WHERE filename LIKE 'final##_report.pdf' ESCAPE '##'

.. list-table::
    :header-rows: 2

    * - :sql:`code`
    * - :sql:`error`
    * - :sql:`22019`
    * - :json:`invalid_escape_character`

.. code-block:: sql

    SELECT filename
    FROM files
    WHERE filename LIKE 'rfrirnrarlr_rrrerprorrrtr.rprdrf' ESCAPE 'r'

.. list-table::
    :header-rows: 2

    * - :sql:`code`
    * - :sql:`error`
    * - :sql:`22025`
    * - :json:`invalid_escape_sequence`

Important Notes
===============

Case Sensitivity
----------------

LIKE comparisons are **case-sensitive**. ``'ABC' LIKE 'abc'`` returns ``FALSE``.

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
    * - ``\%`` (with ESCAPE)
      - Literal ``%``
      - ``'50\%' ESCAPE '\'``
      - ``'50%'``
    * - ``\_`` (with ESCAPE)
      - Literal ``_``
      - ``'test\_file' ESCAPE '\'``
      - ``'test_file'``

Supported Types
===============

LIKE only works with STRING types. Attempting to use LIKE with other types (INTEGER, BIGINT, BYTES, etc.) will result in a type error.

See Also
========

* :ref:`Comparison Operators <comparison-operators>` - Other comparison operations
