====================
BITMAP_BUCKET_NUMBER
====================

.. _bitmap-bucket-number:

See: :ref:`Understanding How Bitmaps Identify Distinct Values <understanding-bitmaps>`.

Syntax
======

.. code-block:: sql

    BITMAP_BUCKET_NUMBER( <expr> )

Parameters
==========

``expr``
    A numeric expression

Returns
=======

Given a numeric input, returns a “bucket number” that finds the bitmap (fixed length = 10000) that has the input value.
If the input in range [0, 9999], output = 0;
if the input in range [10000, 19999], output = 1;
if the input is negative, the output is negative as well -- if input in range [-10000, 0), output = -1, etc.

Example
=======

.. code-block:: sql

    SELECT input, BITMAP_BUCKET_NUMBER(input) FROM T

.. list-table::
    :header-rows: 1

    * - :sql:`input`
      - :sql:`BITMAP_BUCKET_NUMBER`
    * - :json:`0`
      - :json:`0`
    * - :json:`1`
      - :json:`0`
    * - :json:`9999`
      - :json:`0`
    * - :json:`10000`
      - :json:`1`
    * - :json:`-1`
      - :json:`-1`
