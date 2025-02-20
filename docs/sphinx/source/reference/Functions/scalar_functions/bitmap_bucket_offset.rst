====================
BITMAP_BUCKET_OFFSET
====================

.. _bitmap-bucket-offset:

See: :ref:`Understanding How Bitmaps Identify Distinct Values <understanding-bitmaps>`.

Syntax
======

.. code-block:: sql

    BITMAP_BUCKET_OFFSET( <expr> )

Parameters
==========

``expr``
    A numeric expression

Returns
=======

Given a numeric input, returns the "offset" of the input in the bitmap. Essentially, BITMAP_BUCKET_OFFSET + :ref:`BITMAP_BIT_POSITION <bitmap-bit-position>` = INPUT_VALUE.

Example
=======

.. code-block:: sql

    SELECT input, BITMAP_BUCKET_OFFSET(input) FROM T

.. list-table::
    :header-rows: 1

    * - :sql:`input`
      - :sql:`BITMAP_BUCKET_OFFSET`
    * - :json:`0`
      - :json:`0`
    * - :json:`1`
      - :json:`0`
    * - :json:`9999`
      - :json:`0`
    * - :json:`10000`
      - :json:`10000`
    * - :json:`-1`
      - :json:`-10000`