===================
BITMAP_BIT_POSITION
===================

.. _bitmap-bit-position:

See: :ref:`Understanding How Bitmaps Identify Distinct Values <understanding-bitmaps>`.

Syntax
======

.. code-block:: sql

    BITMAP_BIT_POSITION( <expr> )

Parameters
==========

``expr``
    A numeric expression

Returns
=======

Returns the zero-based position of the numeric input in a bitmap (fixed length = 10000).

Example
=======

.. code-block:: sql

    SELECT input, BITMAP_BIT_POSITION(input) FROM T

.. list-table::
    :header-rows: 1

    * - :sql:`input`
      - :sql:`BITMAP_BIT_POSITION`
    * - :json:`0`
      - :json:`0`
    * - :json:`1`
      - :json:`1`
    * - :json:`9999`
      - :json:`9999`
    * - :json:`10000`
      - :json:`0`
    * - :json:`-1`
      - :json:`9999`
