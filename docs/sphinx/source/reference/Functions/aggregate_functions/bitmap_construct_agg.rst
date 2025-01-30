====================
BITMAP_CONSTRUCT_AGG
====================

.. _bitmap-construct-agg:

See: :ref:`Understanding How Bitmaps Identify Distinct Values <understanding-bitmaps>`.

Syntax
======

.. code-block:: sql

    BITMAP_CONSTRUCT_AGG( <bit_position> )

Parameters
==========

``bit_position``
    A numeric value that represents the BITMAP_BIT_POSITION of an input.

Returns
=======
The function takes the relative position of multiple inputs in the bitmap, and returns a byte array that is a bitmap with bits set for each distinct input.
The result should be grouped by :ref:`BITMAP_BUCKET_OFFSET <bitmap-bucket-offset>` or :ref:`BITMAP_BUCKET_NUMBER <bitmap-bucket-number>`, so that values in different "buckets" are not merged together.
For example:

.. code-block:: sql

    SELECT BITMAP_BUCKET_OFFSET(UID) as offset, BITMAP_CONSTRUCT_AGG(BITMAP_BIT_POSITION(UID)) as bitmap FROM T
    GROUP BY BITMAP_BUCKET_OFFSET(UID)

    ``UID = 0, 1, 9999, 10000, -1``

.. list-table::
    :header-rows: 1

    * - :sql:`OFFSET`
      - :sql:`BITMAP`
    * - :json:`0`
      - :json:`[b'00000011, 0, ..., b'10000000]`
    * - :json:`1`
      - :json:`[b'00000001, 0, ..., 0]`
    * - :json:`-1`
      - :json:`[0, 0, ..., b'10000000]`
