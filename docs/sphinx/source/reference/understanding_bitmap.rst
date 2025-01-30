===================================================
Understanding How Bitmaps Identify Distinct Values
===================================================

.. _understanding-bitmaps:

A bitmap is effectively a byte array in which each bit (zero-based) represents existence (bit = 1) or absence (bit = 0)
of an integer in a table. For example,

.. code-block:: sql

    SELECT bitmap_construct_agg(val) AS bitmap FROM bitmap_test_values;

Returns a byte array ``bitmap`` where if ``bitmap[i][j] = 1``, it means that ``value = 8 * i + j`` exists in the table.

The data can be sparse and very big. To be more efficient, we group the data into "buckets". A distinct value is represented
by the combination of a "bucket number" and a bit that is set in that bitmap. To identify the bucket and the bit that
represents a specific value, use the following functions:

* :ref:`BITMAP_BUCKET_NUMBER <bitmap-bucket-number>` to find the bucket.

* :ref:`BITMAP_BIT_POSITION <bitmap-bit-position>` to find the zero-based position of the bit.

In the FDB Relational Layer, the fixed bitmap bucket size is 10,000 bits (1250 bytes).
For example:

.. list-table::
    :header-rows: 1

    * - :sql:`value`
      - :sql:`BITMAP_BUCKET_NUMBER`
      - :sql:`BITMAP_BIT_POSITION`
    * - :json:`1`
      - :json:`0`
      - :json:`1`
    * - :json:`9999`
      - :json:`0`
      - :json:`9999`
    * - :json:`10000`
      - :json:`10000`
      - :json:`0`

If we create and populate the table ``bitmap_test_values``:

.. code-block:: sql

    CREATE TABLE bitmap_test_values (val INT);
    insert into bitmap_test_values values (1), (20003);

Then the following query:

.. code-block:: sql

    select bitmap_bucket_number(val) as offset,
        bitmap_construct_agg(bitmap_bit_position(val)) as bitmap
        from bitmap_test_values
        group by offset;

Returns:

.. list-table::
    :header-rows: 1

    * - :sql:`OFFSET`
      - :sql:`BITMAP`
    * - :json:`0`
      - :json:`[b'00000010, 0, ...0]`
    * - :json:`2`
      - :json:`[b'00001000, 0, ...0]`
