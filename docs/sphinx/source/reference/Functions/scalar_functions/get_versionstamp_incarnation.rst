============================
GET_VERSIONSTAMP_INCARNATION
============================

The incarnation is intended to be incremented when moving data from one cluster.
By combining the incarnation with the `__ROW_VERSION` field you can maintain proper
ordering of inserts/updates even when data is moved between clusters.

There is currently no automatic updating of the version, see: 
`Issue #3866 <https://github.com/FoundationDB/fdb-record-layer/issues/3866>`_

Syntax
======

.. code-block:: sql

    GET_VERSIONSTAMP_INCARNATION()

Parameters
==========


Returns
=======

Returns the current incarnation of the schema.

Example
=======

Insert
------

.. code-block:: sql

    INSERT INTO T (key, incarnation, data) VALUES ('r1', get_versionstamp_incarnation(), 'something0')

.. code-block:: sql

    SELECT key, incarnation FROM T

.. list-table::
    :header-rows: 1

    * - :sql:`key`
      - :sql:`incarnation`
    * - :json:`"r1"`
      - :json:`0`

Update
------

.. code-block:: sql

    UPDATE T set incarnation=get_versionstamp_incarnation(), data='banana' WHERE key='r1'

.. code-block:: sql

    SELECT key, incarnation, data FROM T

.. list-table::
    :header-rows: 1

    * - :sql:`key`
      - :sql:`incarnation`
      - :sql:`data`
    * - :json:`"r1"`
      - :json:`0`
      - :json:`"banana"`



