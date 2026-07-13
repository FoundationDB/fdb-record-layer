========================
ISOLATION LEVEL SNAPSHOT
========================

.. _isolation_level_snapshot:

The ``ISOLATION LEVEL SNAPSHOT`` query option runs a ``SELECT`` at FoundationDB's **snapshot
isolation** instead of the default serializable isolation. Under snapshot isolation, the reads
performed by the query do **not** add read-conflict ranges to the enclosing transaction, so
concurrent writes to the data the query reads will not cause the transaction to fail when it
commits.

.. code-block:: sql

    SELECT ... OPTIONS (ISOLATION LEVEL SNAPSHOT)

Overview
########

By default, every read in a transaction is **serializable**: FoundationDB records the range of keys
that was read, and if any of those keys is modified by another transaction that commits first, this
transaction is rejected with a conflict and must be retried. This guarantees that the transaction
sees a consistent view and that its writes are safe, but it also means a read over a wide or
frequently updated range can cause conflicts even when the exact values read do not matter.

A **snapshot** read still observes a consistent, point-in-time view of the database (as of the
transaction's read version), but it does not register a read-conflict range. This makes snapshot
isolation useful when a query reads data that is likely to be written concurrently and the query
does not need its read to participate in conflict detection — for example, sampling an aggregate to
choose a value that only needs to be approximately current.

The option affects only the query it is attached to. Other reads and writes in the same transaction
continue to use their normal (serializable) isolation, so a snapshot-isolation ``SELECT`` can be
freely mixed with serializable reads and writes in a single transaction.

Examples
########

Consider the following table and index:

.. code-block:: sql

    CREATE TABLE account (id BIGINT, balance BIGINT, PRIMARY KEY(id));
    CREATE INDEX max_balance_ever AS SELECT max_ever(balance) FROM account;

with these records:

.. code-block:: json

    {"ID": 1, "BALANCE": 100}
    {"ID": 2, "BALANCE": 250}
    {"ID": 3, "BALANCE": 175}

Reading an aggregate at snapshot isolation reads the aggregate index without adding a conflict
range, so a concurrent transaction that inserts or updates an account will not cause this read's
transaction to conflict:

.. code-block:: sql

    SELECT max_ever(balance) AS max_balance
    FROM account
    OPTIONS (ISOLATION LEVEL SNAPSHOT);

.. list-table::
    :header-rows: 1

    * - :sql:`max_balance`
    * - :json:`250`

The option applies to any ``SELECT``, not just aggregates. A regular scan can also be run at
snapshot isolation:

.. code-block:: sql

    SELECT id, balance
    FROM account
    WHERE id = 2
    OPTIONS (ISOLATION LEVEL SNAPSHOT);

.. list-table::
    :header-rows: 1

    * - :sql:`id`
      - :sql:`balance`
    * - :json:`2`
      - :json:`250`

Restrictions
############

* The option is only supported on read-only (``SELECT``) queries. Using it on an ``INSERT``,
  ``UPDATE``, ``DELETE``, or DDL statement raises an error. This is because mutations rely on
  serializable reads (for example, when maintaining indexes and enforcing primary-key uniqueness)
  to remain correct.
* Snapshot isolation changes only conflict detection, not visibility. A snapshot read still returns
  data as of the transaction's read version and never sees uncommitted or later-committed writes
  from other transactions. It will include writes from the current transaction.

.. note::

    Because snapshot reads do not conflict, two transactions can each read the same value and act on
    it independently. When using snapshot isolation to derive a value that is then written back,
    design for the possibility that another transaction derived the same value. See the
    :doc:`FAQ </FAQ>` for an example of using a ``MAX_EVER`` index at snapshot isolation together with
    a random offset to generate keys with a low probability of collision.
