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

Snapshot isolation applies only to the reads of the statement it is attached to. Other reads and
writes in the same transaction — and everything else on the connection — continue to use their
normal (serializable) isolation, so a snapshot ``SELECT`` can be freely mixed with serializable reads
and writes in a single transaction. (For how an ``OPTIONS`` clause is scoped in general, see
:doc:`Query options </reference/query_options>`.)

Examples
########

Approximate row limit protected by a count index
-------------------------------------------------

Suppose a table has a ``COUNT(*)`` index and the application wants to cap it at roughly a maximum
number of rows:

.. code-block:: sql

    CREATE TABLE document (id BIGINT, data STRING, PRIMARY KEY(id));
    CREATE INDEX document_count AS SELECT count(*) FROM document;

Before inserting a new row, read the current count and proceed only if it is under the cap:

.. code-block:: sql

    SELECT count(*) AS document_count
    FROM document
    OPTIONS (ISOLATION LEVEL SNAPSHOT);

.. list-table::
    :header-rows: 1

    * - :sql:`document_count`
    * - :json:`3`

Every insert into ``document`` updates the single ``document_count`` index entry. If the count were
read at serializable isolation, that read would conflict with every concurrent insert, effectively
serializing all inserts and causing frequent retries. Reading it at snapshot isolation adds no
conflict range, so concurrent inserts proceed. The trade-off is that the limit becomes
*approximate*: under high concurrency a few rows may slip in past the cap, because each transaction
decides against a count that does not reflect the others' not-yet-committed inserts. This is usually
acceptable for a soft limit.

Sequence-like ids from a ``MAX_EVER`` index and a random offset
---------------------------------------------------------------

Suppose the application needs to assign roughly-increasing ids without a central sequence generator.
A ``MAX_EVER`` index tracks the largest key ever assigned:

.. code-block:: sql

    CREATE TABLE zone (zone_key BIGINT, name STRING, PRIMARY KEY(zone_key));
    CREATE INDEX max_zone_key AS SELECT max_ever(zone_key) FROM zone;

To assign a new key, read the current maximum:

.. code-block:: sql

    SELECT max_ever(zone_key) AS max_key
    FROM zone
    OPTIONS (ISOLATION LEVEL SNAPSHOT);

.. list-table::
    :header-rows: 1

    * - :sql:`max_key`
    * - :json:`250`

The application then adds a small random offset to ``max_key`` and inserts the row with that key
(for example ``INSERT INTO zone VALUES (max_key + <random 1..100>, 'the-name')``). Every insert
updates the single ``max_zone_key`` index entry, so — as in the previous example — reading it at
serializable isolation would conflict with every concurrent id assignment. Reading it at snapshot
isolation avoids that conflict, and the random offset makes it unlikely that two concurrent
assignments choose the same key.

Because snapshot reads do not conflict, two transactions can read the same maximum and act on it
independently, so design for the possibility that another transaction derived the same value. Here
that possibility is handled for free: if two transactions do pick the same new key, the primary-key
write itself conflicts (write-write) and one transaction retries. Widening the random range lowers
the collision probability, at the cost of leaving larger gaps between assigned keys.

Restrictions
############

* The option is only supported on read-only (``SELECT``) statements (and, when resuming one, on
  ``EXECUTE CONTINUATION``). It may be written on an ``INSERT``, ``UPDATE``, or ``DELETE`` statement,
  but is rejected there because mutations rely on serializable reads (for example, when maintaining
  indexes and enforcing primary-key uniqueness) to remain correct.
* Snapshot isolation changes only conflict detection, not visibility. A snapshot read still returns
  data as of the transaction's read version and never sees uncommitted or later-committed writes
  from other transactions. It will include writes from the current transaction.

Continuations
#############

``ISOLATION LEVEL SNAPSHOT`` is a **per-execution** option: it is applied to the execution it is
specified on and is **not** stored in the continuation. When a query is paginated and resumed with
``EXECUTE CONTINUATION``, the resumed execution runs at snapshot isolation only if the option is
specified again on the resuming statement:

.. code-block:: sql

    EXECUTE CONTINUATION ?continuation OPTIONS (ISOLATION LEVEL SNAPSHOT);

If the option is omitted when resuming, the resumed pages fall back to the default (serializable)
isolation and once again add read-conflict ranges — with no error or warning. To keep an entire
paginated scan at snapshot isolation, repeat ``OPTIONS (ISOLATION LEVEL SNAPSHOT)`` on **every**
``EXECUTE CONTINUATION`` call.
