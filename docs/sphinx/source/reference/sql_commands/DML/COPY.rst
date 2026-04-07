======
COPY
======

SQL command to import/export raw data for a database.
This command has two flavors, one "export" is for exporting the entire contents of a database, in a raw/opaque
format. The second is to "import" the data that was exported. It is the responsibility of the caller to break the export
and import into chunks that will fit into a single transaction.
This should be called while connected to a different database/schema such as the CATALOG.

Note: this is an experimental feature under active development, see:
`Issue #3571 <https://github.com/FoundationDB/fdb-record-layer/issues/3571>`_

**Warning**: There are no protections preventing concurrent access. If exporting/importing takes more than one
transaction it is your responsibility to ensure that there are no interactions with the database during this time.

**Warning**: This does not validate existing data, or clear before inserting. It is your responsibility to ensure that
the target is clear. See: `Issue #4005 <https://github.com/FoundationDB/fdb-record-layer/issues/4005>`_

**Note**: Like other update commands, if autoCommit is enabled, import will commit when the resultSet is closed.

Syntax
======

.. raw:: html
    :file: COPY.diagram.svg

Parameters
==========

``path``
    The name of the database to export, or the target database to import into

``PRESERVE INCARNATION``
    Export data without modifying the incarnation value in the store header.
    This can be used this when copying within the same cluster.

``INCREMENT INCARNATION``
    Export data with the incarnation value in the store header incremented by 1, if the incarnation is currently greater
    than 0.
    Use this when copying to a different cluster, so the destination has a higher incarnation
    than the source to reflect that data has been moved. This ensures that records written with
    :doc:`/reference/Functions/scalar_functions/get_versionstamp_incarnation` can be correctly
    attributed to the cluster where they were written.

``preparedStatementParameter``
    The source data when importing. This should be bound to a list of blobs, as returned by a previous call to ``COPY``


Examples
========

Copy to a different FoundationDB cluster
----------------------------------------

Export all the data with an incremented incarnation. Use ``setMaxRows`` to keep it within a single transaction.

.. code-block:: sql

    COPY /MY/DB INCREMENT INCARNATION

Connect to a different cluster, and with the resulting blobs from the export, break it into lists, and import.
You may need to respond to errors, and import smaller lists within a transaction.

.. code-block:: sql

    COPY /MY/DB FROM ?

Copy to a different part of the same FoundationDB cluster
---------------------------------------------------------

Export all the data, preserving the incarnation. Use ``setMaxRows`` to keep it within a single transaction.

.. code-block:: sql

    COPY /MY/DB PRESERVE INCARNATION

With the resulting blobs from the export, break it into lists, and import.
You may need to respond to errors, and import smaller lists within a transaction.

.. code-block:: sql

    COPY /MY/DB2 FROM ?
