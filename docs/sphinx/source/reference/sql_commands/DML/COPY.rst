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
Of particular note is: `COPY command should export and restore necessary catalog entries <https://github.com/FoundationDB/fdb-record-layer/issues/3816>`_

**Warning**: There are no protections preventing concurrent access. If exporting/importing takes more than one
transaction it is your responsibility to ensure that there are no interactions with the database during this time.

Syntax
======

.. raw:: html
    :file: COPY.diagram.svg

Parameters
==========

``path``
    The name of the database to export, or the target database to import into

``preparedStatementParameter``
    The source data when importing. This should be bound to a list of blobs, as returned by a previous call to ``COPY``


Examples
========

Copy to a different FoundationDB cluster
----------------------------------------

Export all the data. Use ``setMaxRows`` to keep it within a single transaction.

.. code-block:: sql

    COPY /MY/DB

Connect to a different cluster, and with the resulting blobs from the export, break it into lists, and import.
You may need to respond to errors, and import smaller lists within a transaction.

.. code-block:: sql

    COPY /MY/DB FROM ?

Copy to a different part of the same FoundationDB cluster
---------------------------------------------------------

Export all the data. Use ``setMaxRows`` to keep it within a single transaction.

.. code-block:: sql

    COPY /MY/DB

With the resulting blobs from the export, break it into lists, and import.
You may need to respond to errors, and import smaller lists within a transaction.

.. code-block:: sql

    COPY /MY/DB2 FROM ?
