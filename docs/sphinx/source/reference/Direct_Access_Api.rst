=================
Direct Access API
=================

The Relational Layer's Direct Access API is a simplified API for accessing data within specific ranges.
It avoids using SQL directly, but it uses the same interface as the SQL connection to allow intermingling of
SQL queries with these simpler operations. The intent is to provide highly simplified access for common
current use-cases (e.g., scanning records within a specific range), but it is *highly* limited. For more
powerful query execution, use the :doc:`SQL query language <sql_commands>` instead.

The Direct Access API consists of the following operations:

* Scan
* Get
* Insert
* Delete

Scan
####

Scans can access all records which fit within a specified primary key or index key range. These scans can be arbitrarily large, based on the data and the range of interest, and thus continuations are provided to enable paginated queries. For example

.. code-block:: java

    try (RelationalStatement statement = createStatement()) {
        TableScan scan = TableScan.newBuilder()
            .setStartKey(<startKeyName>, <startKeyValue>)
            // fill in the rest of the starting keys...
            .setEndKey(<endKeyName>, <endKeyValue>)
            // fill in the rest of the ending keys...
            .build();
        Continuation continuation = Continuation.BEGIN;
        int continuationBatchSize = 10;
        while (!continuation.isEnd()) {
            int rowCount = 0;
            try (RelationalResultSet scanResultSet = statement.executeScan(<tableName>, scan, <options>) {
                while(scanResultSet.next()) {
                    //process a returned record
                    rowCount++;
                    if (rowCount == continuationBatchSize) {
                        continuation = scanResultSet.getContinuation();
                        break;
                    }
                }
            }
        }
    }


Scan keys can be partial, as long as they are contiguous. You can specify :java:`pk1, pk2`, but not :java:`pk1, pk3`;
otherwise, the API will throw an error.

The returned :java:`ResultSet` can access a continuation, which is a pointer to the *first row which has not yet been read*.
Put another way, if the *current* location in the :java:`ResultSet` is position :java:`X`, then the continuation will point to position :java:`X+1`.
This can be used as a marker for restarting a given query. By passing the continuation to the API when executing the
same statement again, the continuation will be used to resume the execution at the specified location.

Get
###

The Get operation provides the ability to specify the primary key of a certain record, and quickly return the record
associated with that Primary Key from a specific table. If the record does not exist, then the result set will be empty.

.. code-block:: java

    try (RelationalStatement statement = createStatement()) {
        KeySet primaryKey = new KeySet()
             .setKeyColumn("<pkColumn1Name>", <pkColumn1Value>)
             // set the other values for the primary key of the table...
        try (RelationalResultSet recordResultSet = statement.executeGet(<tableName>, primaryKey, <options>)) {
            if (recordResultSet.next()){
                //process the returned record -- there will always be no more than 1 record in the returned result set.
            }
        }
    }

If you specify an incomplete :java:`KeySet` (i.e., an incomplete primary key), then the API will throw an error.

Insert
######

The Insert API provides a way to insert a data element into a specific table using programmatic API. The API requires
building a :java:`DynamicMessage`, as follows:

.. code-block:: java

    try (RelationalStatement statement = createStatement()) {
        DynamicMessageBuilder  messageBuilder = statement.getMessageBuilder(<tableName>)
            .setField(<fieldName>, <fieldValue>)
            // set the other fields in the record, including nested or repeated structures...
        statement.executeInsert(<tableName>, messageBuilder.build(), <options>);
    }

You can also insert multiple records together in batch, using an iterable interface of build records.

Delete
######

Deletes are very similar to inserts, except that you specify the primary keys of the rows that you want to delete:

.. code-block:: java

    try (RelationalStatement statement = createStatement()) {
        KeySet primaryKey = new KeySet()
            .setKeyColumn("<pkColumn1Name>", <pkColumn1Value>)
            // set the other values for the primary key of the table...
        statement.executeDelete(<tableName>, primaryKey, <options>);
    }
