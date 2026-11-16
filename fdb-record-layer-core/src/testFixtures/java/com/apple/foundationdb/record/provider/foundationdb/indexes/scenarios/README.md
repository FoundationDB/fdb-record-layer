# Index Maintainer Scenario Testing Framework

The framework's design, and how to add an index type or a scenario, are documented in this
package's javadoc (`package-info.java`). This file tracks what has been learned while using it.

## Current limitations

So far a few limitations have been found:

### `COUNT_UPDATES`

Some of the existing tests are based on the idea that rebuilding the index should result in the same resulting data. This is not true for `COUNT_UPDATES`, and notably `InsertWhileWriteOnlyAlreadyIndexed` has special handling for `COUNT_UPDATES`, which is not great. In theory there is some abstraction akin to `This index type tracks updates`, which could also be applicable if we added a change-tracking index.

### TimeWindowLeaderboard

This currently has all of the special behavior/overrides. Some more investigation is probably worthwhile to check whether it could be handled better.

### `MAX_EVER`/`MIN_EVER`

There is not an example of this yet, but if we add more examples that delete data (which seem good to have), an additional flag about whether deletes matter would be needed.

### `scanResultsEqual`

This method was added during development to make things work, and is now needed again: Lucene index entries cannot be compared with `Tuple` equality, because their key tuple holds the index's key expression and `Tuple` comparison rejects it (`Unsupported data type: NestingKeyExpression`). `LuceneIndexDefinition` overrides this to compare the matched primary keys instead, which also absorbs Lucene's score ordering.

### Lucene and snapshot isolation

`FDBDirectory` always reads through the plain transaction rather than a snapshot read, so a Lucene search takes read-conflict ranges whatever isolation level is requested, and cannot coexist with a concurrent committed writer. `SnapshotScan` is therefore skipped for Lucene via `supportsSnapshotIsolation()`; making it work would be a maintainer change.

## Future work

### Immediate

- [ ] **Lucene** - with partitioning
- [x] **Lucene** - without partitioning
- [x] **Sliding Window**
- [ ] **Index validation** - particularly for Lucene/Sliding Window, validating their internal details are consistent (i.e. counts). There is a `validateEntries` method on `IndexMaintainer`, but it looks like that is only implemented by `ValueIndexMaintainer`, so it would probably be good to replace that with scrubbing, and a new method on the `IndexDefinition`. This new `IndexDefinition` method would be somewhat different from scrubbing, because it can run across multiple transactions, assuming the data is not changing.
- [ ] **Basic Delete** - a basic delete test
- [ ] **Combine synthetic** - It would be good ot combine synthetic record types with other operations, particularly deletes
