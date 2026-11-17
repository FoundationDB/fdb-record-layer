# Index Maintainer Scenario Testing Framework

The framework's design, and how to add an index type or a scenario, are documented in this
package's javadoc (`package-info.java`). This file tracks what has been learned while using it.

## Current limitations

So far a few limitations have been found:

### `COUNT_UPDATES`

Some of the scenarios rely on rebuilding the index giving the same data as maintaining it. That is not true for `COUNT_UPDATES` when a record is saved twice, because it counts every save. Definitions say whether saving an unchanged record leaves the index unchanged through `IndexDefinition.ignoresUnchangedSaves()`, and `InsertWhileWriteOnlyAlreadyIndexed` uses that to skip its final comparison. The flag only covers repeated saves; a change-tracking index, or deletes with `MAX_EVER`/`MIN_EVER` (below), would need something broader.

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
