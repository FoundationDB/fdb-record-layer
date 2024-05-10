# FoundationDB Record Layer Lucene Index Documentation

## Data Layout

### Directory subspaces
The `LuceneIndexMaintainer` puts all data in a set of contiguous keys managed by `FDBDirectory` instances.

If the index is grouped, all the data written by the `FDBDirectory` will be prefixed by the grouping key.
If the index is partitioned, all the data written by the `FDBDirectory` will be prefixed by a partition.

So you may have the following formats for the directory subspace:

#### ungrouped, non-partitioned
Everything is prefixed in the same way as every other index
```
[indexspace]
```
#### grouped, non-partitioned
If the grouping key generates the tuples (`group0`, `group1`, ... `group100`) the directories will be prefixed with the keys:
```
[indexspace][group0]
[indexspace][group1]
    ⋮
[indexspace][group100]

```

#### ungrouped, partitioned
The partitioning adds a partition, it also adds adjacent metadata to manage the partitions, so if you have 14 partitions,
it would be something like this:
```
[indexspace][0, t0, pk0] -> partition metadata for records between t0 and t1 (the primary key is added to deduplicate if needed)
[indexspace][0, t1, pk1] -> partition metadata for records greater than t0 (the primary key is added to deduplicate if needed)
    ⋮
[indexspace][0, t1, pk2] -> partition metadata for "most recent" partition
[indexspace][1, 0] -> the data for the directory in the 0th partition 
[indexspace][1, 1]
    ⋮
[indexspace][1, 13]
```

#### grouped, partitioned
If both grouping and partitioning are enabled, the tuples are concatenated.
```
[indexspace][g0, 0, t1, pk0]
[indexspace][g0, 0, t2, pk1]
    ⋮
[indexspace][g0, 0, t3, pk2]
[indexspace][g0, 1, 0]
[indexspace][g0, 1, 1]
    ⋮
[indexspace][g0, 1, 13]
[indexspace][g0, 0, t0, pk3]
[indexspace][g0, 0, t1, pk4]
    ⋮
[indexspace][g0, 0, t2, pk5]
[indexspace][g0, 1, 0]
[indexspace][g0, 1, 1]
    ⋮
[indexspace][g0, 1, 13]
```

### Within a Directory


#### Sequence Space
```
[directory-subspace][0] -> [Atomic Integer]
```
This stores a counter that is used for mapping various data below to an integer.

#### Metadata Space
```
[directory-subspace][1][filename1] -> {id=1, size=234, ..., content=0xabcde234098}
[directory-subspace][1][filename2] -> {id=1, size=234, ...}
    ⋮
```
Lucene is designed to store a bunch of files, this maps file names to an id (created via the sequence number), 
It also stores metadata about the files, such as size, and block size. See `FDBLuceneFileReference`.
For some files, that are always very small, and always read, we store the content here directly, and not in the data 
space (see below).
The key is made up of the fixed `[1]`, followed by a tuple containing the filename.

#### Data Space
```
[directory-subspace][2][1, 0] -> file content
[directory-subspace][2][1, 1] -> file content
[directory-subspace][2][2, 0] -> file content
[directory-subspace][2][2, 1] -> file content
[directory-subspace][2][2, 2] -> file content
    ⋮
```
Each file is broken into a bunch of blocks, of a fixed size.
The key is made up of the fixed `[2]`, followed by a tuple containing the file id (from the file reference in the
Metadata subspace), and a block number. Each block is a fixed  size.

#### Schema subspace
A deprecated subspace of `[3]`

#### PrimaryKey Subspace
```
[directory-subspace][4][pk0][1, 13]
[directory-subspace][4][pk0][2, 33]
[directory-subspace][4][pk1][1, 8]
    ⋮
```
If enabled (via the index options `primaryKeySegmentIndexEnabled` (deprecated) or `primaryKeySegmentIndexV2Enabled`), this stores a
mapping from the records primary key to the segment(s) that have that document. This is used for efficient deletes.
The key is made up of the fixed `[4]`, followed by the primary key, followed by a tuple containing the `segmentId` and
the `docId` within that segment.
The `segmentId` is derived by creating a file reference for a fake `.pky` file for the segment.
Note: A record may exist in multiple segments if the segment is in the process of being merged, or a merge failed part
way through. Lucene will automatically clean this up at some point.

#### FieldInfos Subspace
```
[directory-subspace][5][-2] -> FieldInfos
[directory-subspace][5][8] -> FieldInfos
    ⋮
```
This contains the data associated with the FieldInfos. The key is made up of the fixed `[5]` followed by an id.
The `-2` is a constant id for the global field infos, which will always be there, and we try to add new field there.
In the case where two segments have incompatible Fields, additional entries may be added, using an id fetched from the
sequence counter.

#### Stored Fields Subspace
```
[directory-subspace][6][seg0, 0] -> LuceneStoredFieldsProto.LuceneStoredFields
[directory-subspace][6][seg0, 1] -> LuceneStoredFieldsProto.LuceneStoredFields
[directory-subspace][6][seg1, 0] -> LuceneStoredFieldsProto.LuceneStoredFields
    ⋮
```
This stores a protobuf for the stored fields. The key is made up of the fixed `[6]` followed by a tuple containing the
segment name and the `docId` within the segment.
Enabled via the index options `OPTIMIZED_STORED_FIELDS_FORMAT_ENABLED` or `PRIMARY_KEY_SEGMENT_INDEX_V2_ENABLED`.

#### File Lock Subspace
```
[directory-subspace][7][lockName] -> [uuid, timeStamp]
```
This contains a file lock for the directory. The key is made up of the fixed `[7]` followed by a tuple containing the
lock name that is provided by lucene. This is necessary to support multi-transaction merges.
