# FoundationDB Record Layer Lucene Index Documentation

## Data Layout

### Sequence Space 
#### {Directory Bytes}{indexspace}{key=0 value={atomic integer}}
This first section stores the atomic integer to make sure temporary files prior to renaming are unique.

### Metadata Space 
#### {Directory Bytes}{indexspace}{1}[{key=file123, value={id=1, size=2018, blockSize=1024}},{key=file12345, value={id=2, size=124, blockSize=1024}}]
The metadata is keyed by filename and in the value it provides the total size, the block size, and an id to lookup the data.

### Data Space 
#### {Directory Bytes}{indexspace}{2}[{key=(1,1), value=byte[]},{key=(1,2), value=byte[]},{key=(2,1), value=byte[]}]
The data section is keyed via the files id in the metadata directory coupled with a block number.
