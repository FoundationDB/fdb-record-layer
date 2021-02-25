# FoundationDB Record Layer Lucene Index

The lucene index implementation on the record layer consists of several components.  The first component is a Directory implementation backed by an FDB KeySpace.

* **Directory Implementation** 

A <a href="https://lucene.apache.org/core/7_7_2/core/org/apache/lucene/store/Directory.html">Directory</a> provides an abstraction layer for storing a list of files. A directory contains only files (no sub-folder hierarchy). Implementing classes must comply with the following:
A file in a directory can be created (createOutput(java.lang.String, org.apache.lucene.store.IOContext)), appended to, then closed.
A file open for writing may not be available for read access until the corresponding IndexOutput is closed.
Once a file is created it must only be opened for input (openInput(java.lang.String, org.apache.lucene.store.IOContext)), or deleted (deleteFile(java.lang.String)). Calling createOutput(java.lang.String, org.apache.lucene.store.IOContext) on an existing file must throw FileAlreadyExistsException.

* **Lucene Index Maintenance and Scans**

The LuceneIndexMaintainer provides core functionality for scanning Lucene Indexes backed by FoundationDB.
The core scan interface is the existing <a href="https://lucene.apache.org/core/7_7_3/queryparser/org/apache/lucene/queryparser/classic/package-summary.html#package.description"> Lucene Query Parser syntax</a>.

## Documentation

* [Documentation Home](docs/index.md)
