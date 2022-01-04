/*
 * package-info.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2021 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Support for {@code LUCENE} indexes and queries.
 *
 * <p>
 * Lucene indexes are backed by FDB, using {@link com.apple.foundationdb.record.lucene.directory.FDBDirectory} to implement a virtual file system holding the inverted index files.
 * This is not fundamental, though. This maintainer used standard {@link org.apache.lucene.index.IndexWriter} and {@link org.apache.lucene.index.IndexReader}, gotten with {@link com.apple.foundationdb.record.lucene.IndexWriterCommitCheckAsync#getOrCreateIndexWriter getOrCreateIndexWriter}, for interfacing to Lucene.
 * </p>
 *
 * <p>
 * The index definition can be grouped. Each group represents an entirely separate Lucene index.
 * </p>
 *
 * <p>
 * Within a group, each record is represented by a single Lucene document.
 * Fields to be included in the document are given by a {@code concat} expression. Unlike most indexes, the order of fields here does not matter for what queries are possible.
 * Repeated record fields turn into multiple document fields.
 * Fields in nested submessages, possibly repeated, are flattened into document fields with longer field names, representing the path through the record.
 * </p>
 *
 * <p>
 * Basic support for correlation is provided by allowing a nested field's string value to contribute to the document field name.
 * This is well suited to {@code map}-like fields where the keys are from a small, known set.
 * </p>
 *
 * <p>
 * Fields are designated for full text tokenization, for storage in the Lucene document, and as refining field naming, by means of function key expressions.
 * </p>
 *
 * <p>
 * The standard form of a Lucene index scan is a Lucene {@link org.apache.lucene.search.Query search query}.
 * A special {@link com.apple.foundationdb.record.lucene.LucenePlanner} is able to synthesize these from regular query expressions and Lucene search syntax.
 * </p>
 *
 * @see com.apple.foundationdb.record.lucene.IndexWriterCommitCheckAsync
 * @see com.apple.foundationdb.record.lucene.directory.FDBDirectory
 * @see com.apple.foundationdb.record.lucene.LuceneIndexExpressions
 * @see com.apple.foundationdb.record.lucene.LuceneFunctionNames
 *
 */
package com.apple.foundationdb.record.lucene;
