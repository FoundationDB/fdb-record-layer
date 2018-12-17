/*
 * package-info.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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
 * Index maintainer classes.
 *
 * <p>
 * Secondary indexes are identified in the meta-data by a string type.
 * A {@link com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerRegistry} maps an index type to {@link com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerFactory}, allowing
 * for index types defined outside of the Record Layer core.
 * An {@link com.apple.foundationdb.record.provider.foundationdb.IndexMaintainer} is responsible for storing index entries for records when they are saved and for scanning those entries to retrieve the records in a query.
 * </p>
 *
 * <p>
 * In addition to B-tree indexes, which provide lexicographic ordering of one or more key fields in the record, the Record Layer core includes some special indexes.
 * </p>
 * <ul>
 * <li>{@link com.apple.foundationdb.record.provider.foundationdb.indexes.AtomicMutationIndexMaintainer} for accumulating contention-free aggregates such as {@code COUNT}, {@code SUM}, {@code MIN}, {@code MAX}.</li>
 * <li>{@link com.apple.foundationdb.record.provider.foundationdb.indexes.RankIndexMaintainer} supporting efficient {@code rank} and {@code select} of a score-like key field using a persistent skip-list.</li>
 * <li>{@link com.apple.foundationdb.record.provider.foundationdb.indexes.TextIndexMaintainer} tokenizing full text into an inverted index.</li>
 * </ul>
 */
package com.apple.foundationdb.record.provider.foundationdb.indexes;
