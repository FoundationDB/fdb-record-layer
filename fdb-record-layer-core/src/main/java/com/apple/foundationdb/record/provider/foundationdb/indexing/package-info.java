/*
 * package-info.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2023 Apple Inc. and the FoundationDB project authors
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
 * Classes used during index builds. These classes are used when building or rebuilding an index on an existing
 * store with existing data. This is intended to be an internal package of classes that adopters should not
 * need to consume directly. Adopters should go through the
 * {@link com.apple.foundationdb.record.provider.foundationdb.OnlineIndexer OnlineIndexer} API to orchestrate
 * builds.
 *
 * <p>
 * This should not be confused with {@link com.apple.foundationdb.record.provider.foundationdb.indexes}, which
 * contains implementations of the different index maintainers, which are used to index new records as they are
 * written (and remove old index entries are records are deleted).
 * </p>
 *
 * <p>
 * There are additional classes that are currently in {@link com.apple.foundationdb.record.provider.foundationdb}
 * that should probably be moved into this package, such as
 * {@link com.apple.foundationdb.record.provider.foundationdb.IndexingBase IndexingBase} and its subclasses.
 * </p>
 */
package com.apple.foundationdb.record.provider.foundationdb.indexing;
