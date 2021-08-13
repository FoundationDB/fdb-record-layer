/*
 * package-info.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021 Apple Inc. and the FoundationDB project authors
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
 * Implement sorting for {@link com.apple.foundationdb.record.RecordCursor} for use in queries without a sorting index.
 *
 * <ul>
 * <li>{@code MemorySortCursor}: Save a limited number of records in memory and then return them.</li>
 *
 * <li>{@code FileSortCursor}: Use a memory table to sort batches of records, then write them to disk in files,
 *     then merge sort those files, then return results by reading the resultant file.</li>
 * </ul>
 */
package com.apple.foundationdb.record.sorting;
