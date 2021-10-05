/*
 * TableMetaData.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.api.catalog;

import com.apple.foundationdb.relational.recordlayer.ddl.ColumnDescriptor;
import com.google.protobuf.Descriptors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Set;

public interface TableMetaData {

    /**
     * Get a list of all the columns in this table.
     *
     * @return a listing of columns for this table. The position in the array is the position in the underlying
     * Tuple(keys first, then value columns), and the value is the information for that Column at that position.
     * No entry is null.
     */
    @Nonnull ColumnDescriptor[] getColumns();

    /**
     * Get a list of the key columns in this table.
     *
     * @return a listing of only key columns for this table. The position in the array is the position in the
     * underlying key tuple, and the value is the information for that column at that position. No entry is null.
     */
    @Nonnull ColumnDescriptor[] getKeyColumns();

    /**
     * Get a list of all the indexes in this table.
     *
     * @return Metadata about each index in the individual table.
     */
    @Nonnull Set<IndexMetaData> getIndexes();

    /**
     * Get the type descriptor for this table, if it exists, or null if it does not.
     *
     * @return the type descriptor for this table, if the table supports protobuf storage, or {@code null} if
     * it does not.
     */
    @Nullable Descriptors.Descriptor getTableTypeDescriptor();
}
