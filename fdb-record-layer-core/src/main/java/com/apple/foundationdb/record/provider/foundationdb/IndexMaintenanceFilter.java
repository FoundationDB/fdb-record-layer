/*
 * IndexMaintenanceFilter.java
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

package com.apple.foundationdb.record.provider.foundationdb;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.metadata.Index;
import com.google.protobuf.MessageOrBuilder;

import javax.annotation.Nonnull;

/**
 * A hook for suppressing secondary indexing of some records.
 * @see FDBRecordStore#indexMaintenanceFilter
 */
@API(API.Status.UNSTABLE)
public interface IndexMaintenanceFilter {

    /**
     * All records should be added to the index. This is the default behavior.
     */
    IndexMaintenanceFilter NORMAL = (i, r) -> IndexValues.ALL;

    /**
     * Do not put <code>null</code> values into the index.
     */
    IndexMaintenanceFilter NO_NULLS = new IndexMaintenanceFilter() {
        @Override
        public IndexValues maintainIndex(@Nonnull Index index, @Nonnull MessageOrBuilder record) {
            return IndexValues.SOME;
        }

        @Override
        public boolean maintainIndexValue(@Nonnull Index index, @Nonnull MessageOrBuilder record,
                                          @Nonnull IndexEntry indexEntry) {
            return !indexEntry.keyContainsNonUniqueNull();
        }
    };

    /**
     * Whether to maintain a subset of the indexable values for the given record.
     */
    enum IndexValues { ALL, NONE, SOME }

    IndexValues maintainIndex(@Nonnull Index index, @Nonnull MessageOrBuilder record);

    /**
     * Get whether a specific index entry should be maintained.
     * Only called if <code>SOME</code> was returned from {@link #maintainIndex}.
     * @param index index to check
     * @param record record that led to the index entry
     * @param indexEntry potential entry in the index
     * @return {@code true} if the given entry should be maintained in the given index
     */
    default boolean maintainIndexValue(@Nonnull Index index, @Nonnull MessageOrBuilder record,
                                       @Nonnull IndexEntry indexEntry) {
        return true;
    }

}
