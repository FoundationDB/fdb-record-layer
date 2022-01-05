/*
 * IndexRangeKeyValueCursor.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2022 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.KeySelector;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.StreamingMode;
import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.record.cursors.CursorLimitManager;
import com.apple.foundationdb.subspace.Subspace;

import javax.annotation.Nonnull;

/**
 * A {@link KeyValueCursor} that scans an index using the IndexPrefetch operation.
 * This cursor is used when executing an IndexPrefetch plan, where the scanned range (index range) is
 * different from the returned records range (record primary key).
 * The differences between this class and the "plain" {@link KeyValueCursor} are:
 * <UL>
 * <LI>The operation performed is {@link com.apple.foundationdb.FDBTransaction#getRangeAndFlatMap}</LI>
 * <LI>There are two subspaces: The "standard" subspace where the scan operation is specified and the
 * "record subspace" where the returned records reside</LI>
 * <LI>PrefixLength is calculated based off of the record subspace (so that the continuation and primary key can
 * be calculated properly</LI>
 * </UL>
 */
@API(API.Status.EXPERIMENTAL)
public class IndexPrefetchRangeKeyValueCursor extends KeyValueCursor {
    private IndexPrefetchRangeKeyValueCursor(@Nonnull final FDBRecordContext context,
                                             @Nonnull final AsyncIterator<KeyValue> iterator,
                                             int prefixLength,
                                             @Nonnull final CursorLimitManager limitManager,
                                             int valuesLimit) {

        super(context, iterator, prefixLength, limitManager, valuesLimit);
    }

    @API(API.Status.EXPERIMENTAL)
    public static class Builder extends KeyValueCursor.Builder {
        // The HopInfo that is used for the getRangeAndFlatMap call
        private final byte[] hopInfo;
        // The subspace for the record keys
        private final Subspace recordSubspace;

        private Builder(@Nonnull Subspace indexSubspace, @Nonnull byte[] hopInfo, @Nonnull Subspace recordSubspace) {
            super(indexSubspace);
            this.hopInfo = hopInfo;
            this.recordSubspace = recordSubspace;
        }

        public static Builder newBuilder(@Nonnull Subspace indexSubspace, @Nonnull byte[] hopInfo, @Nonnull Subspace recordSubspace) {
            return new Builder(indexSubspace, hopInfo, recordSubspace);
        }

        protected AsyncIterator<KeyValue> scanRange(@Nonnull ReadTransaction transaction,
                                                    @Nonnull KeySelector begin,
                                                    @Nonnull KeySelector end,
                                                    int limit, boolean reverse,
                                                    @Nonnull StreamingMode streamingMode) {
            return transaction
                    .getRangeAndFlatMap(begin, end, hopInfo, limit, reverse, streamingMode)
                    .iterator();
        }

        protected int calculatePrefixLength() {
            return recordSubspace.pack().length;
        }
    }
}
