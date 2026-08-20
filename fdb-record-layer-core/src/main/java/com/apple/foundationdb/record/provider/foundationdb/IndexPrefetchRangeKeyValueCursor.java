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

import com.apple.foundationdb.MappedKeyValue;
import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.record.cursors.CursorLimitManager;
import com.apple.foundationdb.subspace.Subspace;

import javax.annotation.Nonnull;

/**
 * A {@link KeyValueCursor} that scans an index using the IndexPrefetch operation.
 * This subclass of {@link KeyValueCursor} uses a different scan operation ({@link com.apple.foundationdb.Transaction#getMappedRange})
 * and therefore the actual returned types of the scanned range are different too - they need to be parsed into records
 * rather than Index entries. The returned values are MappedKeyValues and contain all splits of
 * the record for a particular index entry.
 */
@API(API.Status.EXPERIMENTAL)
public class IndexPrefetchRangeKeyValueCursor extends KeyValueCursorBase<MappedKeyValue> {
    private IndexPrefetchRangeKeyValueCursor(@Nonnull final FDBRecordContext context,
                                             @Nonnull final AsyncIterator<MappedKeyValue> iterator,
                                             int prefixLength,
                                             @Nonnull final CursorLimitManager limitManager,
                                             int valuesLimit,
                                             @Nonnull SerializationMode serializationMode) {

        super(context, iterator, prefixLength, limitManager, valuesLimit, serializationMode);
    }

    /**
     * A Builder for the cursor.
     */
    @API(API.Status.EXPERIMENTAL)
    public static class Builder extends KeyValueCursorBase.Builder<Builder> {
        // The HopInfo that is used for the getRangeAndFlatMap call
        private final byte[] mapper;

        private Builder(@Nonnull Subspace indexSubspace, @Nonnull byte[] mapper) {
            super(indexSubspace);
            this.mapper = mapper;
        }

        public static Builder newBuilder(@Nonnull Subspace indexSubspace, @Nonnull byte[] hopInfo) {
            return new Builder(indexSubspace, hopInfo);
        }


        public IndexPrefetchRangeKeyValueCursor build() {
            prepare();
            AsyncIterator<MappedKeyValue> iterator = getTransaction()
                    .getMappedRange(getBegin(), getEnd(), mapper, getLimit(), isReverse(), getStreamingMode())
                    .iterator();
            return new IndexPrefetchRangeKeyValueCursor(getContext(), iterator, getPrefixLength(), getLimitManager(), getValuesLimit(), serializationMode);
        }

        @Override
        protected Builder self() {
            return this;
        }
    }
}
