/*
 * KeyValueCursor.java
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
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.record.cursors.CursorLimitManager;
import com.apple.foundationdb.subspace.Subspace;

import javax.annotation.Nonnull;

/**
 * The basic cursor for scanning ranges of the FDB database.
 */
@API(API.Status.UNSTABLE)
public class KeyValueCursor extends KeyValueCursorBase<KeyValue> {

    private KeyValueCursor(@Nonnull final FDBRecordContext context,
                             @Nonnull final AsyncIterator<KeyValue> iterator,
                             int prefixLength,
                             @Nonnull final CursorLimitManager limitManager,
                             int valuesLimit,
                             @Nonnull SerializationMode serializationMode) {
        super(context, iterator, prefixLength, limitManager, valuesLimit, serializationMode);
    }

    /**
     * A builder for {@link KeyValueCursor}.
     *
     * <pre><code>
     * KeyValueCursor.Builder.withSubspace(subspace)
     *                     .setContext(context)
     *                     .setRange(TupleRange.ALL)
     *                     .setContinuation(null)
     *                     .setScanProperties(ScanProperties.FORWARD_SCAN)
     *                     .build()
     * </code></pre>
     */
    @API(API.Status.UNSTABLE)
    public static class Builder extends KeyValueCursorBase.Builder<Builder> {

        protected Builder(@Nonnull Subspace subspace) {
            super(subspace);
        }

        /*
         * This will be deprecated: Use the {@link #newBuilder(Subspace)} instead
         */
        public static Builder withSubspace(@Nonnull Subspace subspace) {
            return new Builder(subspace);
        }

        public static Builder newBuilder(@Nonnull Subspace subspace) {
            return new Builder(subspace);
        }

        public KeyValueCursor build() {
            prepare();
            final AsyncIterator<KeyValue> iterator = getTransaction()
                    .getRange(getBegin(), getEnd(), getLimit(), isReverse(), getStreamingMode())
                    .iterator();
            return new KeyValueCursor(getContext(), iterator, getPrefixLength(), getLimitManager(), getValuesLimit(), serializationMode);
        }

        @Override
        protected Builder self() {
            return this;
        }
    }
}
