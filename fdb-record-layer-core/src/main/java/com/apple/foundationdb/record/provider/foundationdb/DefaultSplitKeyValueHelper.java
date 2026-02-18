/*
 * DefaultSplitKeyValueHelper.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;

/**
 * The default implementation of the {@link SplitKeyValueHelper}.
 * This implementation is using the subspace serialization and transaction set methods.
 */
public class DefaultSplitKeyValueHelper implements SplitKeyValueHelper {
    public static final DefaultSplitKeyValueHelper INSTANCE = new DefaultSplitKeyValueHelper();

    /**
     * Since this is setting keys directly, there is a chance that existing keys will interfere with the new keys.
     * @return true to ensure all prior keys are cleared from the subspace
     */
    @Override
    public boolean shouldClearBeforeWrite() {
        return true;
    }

    /**
     * Value can mutate version stamp with this helper.
     * @return true if the SplitHelper should mutate a version in the value
     */
    @Override
    public boolean supportsVersionInValue() {
        return true;
    }

    @Override
    public byte[] packSplitKey(final Subspace subspace, final Tuple key) {
        return subspace.pack(key);
    }

    @Override
    @SuppressWarnings("PMD.CloseResource")
    public void writeSplit(final FDBRecordContext context, final byte[] keyBytes, final byte[] valueBytes) {
        final Transaction tr = context.ensureActive();
        tr.set(keyBytes, valueBytes);
    }
}
