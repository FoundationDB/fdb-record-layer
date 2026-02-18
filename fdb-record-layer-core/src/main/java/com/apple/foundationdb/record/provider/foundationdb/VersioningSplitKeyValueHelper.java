/*
 * VersioningSplitKeyHelper.java
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

import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.record.RecordCoreInternalException;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;

/**
 * A {@link SplitKeyValueHelper} that is used when the Key contains a {@link Versionstamp}.
 * <p>This implementation should be used when the key contains a version stamp, as it will ensure that the proper FDB APIs
 * encode and decode the key correctly.</p>
 * <p>This class is stateful (has a single {@link Versionstamp}) that is going to be used for all splits of a single K/V,
 * so that all contain the same fixed part and can be correlated after the commit.</p>
 * <p>The resulting FDB key looks like:</p>
 * <pre>
 *     [versionstamp, original-key, split-suffix]
 * </pre>
 * <p>which means that the entries are sorted by their insertion order (versionstamp order), then grouped by their
 * split suffixes.</p>
 */
public class VersioningSplitKeyValueHelper implements SplitKeyValueHelper {
    private Versionstamp versionstamp;

    public VersioningSplitKeyValueHelper(final Versionstamp versionstamp) {
        this.versionstamp = versionstamp;
    }

    /**
     * No need to clear subspace.
     * Since the key has a unique component (version), no conflicts are expected, so no need to clean before saving new splits.
     * Furthermore, since the key contains a version stamp, we don't know the actual key contents ahead of committing
     * the transaction, and so no clean can be done.
     * @return false, as new keys should not interfere with old ones.
     */
    @Override
    public boolean shouldClearBeforeWrite() {
        return false;
    }

    /**
     * Since the key has versions, prevent the values from having them.
     * @return false, since only keys or values are allowed to mutate in FDB, and this mutates the keys
     */
    @Override
    public boolean supportsVersionInValue() {
        return false;
    }

    @Override
    public byte[] packSplitKey(final Subspace subspace, final Tuple key) {
        // This uses the same version (local and global for all the splits
        // Use versionstamp first to ensure proper sorting and since split suffix should be at the end
        Tuple keyTuple = Tuple.from(versionstamp).addAll(key);
        return subspace.packWithVersionstamp(keyTuple);
    }

    @Override
    public void writeSplit(final FDBRecordContext context, final byte[] keyBytes, final byte[] valueBytes) {
        final byte[] current = context.addVersionMutation(
                MutationType.SET_VERSIONSTAMPED_KEY,
                keyBytes,
                valueBytes);

        if (current != null) {
            // This should never happen
            throw new RecordCoreInternalException("Key with version overwritten");
        }
    }
}
