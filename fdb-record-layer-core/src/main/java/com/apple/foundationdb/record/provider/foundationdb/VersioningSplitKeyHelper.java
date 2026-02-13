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
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;

import java.util.Arrays;

public class VersioningSplitKeyHelper implements SplitKeyHelper {
    private Versionstamp versionstamp;

    public VersioningSplitKeyHelper(final Versionstamp versionstamp) {
        this.versionstamp = versionstamp;
    }

    // Since the key has a version, no conflicts are expected, so no need to clean
    @Override
    public boolean clearBeforeWrite() {
        return false;
    }

    @Override
    public byte[] packSplitKey(final Subspace subspace, final Tuple key) {
        // This uses the same version (local and global for all the splits
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
