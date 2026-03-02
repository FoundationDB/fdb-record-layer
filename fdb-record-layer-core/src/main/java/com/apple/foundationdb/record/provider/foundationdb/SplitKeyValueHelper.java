/*
 * SplitKeyValueHelper.java
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

import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;

/**
 * An interface extracting the generation and persistence of keys used in the {@link SplitHelper}.
 */
public interface SplitKeyValueHelper {
    /**
     * Whether the {@link SplitHelper} should clear older entries before writing new ones.
     * In the cases where old splits may exist and overlap with the new entries, the SplitHelper should clear the
     * subspace before writing any new splits. This method is used to determine whether this is necessary.
     * @return TRUE if the SplitHelper should clear the subspace before writing new splits.
     */
    boolean shouldClearBeforeWrite();

    /**
     * Whether the helper allows version mutation in the values.
     * There are cases where the value cannot have version mutation, for example, when the key needs to have one.
     * @return true if version mutations are allowed for the value of the k/v pair, false if not
     */
    boolean supportsVersionInValue();

    /**
     * Serialize a key to a format that can be saved to FDB.
     * @param subspace the subspace to use for the key
     * @param key the key Tuple to use for the rest of the key
     * @return the serialized form of the key
     */
    byte[] packSplitKey(Subspace subspace, Tuple key);

    /**
     * Write a key/value pair to FDB.
     * @param context the transaction to use for writing
     * @param keyBytes the key to use
     * @param valueBytes the value to use
     */
    void writeSplit(FDBRecordContext context, byte[] keyBytes, byte[] valueBytes);
}
