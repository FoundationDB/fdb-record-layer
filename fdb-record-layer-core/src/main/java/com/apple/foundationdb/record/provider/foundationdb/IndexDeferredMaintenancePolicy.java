/*
 * IndexDeferredMaintenancePolicy.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2023 Apple Inc. and the FoundationDB project authors
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

/**
 * Some store's indexes may need merging on some occasions. This helper module should allow the caller
 * to set and probe the merge policy and merge requests.
 */
public class IndexDeferredMaintenancePolicy {
    private boolean mergeRequired = false;
    private boolean autoMergeDuringCommit = true;

    /**
     * Indicate to the caller if a deferred index merge operation is required.
     * @return if true, an index merge if required
     */
    public boolean isMergeRequired() {
        return mergeRequired;
    }

    /**
     * Indicate to the caller if a deferred merge operation is required.
     * @param mergeRequired if true, indicate that an index merge if required
     */
    public void setMergeRequired(final boolean mergeRequired) {
        this.mergeRequired = mergeRequired;
    }

    /**
     * Indicate to the index maintenance to automatically merge indexes during commit (if applicable).
     * @return true if should merge
     */
    public boolean shouldAutoMergeDuringCommit() {
        return autoMergeDuringCommit;
    }

    /**
     * Indicate to the index maintenance to automatically merge indexes during commit (if applicable).
     * If the user sets it to false, he is responsible to call, possibly in the background, the {@link OnlineIndexer#mergeIndex()}
     * function if {@link #isMergeRequired()} returns true after the operation.
     * @param autoMergeDuringCommit if true (default) and applicable, automatically merge during commit
     */
    public void setAutoMergeDuringCommit(final boolean autoMergeDuringCommit) {
        this.autoMergeDuringCommit = autoMergeDuringCommit;
    }
}
