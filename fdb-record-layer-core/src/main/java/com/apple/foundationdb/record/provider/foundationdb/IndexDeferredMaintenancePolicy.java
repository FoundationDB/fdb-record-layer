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

import com.apple.foundationdb.record.metadata.Index;

import java.util.HashSet;
import java.util.Set;

/**
 * Some store's indexes may need merging on some occasions. This helper module should allow the caller
 * to set and probe the merge policy and merge requests.
 */
public class IndexDeferredMaintenancePolicy {
    private Set<Index> mergeRequiredIndexes = null;
    private boolean autoMergeDuringCommit = true;
    private int diluteLevel = -1;
    private DilutedResults dilutedResults = DilutedResults.ALL_DONE;

    /**
     * If the merge caller sets dilute level to a non-negative number, the merge implementation is responsible
     * to handle these values correctly.
     */
    public enum DilutedResults {
        // Note that ALL_DONE and CANNOT_DILUTE are only distinguished for debugging purpose
        ALL_DONE,  // No dilution was requested
        HAS_MORE, // Merges were diluted - the caller should call again
        NOT_DILUTED, // Merges cannot be diluted - could be the last chunk if an iteration
    }

    /**
     * Return a set of indexes that need a deferred index merge operation. This function may be used by the
     * caller, to check which index maintainer requested a deferred merge.
     * @return set of indexes to be merged, null if no merge was requested.
     */
    public synchronized Set<Index> getMergeRequiredIndexes() {
        return mergeRequiredIndexes;
    }

    /**
     * Indicate to the caller if a deferred merge operation is required. This function is used by the index maintainer.
     * @param mergeRequiredIndex an index that should be merged
     */
    public synchronized void setMergeRequiredIndexes(final Index mergeRequiredIndex) {
        if (mergeRequiredIndexes == null) {
            this.mergeRequiredIndexes = new HashSet<>();
        }
        this.mergeRequiredIndexes.add(mergeRequiredIndex);
    }

    /**
     * If thrue, indicates to the index maintenance to automatically merge indexes during commit.
     * @return true if should merge
     */
    public boolean shouldAutoMergeDuringCommit() {
        return autoMergeDuringCommit;
    }

    /**
     * Indicate to the index maintenance to automatically merge indexes during commit (if applicable).
     * If the user sets it to false, they are responsible to call, possibly in the background, the {@link OnlineIndexer#mergeIndex()}
     * function with the set of indexes returned by {@link #getMergeRequiredIndexes()} as target indexes.
     * @param autoMergeDuringCommit if true (default) and applicable, automatically merge during commit
     */
    public void setAutoMergeDuringCommit(final boolean autoMergeDuringCommit) {
        this.autoMergeDuringCommit = autoMergeDuringCommit;
    }

    /**
     * Dilute level is:
     * negative: do not apply any dilution logic.
     * zero: do not dilute, but may be diluted in case of failure.
     * positive: attempt dilute of num-merges / (2 ^ dilute-level)
     * @return dilute level
     */
    public int getDiluteLevel() {
        return diluteLevel;
    }

    /**
     * See {@link #getDiluteLevel()}.
     * @param diluteLevel dilute level
     */
    public void setDiluteLevel(final int diluteLevel) {
        this.diluteLevel = diluteLevel;
    }

    /**
     * If the merge caller sets dilute level to a non-negative number, the return value should indicate if there are more
     * merges to merge, or if the merges cannot be diluted..
     * @return dilute result.
     */
    public DilutedResults getDilutedResults() {
        return dilutedResults;
    }

    /**
     * See {@link #getDiluteLevel()}.
     * @param dilutedResults dilute results.
     */
    public void setDilutedResults(final DilutedResults dilutedResults) {
        this.dilutedResults = dilutedResults;
    }
}
