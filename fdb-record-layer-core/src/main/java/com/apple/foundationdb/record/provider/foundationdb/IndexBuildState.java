/*
 * IndexBuildState.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2020 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.AggregateFunctionNotSupportedException;
import com.apple.foundationdb.record.IndexState;
import com.apple.foundationdb.record.metadata.Index;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * A class that contains the build progress of a given index in a given record store.
 * <p>
 * It first contains {@link IndexState}. If the state is {@link IndexState#WRITE_ONLY}, it will also contain the number
 * of records scanned and the estimated total number of records that need to be scanned.
 * </p>
 * <p>
 * Note that index build state (especially records scanned) serves an informational purpose only as the number of
 * records scanned is not necessarily 100% of records in total when the index build completes for several reasons:
 * </p>
 * <ul>
 *     <li> If the {@link OnlineIndexer} has been configured to disable tracking progress (by setting
 *     {@link OnlineIndexer.Builder#setTrackProgress(boolean)} to {@code false}), then {@link #getRecordsScanned()} will
 *     not include the records scanned by that {@link OnlineIndexer}. </li>
 *     <li> Records added or deleted during the index build progress are not included in records scanned but are in
 *     records in total. </li>
 *     <li> Records in total uses the number of all records in the store. In some cases, the real number of records
 *     that need to be scanned is less than this.</li>
 * </ul>
 * @see #loadIndexBuildStateAsync(FDBRecordStoreBase, Index)
 */
@API(API.Status.UNSTABLE)
public class IndexBuildState {
    @Nonnull
    private final IndexState indexState;
    @Nullable
    private final Long recordsScanned;
    @Nullable
    private final Long recordsInTotal;

    /**
     * Load the build progress ({@link IndexBuildState}) of the given index in the given record store asynchronously.
     * @param store the record store containing the index
     * @param index the index needed to be checked
     * @return a future that completes to the index build state
     */
    @Nonnull
    public static CompletableFuture<IndexBuildState> loadIndexBuildStateAsync(FDBRecordStoreBase<?> store, Index index) {
        IndexState indexState = store.getUntypedRecordStore().getIndexState(index);
        if (indexState != IndexState.WRITE_ONLY) {
            return CompletableFuture.completedFuture(new IndexBuildState(indexState));
        }
        CompletableFuture<Long> recordsInTotalFuture;
        try {
            recordsInTotalFuture = store.getSnapshotRecordCount();
        } catch (AggregateFunctionNotSupportedException ex) {
            // getSnapshotRecordCount failed, very likely it is because there is no suitable COUNT type index
            // defined.
            recordsInTotalFuture = CompletableFuture.completedFuture(null);
        }
        return loadRecordsScannedAsync(store, index).thenCombine(recordsInTotalFuture,
                (scannedRecords, recordsInTotal) -> new IndexBuildState(indexState, scannedRecords, recordsInTotal));
    }

    /**
     * Load the number of records successfully scanned and processed during the online index build process
     * asynchronously.
     * <p>
     * If the {@link OnlineIndexer} has been configured to disable tracking progress (by setting
     * {@link OnlineIndexer.Builder#setTrackProgress(boolean)} to {@code false}), then the number returned will not
     * include the records scanned by that {@link OnlineIndexer}.
     * </p>
     * @param store the record store containing the index
     * @param index the index needed to be checked
     * @return a future that completes to the total records scanned
     */
    @Nonnull
    public static CompletableFuture<Long> loadRecordsScannedAsync(FDBRecordStoreBase<?> store, Index index) {
        return store.getContext().ensureActive()
                .get(IndexingSubspaces.indexBuildScannedRecordsSubspace(store, index).getKey())
                .thenApply(FDBRecordStore::decodeRecordCount);
    }

    /**
     * Get the index state.
     * @return the index state
     */
    @Nonnull
    public IndexState getIndexState() {
        return indexState;
    }

    /**
     * Get the number of records successfully scanned and processed during the online index build process.
     * <p>
     * If the {@link OnlineIndexer} has been configured to disable tracking progress (by setting
     * {@link OnlineIndexer.Builder#setTrackProgress(boolean)} to {@code false}), then the number returned will not
     * include the records scanned by that {@link OnlineIndexer}.
     * </p>
     * <p>
     * The returned value should be ignored if the index state is not {@link IndexState#WRITE_ONLY}.
     * </p>
     * <p>
     * See {@link IndexBuildState} if this is used with {@link #getRecordsInTotal()} to track the index build progress.
     * </p>
     * @return the number of scanned records
     * @see #loadRecordsScannedAsync(FDBRecordStoreBase, Index)
     */
    @Nullable
    public Long getRecordsScanned() {
        return recordsScanned;
    }

    /**
     * Get the estimated total number of records that need to be scanned to build the index. Currently, it uses the
     * number of all records in the store. In some cases, the real number of records that need to be scanned is less
     * than this.
     * <p>
     * To get the count, there must be a suitably grouped {@code COUNT} type index defined. Otherwise, it returns
     * {@code null}.
     * </p>
     * <p>
     * The returned value should be ignored if the index state is not {@link IndexState#WRITE_ONLY}.
     * </p>
     * @return the number of total records
     */
    @Nullable
    public Long getRecordsInTotal() {
        return recordsInTotal;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("IndexBuildState{");
        sb.append("indexState=").append(indexState);
        if (indexState == IndexState.WRITE_ONLY) {
            sb.append(", scannedRecords=").append(recordsScanned);
            sb.append(", totalRecords=").append((recordsInTotal == null) ? "UNKNOWN" : recordsInTotal);
        }
        sb.append('}');
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        IndexBuildState that = (IndexBuildState)o;
        return indexState == that.indexState &&
               Objects.equals(recordsScanned, that.recordsScanned) &&
               Objects.equals(recordsInTotal, that.recordsInTotal);
    }

    @Override
    public int hashCode() {
        return Objects.hash(indexState, recordsScanned, recordsInTotal);
    }

    private IndexBuildState(IndexState indexState, @Nullable Long recordsScanned, @Nullable Long recordsInTotal) {
        this.indexState = indexState;
        this.recordsScanned = recordsScanned;
        this.recordsInTotal = recordsInTotal;
    }

    private IndexBuildState(IndexState indexState) {
        this(indexState, null, null);
    }
}
