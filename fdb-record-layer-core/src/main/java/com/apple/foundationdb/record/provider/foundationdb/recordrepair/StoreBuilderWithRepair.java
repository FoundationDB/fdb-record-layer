/*
 * StoreBuilderWithRepair.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb.recordrepair;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FormatVersion;
import com.apple.foundationdb.record.util.pair.NonnullPair;

import javax.annotation.Nonnull;
import java.util.concurrent.CompletableFuture;

/**
 * A flavor of {@link FDBRecordStore.Builder} that can handle the case where the store cannot be opened.
 * In case the given store builder fails to open the store due to a missing header, the {@link FDBRecordStore.Builder#repairMissingHeader(int, FormatVersion)}
 * method is called and the repaired store is returned.
 */
@API(API.Status.INTERNAL)
public class StoreBuilderWithRepair extends FDBRecordStore.Builder {
    private final int userVersion;
    private final FormatVersion minimumPossibleFormatVersion;

    /**
     * Constructor.
     * @param other the source store builder to delegate to
     * @param userVersion the userVersion to use for repairing the header if necessary
     * @param minimumPossibleFormatVersion the minimumPossibleFormatVersion to use if necessary
     */
    public StoreBuilderWithRepair(@Nonnull FDBRecordStore.Builder other,
                                  final int userVersion,
                                  @Nonnull FormatVersion minimumPossibleFormatVersion) {
        super(other);
        this.userVersion = userVersion;
        this.minimumPossibleFormatVersion = minimumPossibleFormatVersion;
    }

    /**
     * Override the {@link FDBRecordStore.Builder#openAsync()} method to add support for repairing the header.
     * In case the store fails to be opened normally, try to repair it given the provided repair
     * parameters.
     *
     * @return a future that will contain the opened store if successful
     */
    @Nonnull
    @Override
    public CompletableFuture<FDBRecordStore> openAsync() {
        return repairMissingHeader(userVersion, minimumPossibleFormatVersion)
                .thenApply(NonnullPair::getRight);
    }
}
