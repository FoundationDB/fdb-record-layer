/*
 * LuceneScanBounds.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2022 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.lucene;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.provider.foundationdb.IndexScanBounds;
import com.apple.foundationdb.tuple.Tuple;

import javax.annotation.Nonnull;

/**
 * Base class for {@link IndexScanBounds} used by {@code LUCENE} indexes.
 * Stores any group key prefix used to determine the directory location.
 */
@API(API.Status.UNSTABLE)
public abstract class LuceneScanBounds implements IndexScanBounds {
    @Nonnull
    protected final IndexScanType scanType;
    @Nonnull
    protected final Tuple groupKey;

    protected LuceneScanBounds(@Nonnull IndexScanType scanType, @Nonnull Tuple groupKey) {
        this.scanType = scanType;
        this.groupKey = groupKey;
    }

    @Nonnull
    @Override
    public IndexScanType getScanType() {
        return scanType;
    }

    @Nonnull
    public Tuple getGroupKey() {
        return groupKey;
    }

    @Override
    public String toString() {
        return scanType + ":" + groupKey;
    }
}
