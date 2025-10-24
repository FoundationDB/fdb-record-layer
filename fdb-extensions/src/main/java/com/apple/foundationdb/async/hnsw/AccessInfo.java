/*
 * AccessInfo.java
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

package com.apple.foundationdb.async.hnsw;

import com.apple.foundationdb.linear.RealVector;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

class AccessInfo {
    @Nonnull
    private final EntryNodeReference entryNodeReference;

    @Nullable
    private final RealVector centroid;

    public AccessInfo(@Nonnull final EntryNodeReference entryNodeReference, @Nullable final RealVector centroid) {
        this.entryNodeReference = entryNodeReference;
        this.centroid = centroid;
    }

    @Nonnull
    public EntryNodeReference getEntryNodeReference() {
        return entryNodeReference;
    }

    @Nullable
    public RealVector getCentroid() {
        return centroid;
    }

    @Nonnull
    public AccessInfo withNewEntryNodeReference(@Nonnull final EntryNodeReference entryNodeReference) {
        return new AccessInfo(entryNodeReference, centroid);
    }
}
