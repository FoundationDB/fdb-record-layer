/*
 * VectorId.java
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

package com.apple.foundationdb.async.guardiann;

import com.apple.foundationdb.tuple.Tuple;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;
import java.util.UUID;

class VectorMetadata extends VectorId {
    @Nullable
    private final Tuple additionalValues;

    VectorMetadata(@Nonnull final Tuple primaryKey, @Nonnull final UUID uuid, @Nullable final Tuple additionalValues) {
        super(primaryKey, uuid);
        this.additionalValues = additionalValues;
    }

    @Nullable
    public Tuple getAdditionalValues() {
        return additionalValues;
    }

    @Override
    public boolean equals(final Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        final VectorMetadata that = (VectorMetadata)o;
        return Objects.equals(getAdditionalValues(), that.getAdditionalValues());
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), getAdditionalValues());
    }
}
