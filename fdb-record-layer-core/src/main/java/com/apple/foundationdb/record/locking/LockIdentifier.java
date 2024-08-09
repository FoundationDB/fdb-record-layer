/*
 * LockIdentifier.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.locking;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.subspace.Subspace;
import com.google.common.base.Suppliers;

import javax.annotation.Nonnull;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Tuple-based identifier used to locate a resource in the {@link LockRegistry}.
 */
@API(API.Status.INTERNAL)
public class LockIdentifier {
    @Nonnull
    private final Subspace lockingSubspace;
    @Nonnull
    private final Supplier<Integer> memoizedHashCode = Suppliers.memoize(this::calculateHashCode);

    public LockIdentifier(@Nonnull final Subspace lockingSubspace) {
        this.lockingSubspace = lockingSubspace;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof LockIdentifier)) {
            return false;
        }
        return lockingSubspace.equals(((LockIdentifier)obj).lockingSubspace);
    }

    private int calculateHashCode() {
        return Objects.hashCode(lockingSubspace);
    }

    @Override
    public int hashCode() {
        return memoizedHashCode.get();
    }
}
