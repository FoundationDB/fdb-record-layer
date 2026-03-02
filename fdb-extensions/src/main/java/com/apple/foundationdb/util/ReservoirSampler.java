/*
 * ReservoirSampler.java
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

package com.apple.foundationdb.util;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.SplittableRandom;

public final class ReservoirSampler<T> {
    private final int k;
    private final List<T> reservoir;
    private final SplittableRandom random;
    private long seen;

    public ReservoirSampler(final int k, @Nonnull final SplittableRandom random) {
        Preconditions.checkArgument(k > 0, "k must be > 0");
        this.k = k;
        this.random = random;
        this.seen = 0;
        this.reservoir = Lists.newArrayListWithCapacity(k);
    }

    public void add(@Nullable T item) {
        if (seen < k) {
            reservoir.add(item);
        } else {
            long j = random.nextLong(seen + 1);
            if (j < k) {
                reservoir.set((int) j, item);
            }
        }
        seen++;
    }

    public List<T> sample() {
        return ImmutableList.copyOf(reservoir);
    }

    public long seenCount() {
        return seen;
    }
}
