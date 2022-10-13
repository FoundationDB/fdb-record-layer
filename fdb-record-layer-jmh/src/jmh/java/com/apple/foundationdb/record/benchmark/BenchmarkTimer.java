/*
 * FDBBenchmarkTimer.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.benchmark;

import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.profile.RecordLayerProfiler;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * Per thread state for logging and stats.
 */
@State(Scope.Thread)
public class BenchmarkTimer {
    @Nonnull
    private final FDBStoreTimer timer;
    @Nonnull
    private final Map<String, String> mdc;

    private final Random random = new Random(0);

    public BenchmarkTimer() {
        timer = RecordLayerProfiler.getStoreTimer();
        mdc = new HashMap<>();
    }

    @Nonnull
    public FDBStoreTimer getTimer() {
        return timer;
    }

    @Nonnull
    public Map<String, String> getMdc() {
        return mdc;
    }

    @Nonnull
    public Random getRandom() {
        return random;
    }
}

