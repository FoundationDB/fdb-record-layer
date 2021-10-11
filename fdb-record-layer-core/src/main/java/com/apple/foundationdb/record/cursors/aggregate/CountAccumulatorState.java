/*
 * CountAccumulatorState.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2021 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.cursors.aggregate;

import javax.annotation.Nullable;

/**
 * Accumulator state for Count operations.
 * The implementation of the state classes is using primitive types in order to minimize Boxing/Unboxing and object
 * garbage collection. These classes are mutable and perform the accumulation using primitive math operators.
 * Once {@link #finish} is called, this instance should be recycled and the accumulation started with a new instance.
 */
public class CountAccumulatorState implements AccumulatorState<Object, Long> {
    public enum CountType { INCLUDE_NULL, EXCLUDE_NULL }

    private long currentState;
    private final CountType countType;

    public CountAccumulatorState(CountType countType) {
        this.countType = countType;
        resetState();
    }

    @Override
    public void accumulate(@Nullable final Object value) {
        if ((value != null) || (countType == CountType.INCLUDE_NULL)) {
            currentState++;
        }
    }

    @Override
    @Nullable
    public Long finish() {
        return currentState;
    }

    private void resetState() {
        currentState = 0;
    }
}
