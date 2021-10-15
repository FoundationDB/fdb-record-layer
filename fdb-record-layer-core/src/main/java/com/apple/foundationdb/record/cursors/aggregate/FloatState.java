/*
 * IntState.java
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

import com.apple.foundationdb.record.RecordCoreException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Accumulator state for Float types.
 * The implementation of the state classes is using primitive types in order to minimize Boxing/Unboxing and object
 * garbage collection. These classes are mutable and perform the accumulation using primitive math operators.
 * Once {@link #finish} is called, this instance should be recycled and the accumulation started with a new instance.
 */
public class FloatState implements AccumulatorState<Float, Float> {
    private float currentState;
    private boolean hasValue;
    private final NumericAccumulatorOperation operation;

    public FloatState(NumericAccumulatorOperation operation) {
        this.operation = operation;
        resetState(operation);
    }

    @Override
    public void accumulate(@Nullable final Float value) {
        if (value != null) {
            switch (operation) {
                case SUM:
                    currentState = currentState + value;
                    break;
                case MIN:
                    currentState = Math.min(currentState, value);
                    break;
                case MAX:
                    currentState = Math.max(currentState, value);
                    break;
                default:
                    break;
            }
            hasValue = true;
        }
    }

    @Override
    @Nullable
    public Float finish() {
        if (hasValue) {
            return currentState;
        } else {
            return null;
        }
    }

    @Nonnull
    @Override
    public AggregateCursorContinuation.ContinuationAccumulatorState getContinuationState() {
        if (hasValue) {
            return new AggregateCursorContinuation.ContinuationAccumulatorState(currentState, null);
        } else {
            return AggregateCursorContinuation.ContinuationAccumulatorState.EMPTY;
        }
    }

    @Override
    public void setContinuationState(final @Nonnull AggregateCursorContinuation.ContinuationAccumulatorState value) {
        Object newState = value.getValue();
        if (newState != null) {
            if (!(newState instanceof Float)) {
                throw new RecordCoreException("Failed to initialize from continuation: type of state values does not match")
                        .addLogInfo("expected",  "Float")
                        .addLogInfo("actual", newState.getClass().getSimpleName());
            }
            currentState = (Float)newState;
            hasValue = true;
        }
    }

    private void resetState(final NumericAccumulatorOperation operation) {
        hasValue = false;
        switch (operation) {
            case SUM:
                currentState = 0.0f;
                break;
            case MIN:
                currentState = Float.MAX_VALUE;
                break;
            case MAX:
                currentState = Float.MIN_VALUE;
                break;
            default:
                break;
        }
    }
}
