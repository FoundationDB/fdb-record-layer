/*
 * AccumulatorState.java
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
 * This interface presents a state af an accumulated value. The AccumulatorState holds on to an intermediate state throughout
 * the aggregation operation, allowing for {@link #accumulate} operation to add additional values throughout the aggregation process
 * and then to {@link #finish()} operation once the aggregation is done.
 * @param <T> The type that is being accumulated (e.g. Integer when the aggregation is Sum of Integers)
 * @param <R> The type that is being returned (e.g. Double when the aggregation is Average of Integers)
 */
public interface AccumulatorState<T, R> {
    /**
     * Add additional value to the state. Depending on the type of aggregation (e.g. SUM/MIN/MAX etc.) that
     * value will be applied to the existing state differently.
     * @param value the value to accumulate.
     */
    void accumulate(@Nullable T value);

    /**
     * Finalize the accumulation operation and return the result. Depending on the type of aggregation being done, different
     * actions will b taking place. For example, SUM, MIN and MAX simply return the accumulation, where AVG divides the sum by the count.
     * Note that after calling {@link #finish}, continuing to {@link #accumulate} may produce unpredictable results.
     * @return the finalized accumulation result.
     */
    @Nullable
    R finish();
}
