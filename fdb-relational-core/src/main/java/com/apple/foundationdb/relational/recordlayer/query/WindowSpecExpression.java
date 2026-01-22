/*
 * OverExpression.java
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

package com.apple.foundationdb.relational.recordlayer.query;

import javax.annotation.Nonnull;

/**
 * Helper class that captures the components of an SQL {@code OVER} clause used in window functions.
 * <p>
 * The {@code OVER} clause defines the window (set of rows) over which a window function operates.
 * It consists of two optional components:
 * <ul>
 *     <li><b>PARTITION BY</b> - Divides the result set into partitions to which the window function is applied independently</li>
 *     <li><b>ORDER BY</b> - Defines the logical order of rows within each partition</li>
 * </ul>
 * <p>
 * Example SQL: {@code ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC)}
 * <p>
 * This class is immutable and uses the static factory method {@link #of} for construction.
 */
public final class WindowSpecExpression {

    @Nonnull
    private final Expressions partitions;

    @Nonnull
    private final Iterable<OrderByExpression> orderByExpressions;

    @Nonnull
    private final Expressions windowOptions;

    private WindowSpecExpression(@Nonnull final Expressions partitions,
                                 @Nonnull final Iterable<OrderByExpression> orderByExpressions,
                                 @Nonnull final Expressions windowOptions) {
        this.partitions = partitions;
        this.orderByExpressions = orderByExpressions;
        this.windowOptions = windowOptions;
    }

    /**
     * Creates a new {@code OverExpression} with the specified partitions and ordering.
     *
     * @param partitions the expressions to partition by (corresponds to PARTITION BY clause)
     * @param orderByExpressions the ordering expressions (corresponds to ORDER BY clause)
     * @return a new OverExpression instance
     */
    @Nonnull
    public static WindowSpecExpression of(@Nonnull final Expressions partitions,
                                          @Nonnull final Iterable<OrderByExpression> orderByExpressions,
                                          @Nonnull final Expressions windowOptions) {
        return new WindowSpecExpression(partitions, orderByExpressions, windowOptions);
    }

    /**
     * Returns the partition expressions that define how to divide rows into partitions.
     *
     * @return the partition expressions (PARTITION BY clause)
     */
    @Nonnull
    public Expressions getPartitions() {
        return partitions;
    }

    /**
     * Returns the ordering expressions that define the logical order of rows within partitions.
     *
     * @return the ordering expressions (ORDER BY clause)
     */
    @Nonnull
    public Iterable<OrderByExpression> getOrderByExpressions() {
        return orderByExpressions;
    }

    @Nonnull
    public Expressions getWindowOptions() {
        return windowOptions;
    }
}
