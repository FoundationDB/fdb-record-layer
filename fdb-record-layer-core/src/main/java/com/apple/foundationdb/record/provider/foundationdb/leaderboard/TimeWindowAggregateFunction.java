/*
 * TimeWindowAggregateFunction.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb.leaderboard;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.IndexAggregateFunction;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Evaluate aggregate functions in a certain time window.
 */
@API(API.Status.EXPERIMENTAL)
public class TimeWindowAggregateFunction extends IndexAggregateFunction {
    @Nonnull
    private final TimeWindowForFunction timeWindow;

    public TimeWindowAggregateFunction(@Nonnull String name, @Nonnull KeyExpression operand, @Nullable String index,
                                       @Nonnull TimeWindowForFunction timeWindow) {
        super(name, operand, index);
        this.timeWindow = timeWindow;
    }

    @Nonnull
    public TimeWindowForFunction getTimeWindow() {
        return timeWindow;
    }

    @Nonnull
    @Override
    public TimeWindowAggregateFunction cloneWithOperand(@Nonnull KeyExpression operand) {
        return new TimeWindowAggregateFunction(getName(), operand, getIndex(), timeWindow);
    }

    @Nonnull
    @Override
    public TimeWindowAggregateFunction cloneWithIndex(@Nonnull String index) {
        return new TimeWindowAggregateFunction(getName(), getOperand(), index, timeWindow);
    }

    @Nonnull
    @Override
    public TupleRange adjustRange(@Nonnull EvaluationContext context, @Nonnull TupleRange tupleRange) {
        return timeWindow.prependLeaderboardKeys(context, tupleRange);
    }

    @Override
    public String toString() {
        return super.toString() + "@" + timeWindow;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }

        TimeWindowAggregateFunction that = (TimeWindowAggregateFunction) o;
        return this.timeWindow.equals(that.timeWindow);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + timeWindow.hashCode();
        return result;
    }
}
