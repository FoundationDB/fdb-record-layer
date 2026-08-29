/*
 * TimeWindowRecordFunction.java
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
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.metadata.IndexRecordFunction;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;

import org.jspecify.annotations.Nullable;

/**
 * The <code>TIME_WINDOW_RANK</code> record function.
 * Evaluates to the given record's rank of the given type at the given time.
 * @param <T> type of the function result
 */
@API(API.Status.EXPERIMENTAL)
public class TimeWindowRecordFunction<T> extends IndexRecordFunction<T> {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Time-Window-Record-Function");

    private final TimeWindowForFunction timeWindow;

    public TimeWindowRecordFunction(String name, GroupingKeyExpression operand, @Nullable String index,
                                    TimeWindowForFunction timeWindow) {
        super(name, operand, index);
        this.timeWindow = timeWindow;
    }

    public TimeWindowForFunction getTimeWindow() {
        return timeWindow;
    }

    @Override
    public TimeWindowRecordFunction<T> cloneWithOperand(GroupingKeyExpression operand) {
        return new TimeWindowRecordFunction<>(getName(), operand, getIndex(), timeWindow);
    }

    @Override
    public TimeWindowRecordFunction<T> cloneWithIndex(String index) {
        return new TimeWindowRecordFunction<>(getName(), getOperand(), index, timeWindow);
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

        TimeWindowRecordFunction<?> that = (TimeWindowRecordFunction<?>) o;
        return this.timeWindow.equals(that.timeWindow);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + timeWindow.hashCode();
        return result;
    }

    @Override
    public int planHash(final PlanHashable.PlanHashMode mode) {
        switch (mode.getKind()) {
            case LEGACY:
                return super.planHash(mode);
            case FOR_CONTINUATION:
                return PlanHashable.objectsPlanHash(mode, BASE_HASH, super.planHash(mode), timeWindow);
            default:
                throw new UnsupportedOperationException("Hash kind " + mode.getKind() + " is not supported");
        }
    }
}
