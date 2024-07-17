/*
 * TimeWindowForFunction.java
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
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.planprotos.PTimeWindowForFunction;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.base.Verify;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collections;

/**
 * Additional function arguments for time window.
 */
@API(API.Status.EXPERIMENTAL)
public class TimeWindowForFunction implements PlanHashable, PlanSerializable {

    private final int leaderboardType;
    private final long leaderboardTimestamp;
    @Nullable
    private final String leaderboardTypeParameter;
    @Nullable
    private final String leaderboardTimestampParameter;

    public TimeWindowForFunction(int leaderboardType, long leaderboardTimestamp,
                                 @Nullable String leaderboardTypeParameter, @Nullable String leaderboardTimestampParameter) {
        this.leaderboardType = leaderboardType;
        this.leaderboardTimestamp = leaderboardTimestamp;
        this.leaderboardTypeParameter = leaderboardTypeParameter;
        this.leaderboardTimestampParameter = leaderboardTimestampParameter;
    }

    public int getLeaderboardType() {
        return leaderboardType;
    }

    public int getLeaderboardType(@Nonnull EvaluationContext context) {
        if (leaderboardTypeParameter == null) {
            return leaderboardType;
        } else {
            return ((Number)context.getBinding(leaderboardTypeParameter)).intValue();
        }
    }

    public long getLeaderboardTimestamp() {
        return leaderboardTimestamp;
    }

    public long getLeaderboardTimestamp(@Nonnull EvaluationContext context) {
        if (leaderboardTimestampParameter == null) {
            return leaderboardTimestamp;
        } else {
            return ((Number)context.getBinding(leaderboardTimestampParameter)).longValue();
        }
    }

    @Nullable
    public String getLeaderboardTypeParameter() {
        return leaderboardTypeParameter;
    }

    @Nullable
    public String getLeaderboardTimestampParameter() {
        return leaderboardTimestampParameter;
    }

    @Nonnull
    public ScanComparisons prependLeaderboardKeys(@Nonnull ScanComparisons scanComparisons) {
        final Comparisons.Comparison typeComparison = leaderboardTypeParameter == null ?
                new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, leaderboardType) :
                new Comparisons.ParameterComparison(Comparisons.Type.EQUALS, leaderboardTypeParameter);
        final Comparisons.Comparison timestampComparison = leaderboardTimestampParameter == null ?
                new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, leaderboardTimestamp) :
                new Comparisons.ParameterComparison(Comparisons.Type.EQUALS, leaderboardTimestampParameter);
        return new ScanComparisons(Arrays.asList(typeComparison, timestampComparison), Collections.emptySet())
                .append(scanComparisons);
    }

    @Nonnull
    public TupleRange prependLeaderboardKeys(@Nonnull EvaluationContext context, @Nonnull TupleRange tupleRange) {
        return tupleRange.prepend(Tuple.from(getLeaderboardType(context), getLeaderboardTimestamp(context)));
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        return PlanHashable.objectsPlanHash(mode, leaderboardType, leaderboardTimestamp, leaderboardTypeParameter,
                leaderboardTimestampParameter);
    }

    @Nonnull
    public String leaderboardTypeString() {
        return leaderboardTypeParameter == null ? Integer.toString(leaderboardType) : ("$" + leaderboardTypeParameter);
    }

    @Nonnull
    public String leaderboardTimestampString() {
        return leaderboardTimestampParameter == null ? Long.toString(leaderboardTimestamp) : ("$" + leaderboardTimestampParameter);
    }

    @Override
    public String toString() {
        return leaderboardTypeString() + "," + leaderboardTimestampString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TimeWindowForFunction that = (TimeWindowForFunction) o;

        if (this.leaderboardType != that.leaderboardType) {
            return false;
        }
        if (this.leaderboardTimestamp != that.leaderboardTimestamp) {
            return false;
        }
        if (this.leaderboardTypeParameter != null ? !this.leaderboardTypeParameter.equals(that.leaderboardTypeParameter) : that.leaderboardTypeParameter != null) {
            return false;
        }
        return this.leaderboardTimestampParameter != null ? this.leaderboardTimestampParameter.equals(that.leaderboardTimestampParameter) : that.leaderboardTimestampParameter == null;
    }

    @Override
    public int hashCode() {
        int result = leaderboardType;
        result = 31 * result + (int) (leaderboardTimestamp ^ (leaderboardTimestamp >>> 32));
        result = 31 * result + (leaderboardTypeParameter != null ? leaderboardTypeParameter.hashCode() : 0);
        result = 31 * result + (leaderboardTimestampParameter != null ? leaderboardTimestampParameter.hashCode() : 0);
        return result;
    }

    @Nonnull
    @Override
    public PTimeWindowForFunction toProto(@Nonnull final PlanSerializationContext serializationContext) {
        final PTimeWindowForFunction.Builder builder = PTimeWindowForFunction.newBuilder();
        builder.setLeaderboardType(leaderboardType);
        builder.setLeaderboardTimestamp(leaderboardTimestamp);
        if (leaderboardTypeParameter != null) {
            builder.setLeaderboardTypeParameter(leaderboardTypeParameter);
        }
        if (leaderboardTimestampParameter != null) {
            builder.setLeaderboardTimestampParameter(leaderboardTimestampParameter);
        }
        return builder.build();
    }

    @Nonnull
    @SuppressWarnings("unused")
    public static TimeWindowForFunction fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                  @Nonnull final PTimeWindowForFunction timeWindowForFunctionProto) {
        Verify.verify(timeWindowForFunctionProto.hasLeaderboardType());
        Verify.verify(timeWindowForFunctionProto.hasLeaderboardTimestamp());

        final String leaderboardTypeParameter;
        if (timeWindowForFunctionProto.hasLeaderboardTypeParameter()) {
            leaderboardTypeParameter = timeWindowForFunctionProto.getLeaderboardTypeParameter();
        } else {
            leaderboardTypeParameter = null;
        }
        final String leaderboardTimestampParameter;
        if (timeWindowForFunctionProto.hasLeaderboardTimestampParameter()) {
            leaderboardTimestampParameter = timeWindowForFunctionProto.getLeaderboardTimestampParameter();
        } else {
            leaderboardTimestampParameter = null;
        }
        return new TimeWindowForFunction(timeWindowForFunctionProto.getLeaderboardType(),
                timeWindowForFunctionProto.getLeaderboardTimestamp(),
                leaderboardTypeParameter,
                leaderboardTimestampParameter);
    }
}
