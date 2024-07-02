/*
 * LogicalSortValue.java
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

package com.apple.foundationdb.record.query.plan.cascades.expressions;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.Correlated;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Set;

/**
 * A {@link Value} with a direction used as part of a {@link LogicalSortExpression}.
 */
@API(API.Status.EXPERIMENTAL)
public class LogicalSortValue implements PlanHashable, Correlated<LogicalSortValue> {
    @Nonnull
    private final Value value;

    private final boolean descending;

    private final boolean counterflowNulls;

    public LogicalSortValue(@Nonnull Value value,
                            boolean descending, boolean counterflowNulls) {
        this.value = value;
        this.descending = descending;
        this.counterflowNulls = counterflowNulls;
    }

    public LogicalSortValue(@Nonnull Value value, boolean reverse) {
        this(value, reverse, false);
    }

    /**
     * Get the value being sorted.
     * @return the value of this sort value
     */
    @Nonnull
    public Value getValue() {
        return value;
    }

    /**
     * Get whether sort is ascending.
     * @return {@code true} is sort is ascending
     */
    public boolean isAscending() {
        return !descending;
    }

    /**
     * Get whether sort is descending.
     * @return {@code true} if sort is descending
     */
    public boolean isDescending() {
        return descending;
    }

    /**
     * Get whether null values sort on the same side as larger values.
     * @return {@code true} if nulls sort like smaller values
     */
    public boolean isCounterflowNulls() {
        return counterflowNulls;
    }

    /**
     * Get whether null values come first.
     * @return {@code true} if nulls come first
     */
    public boolean isNullsFirst() {
        return descending == counterflowNulls;
    }

    /**
     * Get whether null values come last.
     * @return {@code true} if nulls come last
     */
    public boolean isNullsLast() {
        return descending != counterflowNulls;
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        return PlanHashable.objectsPlanHash(mode, value, descending, counterflowNulls);
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedTo() {
        return value.getCorrelatedTo();
    }

    @Nonnull
    @Override
    public LogicalSortValue rebase(@Nonnull final AliasMap translationMap) {
        return new LogicalSortValue(value.rebase(translationMap), descending, counterflowNulls);
    }

    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public boolean semanticEquals(@Nullable final Object other, @Nonnull final AliasMap aliasMap) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        LogicalSortValue otherValue = (LogicalSortValue)other;
        return Correlated.semanticEquals(this.value, otherValue.value, aliasMap) &&
                this.descending == otherValue.descending &&
                this.counterflowNulls == otherValue.counterflowNulls;
    }

    @Override
    public int semanticHashCode() {
        return value.semanticHashCode();
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @SpotBugsSuppressWarnings("EQ_UNUSUAL")
    @Override
    public boolean equals(final Object obj) {
        return semanticEquals(obj, AliasMap.identitiesFor(getCorrelatedTo()));
    }

    @Override
    public int hashCode() {
        return semanticHashCode();
    }

    @Override
    public String toString() {
        return value +
                (descending ? " DESC" : "") +
                (counterflowNulls ? (isNullsFirst() ? " NULLS FIRST" : " NULLS LAST") : "");
    }
}
