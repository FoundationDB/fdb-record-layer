/*
 * ComparisonRange.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2019 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.temp;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * A set of compatible comparisons on a single field of a {@link com.apple.foundationdb.record.metadata.expressions.KeyExpression}
 * representing a contiguous range of values for that field.
 *
 * <p>
 * A {@code ComparisonRange} is similar to a {@link ScanComparisons} but represents a contiguous range of values for a
 * single field, rather than for an entire {@code KeyExpression}. A comparison range is effectively a sum type, with
 * the following options:
 * </p>
 * <ul>
 *     <li>An empty set of comparisons, indicating that the entire universe of possible values is in the range.</li>
 *     <li>
 *         An equality comparison on the field. There can only be a single such comparison. For example, single
 *         comparison of the form {@code EQUALS "foo"}.
 *     </li>
 *     <li>
 *         A set of inequality comparisons that define a single contiguous range of values. For example, the combination
 *         of the comparisons {@code > 8} and {@code < 30}. The comparison range may include redundant
 *         comparisons by contract, but it may or may not simplify the range into a more compact form. For example,
 *         the comparison range can include the comparisons {@code > 8}, {@code < 30}, and {@code < 20}, but it
 *         may optionally simplify this to {@code > 8} and {@code < 20}. Note that this behavior is not fully
 *         implemented right now; similarly, this implementation does not currently convert a range such as {@code < 8}
 *         and {@code > 30} to an empty range. However, this normalization logic will be added here in the future.
 *         <!-- TODO Add range validation to ensure that a {@code ComparisonRange} is contiguous. -->
 *     </li>
 * </ul>
 *
 * <p>
 * A {@code ComparisonRange} is an immutable object that provides a variety of methods for producing new range from the
 * current one and some {@link Comparisons.Comparison} objects. For example, see {@link #tryToAdd(Comparisons.Comparison)}
 * and {@link #from(Comparisons.Comparison)}.
 * </p>
 */
@API(API.Status.EXPERIMENTAL)
public class ComparisonRange {
    public static final ComparisonRange EMPTY = new ComparisonRange();

    @Nullable
    private final Comparisons.Comparison equalityComparison;
    @Nullable
    private final List<Comparisons.Comparison> inequalityComparisons;

    private ComparisonRange() {
        this.equalityComparison = null;
        this.inequalityComparisons = null;
    }

    private ComparisonRange(@Nonnull Comparisons.Comparison equalityComparison) {
        this.equalityComparison = equalityComparison;
        this.inequalityComparisons = null;
    }

    private ComparisonRange(@Nonnull List<Comparisons.Comparison> inequalityComparisons) {
        this.equalityComparison = null;
        this.inequalityComparisons = inequalityComparisons;
    }

    public boolean isEmpty() {
        return equalityComparison == null && inequalityComparisons == null;
    }

    public boolean isEquality() {
        return equalityComparison != null && inequalityComparisons == null;
    }

    public boolean isInequality() {
        return equalityComparison == null && inequalityComparisons != null;
    }

    @Nonnull
    public Comparisons.Comparison getEqualityComparison() {
        if (equalityComparison == null) {
            throw new RecordCoreException("tried to get non-existent equality comparison from ComparisonRange");
        }
        return equalityComparison;
    }

    @Nullable
    public List<Comparisons.Comparison> getInequalityComparisons() {
        if (inequalityComparisons == null) {
            throw new RecordCoreException("tried to get non-existent inequality comparisons from ComparisonRange");
        }
        return inequalityComparisons;
    }

    @Nonnull
    public Optional<ComparisonRange> tryToAdd(@Nonnull Comparisons.Comparison comparison) {
        // TODO This isn't actually right becuase of how ranges work. Specifically, we should check that an
        // inequality comparison is compatible with the existing set of inequality comparisons (i.e., that they define
        // a contiguous range) before adding it.
        // However, this appears to be compatible with the AndWithThenPlanner.
        if (isEmpty()) {
            return from(comparison);
        } else if (isEquality() && getEqualityComparison().equals(comparison)) {
            return Optional.of(this);
        } else if (isInequality() &&
                ScanComparisons.getComparisonType(comparison).equals(ScanComparisons.ComparisonType.INEQUALITY)) {
            return Optional.of(new ComparisonRange(ImmutableList.<Comparisons.Comparison>builder()
                    .addAll(inequalityComparisons)
                    .add(comparison)
                    .build()));
        }
        // TODO there are some subtle cases to handle. For example, != 3 and >= 3 is the same as > 3.
        return Optional.empty();
    }

    @Override
    public String toString() {
        if (isEquality()) {
            return getEqualityComparison().toString();
        } else if (isInequality()) {
            return inequalityComparisons.stream().map(Comparisons.Comparison::toString)
                            .collect(Collectors.joining(" && ", "[", "]"));
        } else {
            return "[]";
        }
    }

    @Nonnull
    public static Optional<ComparisonRange> from(@Nonnull Comparisons.Comparison comparison) {
        switch (ScanComparisons.getComparisonType(comparison)) {
            case EQUALITY:
                return Optional.of(new ComparisonRange(comparison));
            case INEQUALITY:
                return Optional.of(new ComparisonRange(Collections.singletonList(comparison)));
            case NONE:
                return Optional.empty();
            default:
                throw new RecordCoreException("unexpected comparison type");
        }
    }

}
