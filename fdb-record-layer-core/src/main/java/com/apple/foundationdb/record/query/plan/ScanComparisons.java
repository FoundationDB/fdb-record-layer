/*
 * ScanComparisons.java
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

package com.apple.foundationdb.record.query.plan;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.EndpointType;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordVersion;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.temp.ComparisonRange;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.ProtocolMessageEnum;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A set of {@link Comparisons.Comparison} for scanning an index.
 * A <em>prefix</em> of zero or more equality comparisons for each of
 * the corresponding indexed fields, followed by zero or more
 * inequality comparisons to be applied to the next field. */
@API(API.Status.INTERNAL)
public class ScanComparisons implements PlanHashable {
    @Nonnull
    protected final List<Comparisons.Comparison> equalityComparisons;
    @Nonnull
    protected final List<Comparisons.Comparison> inequalityComparisons;

    public static final ScanComparisons EMPTY = new ScanComparisons(Collections.emptyList(), Collections.emptyList());
    
    public ScanComparisons(@Nonnull List<Comparisons.Comparison> equalityComparisons,
                           @Nonnull List<Comparisons.Comparison> inequalityComparisons) {
        checkComparisonTypes(equalityComparisons, ComparisonType.EQUALITY);
        checkComparisonTypes(inequalityComparisons, ComparisonType.INEQUALITY);
        this.equalityComparisons = equalityComparisons;
        this.inequalityComparisons = inequalityComparisons;
    }

    private static void checkComparisonTypes(@Nonnull Iterable<Comparisons.Comparison> comparisons,
                                             @Nonnull ComparisonType comparisonType) {
        for (Comparisons.Comparison comparison : comparisons) {
            if (getComparisonType(comparison) != comparisonType) {
                throw new RecordCoreException("wrong comparison type for " + comparison +
                                              ", required " + comparisonType);
            }
        }
    }

    @Nonnull
    public List<Comparisons.Comparison> getEqualityComparisons() {
        return equalityComparisons;
    }

    @Nonnull
    public List<Comparisons.Comparison> getInequalityComparisons() {
        return inequalityComparisons;
    }

    public int getEqualitySize() {
        return equalityComparisons.size();
    }

    public int size() {
        int result = equalityComparisons.size();
        if (!inequalityComparisons.isEmpty()) {
            result++;
        }
        return result;
    }

    public int totalSize() {
        return equalityComparisons.size() + inequalityComparisons.size();
    }

    public boolean isEmpty() {
        return equalityComparisons.isEmpty() && inequalityComparisons.isEmpty();
    }

    public boolean isEquality() {
        return inequalityComparisons.isEmpty();
    }

    /**
     * The type of a comparison.
     */
    public enum ComparisonType {
        EQUALITY, INEQUALITY, NONE
    }

    @Nonnull
    public static ComparisonType getComparisonType(@Nonnull Comparisons.Comparison comparison) {
        switch (comparison.getType()) {
            case EQUALS:
            case IS_NULL:
                return ComparisonType.EQUALITY;
            case LESS_THAN:
            case LESS_THAN_OR_EQUALS:
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUALS:
            case STARTS_WITH:
            case NOT_NULL:
                return ComparisonType.INEQUALITY;
            case NOT_EQUALS:
            default:
                return ComparisonType.NONE;
        }
    }

    @Nullable
    public static ScanComparisons from(@Nonnull Comparisons.Comparison comparison) {
        switch (getComparisonType(comparison)) {
            case EQUALITY:
                return new ScanComparisons(Collections.singletonList(comparison),
                        Collections.emptyList());
            case INEQUALITY:
                return new ScanComparisons(Collections.emptyList(),
                        Collections.singletonList(comparison));
            default:
                return null;
        }
    }

    @Nullable
    public ScanComparisons merge(@Nonnull ScanComparisons other) {
        if (equalityComparisons.equals(other.equalityComparisons)) {
            List<Comparisons.Comparison> comparisons = new ArrayList<>(inequalityComparisons);
            comparisons.addAll(other.inequalityComparisons);
            return new ScanComparisons(equalityComparisons, comparisons);
        }
        return null;
    }

    @Nullable
    public ScanComparisons append(@Nonnull ScanComparisons other) {
        if (isEquality()) {
            if (other.equalityComparisons.isEmpty()) {
                return new ScanComparisons(equalityComparisons, other.inequalityComparisons);
            } else {
                List<Comparisons.Comparison> comparisons = new ArrayList<>(equalityComparisons);
                comparisons.addAll(other.equalityComparisons);
                return new ScanComparisons(comparisons, other.inequalityComparisons);
            }
        }
        return null;
    }

    /**
     * A builder for {@link ScanComparisons}.
     */
    public static class Builder extends ScanComparisons {
        public Builder() {
            super(new ArrayList<>(), new ArrayList<>());
        }

        @Nonnull
        public Builder addEqualityComparison(@Nonnull Comparisons.Comparison comparison) {
            if (!inequalityComparisons.isEmpty()) {
                throw new RecordCoreException("Cannot add equality comparison after inequalities");
            }
            equalityComparisons.add(comparison);
            return this;
        }

        @Nonnull
        public Builder addInequalityComparison(@Nonnull Comparisons.Comparison comparison) {
            inequalityComparisons.add(comparison);
            return this;
        }

        @API(API.Status.EXPERIMENTAL)
        @Nonnull
        public Builder addComparisonRange(@Nonnull ComparisonRange comparisonRange) {
            if (comparisonRange.isEquality()) {
                addEqualityComparison(comparisonRange.getEqualityComparison());
            } else if (comparisonRange.isInequality()) {
                inequalityComparisons.addAll(comparisonRange.getInequalityComparisons());
            }
            return this;
        }

        @Nonnull
        public Builder addAll(@Nonnull ScanComparisons other) {
            equalityComparisons.addAll(other.equalityComparisons);
            inequalityComparisons.addAll(other.inequalityComparisons);
            return this;
        }

        @Nonnull
        public ScanComparisons build() {
            return new ScanComparisons(equalityComparisons, inequalityComparisons);
        }
    }

    @Nonnull
    public TupleRange toTupleRange() {
        return toTupleRange(null, null);
    }

    @Nonnull
    public TupleRange toTupleRange(@Nullable FDBRecordStoreBase<?> store, @Nullable EvaluationContext context) {
        if (isEmpty()) {
            return TupleRange.ALL;
        }

        final List<Object> items = new ArrayList<>(equalityComparisons.size());
        for (Comparisons.Comparison comparison : equalityComparisons) {
            items.add(toTupleItem(comparison.getComparand(store, context)));
        }
        final Tuple baseTuple = Tuple.fromList(items);
        if (inequalityComparisons.isEmpty()) {
            return TupleRange.allOf(baseTuple);
        }

        if (inequalityComparisons.size() == 1 &&
                inequalityComparisons.get(0).getType() == Comparisons.Type.STARTS_WITH) {
            final Tuple startTuple = baseTuple.addObject(toTupleItem(inequalityComparisons.get(0).getComparand(store, context)));
            return new TupleRange(startTuple, startTuple, EndpointType.PREFIX_STRING, EndpointType.PREFIX_STRING);
        }

        InequalityRangeCombiner rangeCombiner = new InequalityRangeCombiner(store, context, baseTuple, inequalityComparisons);
        return rangeCombiner.toTupleRange();
    }


    public static Object toTupleItem(@Nullable Object item) {
        if (item instanceof ByteString) {
            return ((ByteString) item).toByteArray();
        // Following two are both Internal.EnumLite, so could use that, too.
        } else if (item instanceof ProtocolMessageEnum) {
            return ((ProtocolMessageEnum) item).getNumber();
        } else if (item instanceof Descriptors.EnumValueDescriptor) {
            return ((Descriptors.EnumValueDescriptor) item).getNumber();
        } else if (item instanceof FDBRecordVersion) {
            return ((FDBRecordVersion) item).toVersionstamp();
        } else {
            return item;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ScanComparisons that = (ScanComparisons)o;
        return this.equalityComparisons.equals(that.equalityComparisons) &&
               this.inequalityComparisons.equals(that.inequalityComparisons);
    }

    @Override
    public int hashCode() {
        return equalityComparisons.hashCode() + inequalityComparisons.hashCode();
    }

    @Override
    public int planHash() {
        return PlanHashable.planHash(equalityComparisons) + PlanHashable.planHash(inequalityComparisons);
    }

    @Override
    public String toString() {
        Stream<String> strs = equalityComparisons.stream().map(Comparisons.Comparison::toString);
        if (!inequalityComparisons.isEmpty()) {
            strs = Stream.concat(strs, Stream.of(
                    inequalityComparisons.stream().map(Comparisons.Comparison::toString)
                            .collect(Collectors.joining(" && ", "[", "]"))));
        }
        return strs.collect(Collectors.joining(", ", "[", "]"));
    }

    private static class InequalityRangeCombiner {
        @Nullable
        FDBRecordStoreBase<?> store;
        @Nullable
        private final EvaluationContext context;
        @Nonnull
        private final Tuple baseTuple;
        private Object lowItem = null;
        private Object highItem = null;
        private EndpointType lowEndpoint;
        private EndpointType highEndpoint;
        private boolean hasLow = false;
        private boolean hasHigh = false;

        public InequalityRangeCombiner(@Nullable FDBRecordStoreBase<?> store, @Nullable EvaluationContext context, @Nonnull Tuple baseTuple,
                                       @Nonnull List<Comparisons.Comparison> inequalityComparisons) {
            this.store = store;
            this.context = context;
            this.baseTuple = baseTuple;

            if (baseTuple.isEmpty()) {
                lowEndpoint = EndpointType.TREE_START;
                highEndpoint = EndpointType.TREE_END;
            } else {
                lowEndpoint = highEndpoint = EndpointType.RANGE_INCLUSIVE;
            }
            for (Comparisons.Comparison comparison : inequalityComparisons) {
                addComparison(comparison);
            }
        }

        public void addComparison(Comparisons.Comparison comparison) {
            final Object comparand = comparison.getComparand(store, context);
            if (comparand == Comparisons.COMPARISON_SKIPPED_BINDING) {
                return;
            }
            switch (comparison.getType()) {
                case GREATER_THAN:
                    if (lowItem == null || Comparisons.compare(lowItem, comparand) <= 0) {
                        lowItem = comparand;
                        lowEndpoint = EndpointType.RANGE_EXCLUSIVE;
                        hasLow = true;
                    }
                    break;
                case GREATER_THAN_OR_EQUALS:
                    if (lowItem == null || Comparisons.compare(lowItem, comparand) < 0) {
                        lowItem = comparand;
                        lowEndpoint = EndpointType.RANGE_INCLUSIVE;
                        hasLow = true;
                    }
                    break;
                case NOT_NULL:
                    if (lowItem == null) {
                        lowEndpoint = EndpointType.RANGE_EXCLUSIVE;
                        hasLow = true;
                    }
                    break;
                case LESS_THAN:
                    if (highItem == null || Comparisons.compare(highItem, comparand) >= 0) {
                        highItem = comparand;
                        highEndpoint = EndpointType.RANGE_EXCLUSIVE;
                        hasHigh = true;
                    }
                    if (lowItem == null) {
                        lowEndpoint = EndpointType.RANGE_EXCLUSIVE;
                        hasLow = true;
                    }
                    break;
                case LESS_THAN_OR_EQUALS:
                    if (highItem == null || Comparisons.compare(highItem, comparand) > 0) {
                        highItem = comparand;
                        highEndpoint = EndpointType.RANGE_INCLUSIVE;
                        hasHigh = true;
                    }
                    if (lowItem == null) {
                        lowEndpoint = EndpointType.RANGE_EXCLUSIVE;
                        hasLow = true;
                    }
                    break;
                default:
                    throw new RecordCoreException("Unexpected inequality comparison " + comparison);
            }
        }

        @Nullable
        private Tuple buildEndpointTuple(boolean hasItem, Object item) {
            if (hasItem) {
                return baseTuple.addObject(toTupleItem(item));
            } else if (baseTuple.isEmpty()) {
                return null;
            } else {
                return baseTuple;
            }
        }

        @Nonnull
        public TupleRange toTupleRange() {
            Tuple low = buildEndpointTuple(hasLow, lowItem);
            Tuple high = buildEndpointTuple(hasHigh, highItem);
            return new TupleRange(low, high, lowEndpoint, highEndpoint);
        }
    }
}
