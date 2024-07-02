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
import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.EndpointType;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.planprotos.PScanComparisons;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordVersion;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.ComparisonRange;
import com.apple.foundationdb.record.query.plan.cascades.Correlated;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.CollectionMatcher;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.Extractor;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.PlannerBindings;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.PrimitiveMatchers;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.TypedMatcher;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Streams;
import com.google.protobuf.ByteString;
import com.google.protobuf.Internal;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.TypedMatcher.typed;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.TypedMatcherWithExtractAndDownstream.typedWithDownstream;

/**
 * A set of {@link Comparisons.Comparison} for scanning an index.
 * A <em>prefix</em> of zero or more equality comparisons for each of
 * the corresponding indexed fields, followed by zero or more
 * inequality comparisons to be applied to the next field.
 */
@API(API.Status.INTERNAL)
public class ScanComparisons implements PlanHashable, Correlated<ScanComparisons>, PlanSerializable {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Scan-Comparisons");

    @Nonnull
    protected final List<Comparisons.Comparison> equalityComparisons;
    @Nonnull
    protected final Set<Comparisons.Comparison> inequalityComparisons;

    public static final ScanComparisons EMPTY = new ScanComparisons(Collections.emptyList(), Collections.emptySet());
    
    public ScanComparisons(@Nonnull List<Comparisons.Comparison> equalityComparisons,
                           @Nonnull Set<Comparisons.Comparison> inequalityComparisons) {
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
    public Set<Comparisons.Comparison> getInequalityComparisons() {
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
            case SORT:
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
                        Collections.emptySet());
            case INEQUALITY:
                return new ScanComparisons(Collections.emptyList(),
                        Collections.singleton(comparison));
            default:
                return null;
        }
    }

    @Nullable
    public ScanComparisons merge(@Nonnull ScanComparisons other) {
        if (equalityComparisons.equals(other.equalityComparisons)) {
            Set<Comparisons.Comparison> comparisons = new HashSet<>(inequalityComparisons);
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
            super(new ArrayList<>(), new HashSet<>());
        }

        public Builder clear() {
            this.equalityComparisons.clear();
            this.inequalityComparisons.clear();
            return this;
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
                for (Comparisons.Comparison comparison : comparisonRange.getInequalityComparisons()) {
                    if (!comparison.getType().equals(Comparisons.Type.SORT)) {
                        inequalityComparisons.add(comparison);
                    }
                }
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
        public Builder addAll(@Nonnull List<Comparisons.Comparison> newEqualityComparisons,
                              @Nonnull Set<Comparisons.Comparison> newInequalityComparisons) {
            equalityComparisons.addAll(newEqualityComparisons);
            inequalityComparisons.addAll(newInequalityComparisons);
            return this;
        }

        @Nonnull
        @Override
        protected ScanComparisons.Builder withComparisons(@Nonnull List<Comparisons.Comparison> equalityComparisons,
                                                          @Nonnull Set<Comparisons.Comparison> inequalityComparisons) {
            return new Builder().addAll(equalityComparisons, inequalityComparisons);
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
            addComparandToList(items, comparison, store, context);
        }
        final Tuple baseTuple = Tuple.fromList(items);
        if (inequalityComparisons.isEmpty()) {
            return TupleRange.allOf(baseTuple);
        }

        if (inequalityComparisons.size() == 1) {
            final Comparisons.Comparison inequalityComparison = inequalityComparisons.iterator().next();
            if (inequalityComparison.getType() == Comparisons.Type.STARTS_WITH) {
                final Tuple startTuple = baseTuple.addObject(toTupleItem(inequalityComparison.getComparand(store, context)));
                return new TupleRange(startTuple, startTuple, EndpointType.PREFIX_STRING, EndpointType.PREFIX_STRING);
            }
        }

        InequalityRangeCombiner rangeCombiner = new InequalityRangeCombiner(store, context, baseTuple, inequalityComparisons);
        return rangeCombiner.toTupleRange();
    }

    @Nullable
    public TupleRange toTupleRangeWithoutContext() {
        try {
            return toTupleRange();
        } catch (Comparisons.EvaluationContextRequiredException ex) {
            return null;
        }
    }

    protected static void addComparandToList(@Nonnull List<Object> items, @Nonnull Comparisons.Comparison comparison,
                                             @Nullable FDBRecordStoreBase<?> store, @Nullable EvaluationContext context) {
        if (comparison.hasMultiColumnComparand()) {
            items.addAll(((Tuple)comparison.getComparand(store, context)).getItems());
        } else {
            items.add(toTupleItem(comparison.getComparand(store, context)));
        }
    }

    public static Object toTupleItem(@Nullable Object item) {
        if (item instanceof ByteString) {
            return ((ByteString) item).toByteArray();
        // Following two are both Internal.EnumLite, so could use that, too.
        } else if (item instanceof Internal.EnumLite) {
            return ((Internal.EnumLite) item).getNumber();
        } else if (item instanceof FDBRecordVersion) {
            return ((FDBRecordVersion) item).toVersionstamp();
        } else {
            return item;
        }
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedTo() {
        final ImmutableSet.Builder<CorrelationIdentifier> resultBuilder = ImmutableSet.builder();

        equalityComparisons.forEach(comparison -> resultBuilder.addAll(comparison.getCorrelatedTo()));
        inequalityComparisons.forEach(comparison -> resultBuilder.addAll(comparison.getCorrelatedTo()));

        return resultBuilder.build();
    }

    @Nonnull
    @Override
    public ScanComparisons rebase(@Nonnull final AliasMap translationMap) {
        return translateCorrelations(TranslationMap.rebaseWithAliasMap(translationMap));
    }

    @Override
    @SuppressWarnings({"UnstableApiUsage", "PMD.CompareObjectsWithEquals"})
    public boolean semanticEquals(@Nullable final Object other, @Nonnull final AliasMap aliasMap) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        ScanComparisons that = (ScanComparisons)other;

        if (this.equalityComparisons.size() != that.equalityComparisons.size()) {
            return false;
        }

        if (this.inequalityComparisons.size() != that.inequalityComparisons.size()) {
            return false;
        }

        return Streams.zip(this.equalityComparisons.stream(), that.equalityComparisons.stream(), (a, b) -> a.semanticEquals(b, aliasMap)).allMatch(e -> e) &&
               this.inequalityComparisons.stream()
                       .allMatch(thisComparison -> that.inequalityComparisons.stream().anyMatch(thatComparison -> thisComparison.semanticEquals(thatComparison, aliasMap)));
    }

    @Override
    public int semanticHashCode() {
        final int equalityComparisonsHash =
                equalityComparisons.stream().map(Comparisons.Comparison::semanticHashCode).collect(ImmutableList.toImmutableList()).hashCode();
        final int inequalityComparisonsHash =
                inequalityComparisons.stream().map(Comparisons.Comparison::semanticHashCode).collect(ImmutableSet.toImmutableSet()).hashCode();
        return Objects.hash(equalityComparisonsHash, inequalityComparisonsHash);
    }

    @Nonnull
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public ScanComparisons translateCorrelations(@Nonnull final TranslationMap translationMap) {
        boolean needsCopy = false;
        final var translatedEqualityComparisonsBuilder = ImmutableList.<Comparisons.Comparison>builder();
        for (final var comparison : equalityComparisons) {
            final var translatedComparison = comparison.translateCorrelations(translationMap);
            translatedEqualityComparisonsBuilder.add(translatedComparison);
            if (translatedComparison != comparison) {
                needsCopy = true;
            }
        }

        final var translatedInequalityComparisonsBuilder = ImmutableSet.<Comparisons.Comparison>builder();
        for (final var comparison : inequalityComparisons) {
            final var translatedComparison = comparison.translateCorrelations(translationMap);
            translatedInequalityComparisonsBuilder.add(translatedComparison);
            if (translatedComparison != comparison) {
                needsCopy = true;
            }
        }
        if (needsCopy) {
            return withComparisons(translatedEqualityComparisonsBuilder.build(), translatedInequalityComparisonsBuilder.build());
        }
        return this;
    }

    @Nonnull
    protected ScanComparisons withComparisons(@Nonnull List<Comparisons.Comparison> equalityComparisons,
                                              @Nonnull Set<Comparisons.Comparison> inequalityComparisons) {
        return new ScanComparisons(equalityComparisons, inequalityComparisons);
    }

    @Override
    @SpotBugsSuppressWarnings("EQ_UNUSUAL")
    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    public boolean equals(Object o) {
        return semanticEquals(o, AliasMap.emptyMap());
    }

    @Override
    public int hashCode() {
        return semanticHashCode();
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        switch (mode.getKind()) {
            case LEGACY:
                return PlanHashable.planHash(mode, equalityComparisons) + PlanHashable.planHashUnordered(mode, inequalityComparisons);
            case FOR_CONTINUATION:
                // TODO: Discuss why these should be unordered...
                return PlanHashable.objectsPlanHash(mode, BASE_HASH, equalityComparisons,
                        PlanHashable.planHashUnordered(mode, inequalityComparisons));
            default:
                throw new UnsupportedOperationException("Hash kind " + mode.getKind() + " is not supported");
        }
    }

    @Override
    public String toString() {
        Stream<String> strs = equalityComparisons.stream().map(Comparisons.Comparison::toString);
        if (!inequalityComparisons.isEmpty()) {
            strs = Stream.concat(strs, Stream.of(
                    inequalityComparisons.stream().map(Comparisons.Comparison::toString)
                            .sorted() // Make the order stable.
                            .collect(Collectors.joining(" && ", "[", "]"))));
        }
        return strs.collect(Collectors.joining(", ", "[", "]"));
    }

    @Nonnull
    @Override
    public PScanComparisons toProto(@Nonnull final PlanSerializationContext serializationContext) {
        final PScanComparisons.Builder builder = PScanComparisons.newBuilder();
        for (final Comparisons.Comparison equalityComparison : equalityComparisons) {
            builder.addEqualityComparisons(equalityComparison.toComparisonProto(serializationContext));
        }
        for (final Comparisons.Comparison inequalityComparison : inequalityComparisons) {
            builder.addInequalityComparisons(inequalityComparison.toComparisonProto(serializationContext));
        }
        return builder.build();
    }

    @Nonnull
    public static ScanComparisons fromProto(@Nonnull final PlanSerializationContext serializationContext, @Nonnull PScanComparisons scanComparisonsProto) {
        final ImmutableList.Builder<Comparisons.Comparison> equalityComparisonsBuilder = ImmutableList.builder();
        for (int i = 0; i < scanComparisonsProto.getEqualityComparisonsCount(); i ++) {
            equalityComparisonsBuilder.add(Comparisons.Comparison.fromComparisonProto(serializationContext, scanComparisonsProto.getEqualityComparisons(i)));
        }
        final ImmutableSet.Builder<Comparisons.Comparison> inequalityComparisonsBuilder = ImmutableSet.builder();
        for (int i = 0; i < scanComparisonsProto.getInequalityComparisonsCount(); i ++) {
            inequalityComparisonsBuilder.add(Comparisons.Comparison.fromComparisonProto(serializationContext, scanComparisonsProto.getInequalityComparisons(i)));
        }
        return new ScanComparisons(equalityComparisonsBuilder.build(), inequalityComparisonsBuilder.build());
    }

    @Nonnull
    public static BindingMatcher<ScanComparisons> range(@Nonnull String tupleString) {
        return typedWithDownstream(ScanComparisons.class,
                Extractor.of(
                        scanComparisons -> {
                            try {
                                return scanComparisons.toTupleRange().toString();
                            } catch (Comparisons.EvaluationContextRequiredException ex) {
                                return scanComparisons.toString();
                            }
                        }, name -> "range(" + name + ")"),
                PrimitiveMatchers.equalsObject(tupleString));
    }

    @Nonnull
    public static BindingMatcher<ScanComparisons> unbounded() {
        return new TypedMatcher<>(ScanComparisons.class) {
            @Nonnull
            @Override
            public Stream<PlannerBindings> bindMatchesSafely(@Nonnull final RecordQueryPlannerConfiguration plannerConfiguration, @Nonnull final PlannerBindings outerBindings, @Nonnull final ScanComparisons in) {
                return super.bindMatchesSafely(plannerConfiguration, outerBindings, in)
                        .flatMap(bindings -> {
                            if (in.isEmpty()) {
                                return Stream.of(bindings);
                            } else {
                                return Stream.empty();
                            }
                        });
            }
        };
    }

    @Nonnull
    public static BindingMatcher<ScanComparisons> equalities(@Nonnull CollectionMatcher<Comparisons.Comparison> equalityComparisonsCollectionMatcher) {
        return typedWithDownstream(ScanComparisons.class,
                Extractor.of(ScanComparisons::getEqualityComparisons, name -> "equalities(" + name + ")"),
                equalityComparisonsCollectionMatcher);
    }

    @Nonnull
    public static BindingMatcher<Comparisons.ParameterComparison> anyParameterComparison() {
        return typed(Comparisons.ParameterComparison.class);
    }

    @Nonnull
    public static BindingMatcher<Comparisons.ValueComparison> anyValueComparison() {
        return typed(Comparisons.ValueComparison.class);
    }

    private static class InequalityRangeCombiner {
        enum EndpointComparison {
            NONE, VALUE, MULTIPLE
        }

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
        private EndpointComparison hasLow = EndpointComparison.NONE;
        private EndpointComparison hasHigh = EndpointComparison.NONE;

        public InequalityRangeCombiner(@Nullable FDBRecordStoreBase<?> store, @Nullable EvaluationContext context, @Nonnull Tuple baseTuple,
                                       @Nonnull Set<Comparisons.Comparison> inequalityComparisons) {
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

        @SuppressWarnings("PMD.CompareObjectsWithEquals")
        public void addComparison(Comparisons.Comparison comparison) {
            final Object comparand = comparison.getComparand(store, context);
            if (comparand == Comparisons.COMPARISON_SKIPPED_BINDING) {
                return;
            }
            final EndpointComparison endpointComparison = comparison.hasMultiColumnComparand() ? EndpointComparison.MULTIPLE : EndpointComparison.VALUE;
            switch (comparison.getType()) {
                case GREATER_THAN:
                    if (lowItem == null || Comparisons.compare(lowItem, comparand) <= 0) {
                        lowItem = comparand;
                        lowEndpoint = EndpointType.RANGE_EXCLUSIVE;
                        hasLow = endpointComparison;
                    }
                    break;
                case GREATER_THAN_OR_EQUALS:
                    if (lowItem == null || Comparisons.compare(lowItem, comparand) < 0) {
                        lowItem = comparand;
                        lowEndpoint = EndpointType.RANGE_INCLUSIVE;
                        hasLow = endpointComparison;
                    }
                    break;
                case NOT_NULL:
                    if (lowItem == null) {
                        lowEndpoint = EndpointType.RANGE_EXCLUSIVE;
                        hasLow = endpointComparison;
                    }
                    break;
                case LESS_THAN:
                    if (highItem == null || Comparisons.compare(highItem, comparand) >= 0) {
                        highItem = comparand;
                        highEndpoint = EndpointType.RANGE_EXCLUSIVE;
                        hasHigh = endpointComparison;
                    }
                    if (lowItem == null) {
                        lowEndpoint = EndpointType.RANGE_EXCLUSIVE;
                        hasLow = EndpointComparison.VALUE;
                    }
                    break;
                case LESS_THAN_OR_EQUALS:
                    if (highItem == null || Comparisons.compare(highItem, comparand) > 0) {
                        highItem = comparand;
                        highEndpoint = EndpointType.RANGE_INCLUSIVE;
                        hasHigh = endpointComparison;
                    }
                    if (lowItem == null) {
                        lowEndpoint = EndpointType.RANGE_EXCLUSIVE;
                        hasLow = EndpointComparison.VALUE;
                    }
                    break;
                default:
                    throw new RecordCoreException("Unexpected inequality comparison " + comparison);
            }
        }

        @Nullable
        private Tuple buildEndpointTuple(EndpointComparison hasItem, Object item) {
            switch (hasItem) {
                case VALUE:
                    return baseTuple.addObject(toTupleItem(item));
                case MULTIPLE:
                    return baseTuple.addAll((Tuple)item);
                case NONE:
                default:
                    if (baseTuple.isEmpty()) {
                        return null;
                    } else {
                        return baseTuple;
                    }
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
