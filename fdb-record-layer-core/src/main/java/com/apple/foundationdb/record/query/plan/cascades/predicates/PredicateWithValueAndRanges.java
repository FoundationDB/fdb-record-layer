/*
 * PredicateWithValueAndRanges.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2023 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.cascades.predicates;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.PlanDeserializer;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.planprotos.PPredicateWithValueAndRanges;
import com.apple.foundationdb.record.planprotos.PQueryPredicate;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.QueryPlanConstraint;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.BooleanWithConstraint;
import com.apple.foundationdb.record.query.plan.cascades.ComparisonRange;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.LinkedIdentitySet;
import com.apple.foundationdb.record.query.plan.cascades.PartialMatch;
import com.apple.foundationdb.record.query.plan.cascades.PredicateMultiMap.PredicateCompensationFunction;
import com.apple.foundationdb.record.query.plan.cascades.PredicateMultiMap.PredicateMapping;
import com.apple.foundationdb.record.query.plan.cascades.ValueEquivalence;
import com.apple.foundationdb.record.query.plan.cascades.values.ConstantObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.google.auto.service.AutoService;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Streams;
import com.google.protobuf.Message;
import org.checkerframework.checker.nullness.qual.NonNull;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

/**
 * This class associates a {@link Value} with a set of range constraints ({@link RangeConstraints}). Each one of these
 * range constraints refers to a conjunction of:
 * <ul>
 *  <li> a contiguous compile-time evaluable range. </li>
 *  <li> a set of non-compile-time (deferred) ranges. </li>
 * </ul>
 * <br>
 * The set here represents a disjunction of these ranges. So, in a way, this class represents a boolean expression in
 * DNF form defined on the associated {@link Value}.
 * <br>
 * It is mainly used for index matching, i.e. it is not evaluable at runtime. On the query side it is normally used to
 * represent a search-argument (sargable). On the candidate side, it is normally used to either represent a restriction
 * on a specific attribute of a scan.
 * <br>
 * If the attribute is indexed, we use the {@link Placeholder} subtype to represent it along with its alias used later
 * on for substituting one of the index scan search prefix) and an (optional) range(s) defined on it to semantically
 * represent the filtering nature of the associated index and use it to plan accordingly.
 * <br>
 * If the attribute, however, is not indexed, then we use an instance of {@code this} class as a restriction on that
 * particular attribute.
 */
@API(API.Status.EXPERIMENTAL)
public class PredicateWithValueAndRanges extends AbstractQueryPredicate implements PredicateWithValue, PredicateWithComparisons {

    /**
     * The value associated with the {@code ranges}.
     */
    @Nonnull
    private final Value value;

    /**
     * A set of ranges, implicitly defining a boolean predicate in DNF form defined on the {@code value}.
     */
    @Nonnull
    private final Set<RangeConstraints> ranges;

    @Nonnull
    private final Supplier<Boolean> rangesCompileTimeChecker;

    protected PredicateWithValueAndRanges(@Nonnull final PlanSerializationContext serializationContext,
                                          @Nonnull final PPredicateWithValueAndRanges predicateWithValueAndRangesProto) {
        super(serializationContext, Objects.requireNonNull(predicateWithValueAndRangesProto.getSuper()));
        this.value = Value.fromValueProto(serializationContext, Objects.requireNonNull(predicateWithValueAndRangesProto.getValue()));
        ImmutableSet.Builder<RangeConstraints> rangeConstraintsBuilder = ImmutableSet.builder();
        for (int i = 0; i < predicateWithValueAndRangesProto.getRangesCount(); i ++) {
            rangeConstraintsBuilder.add(RangeConstraints.fromProto(serializationContext, predicateWithValueAndRangesProto.getRanges(i)));
        }
        this.ranges = rangeConstraintsBuilder.build();
        this.rangesCompileTimeChecker = () -> ranges.stream().allMatch(RangeConstraints::isCompileTime);
    }

    /**
     * Creates a new instance of {@link PredicateWithValueAndRanges}.
     *
     * @param value The value.
     * @param ranges A set of ranges defined on the value (can be empty).
     */
    protected PredicateWithValueAndRanges(@Nonnull final Value value, @Nonnull final Set<RangeConstraints> ranges) {
        super(false);
        this.value = value;
        this.ranges = ImmutableSet.copyOf(ranges);
        this.rangesCompileTimeChecker = () -> ranges.stream().allMatch(RangeConstraints::isCompileTime);
    }

    @Override
    @Nonnull
    public Value getValue() {
        return value;
    }

    @Nonnull
    @Override
    public PredicateWithValueAndRanges withValue(@Nonnull final Value value) {
        return new PredicateWithValueAndRanges(value, ranges);
    }

    @Nonnull
    public PredicateWithValueAndRanges withRanges(@Nonnull final Set<RangeConstraints> ranges) {
        return new PredicateWithValueAndRanges(value, ranges);
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedToWithoutChildren() {
        return Streams.concat(value.getCorrelatedTo().stream(),
                ranges.stream().flatMap(r -> r.getCorrelatedTo().stream()))
                .collect(ImmutableSet.toImmutableSet());
    }

    @Nonnull
    @Override
    public List<Comparisons.Comparison> getComparisons() {
        return ranges.stream()
                .flatMap(range -> range.getComparisons().stream())
                .collect(ImmutableList.toImmutableList());
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @SpotBugsSuppressWarnings("EQ_UNUSUAL")
    @Override
    public boolean equals(final Object other) {
        return semanticEquals(other, AliasMap.emptyMap());
    }

    /**
     * Performs algebraic equality between {@code this} and {@code other}, if {@code other} is also a {@link PredicateWithValueAndRanges}.
     *
     * @param other The other predicate
     * @param valueEquivalence the value equivalence.
     * @return {@code true} if both predicates are equal, otherwise {@code false}.
     */
    @Nonnull
    @Override
    public BooleanWithConstraint equalsWithoutChildren(@Nonnull final QueryPredicate other, @Nonnull final ValueEquivalence valueEquivalence) {
        return PredicateWithValue.super.equalsWithoutChildren(other, valueEquivalence)
                .compose(ignored -> {
                    final PredicateWithValueAndRanges that = (PredicateWithValueAndRanges)other;
                    return value.semanticEquals(that.value, valueEquivalence);
                })
                .compose(ignored -> {
                    final PredicateWithValueAndRanges that = (PredicateWithValueAndRanges)other;
                    return valueEquivalence.semanticEquals(ranges, that.ranges);
                });
    }

    @Override
    public int hashCode() {
        return semanticHashCode();
    }

    @Override
    public int computeSemanticHashCode() {
        return PredicateWithValue.super.computeSemanticHashCode();
    }

    @Override
    public int hashCodeWithoutChildren() {
        // TODO why not the ranges
        return value.semanticHashCode();
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        throw new RecordCoreException("this method should not ever be reached");
    }

    @Nonnull
    public Set<RangeConstraints> getRanges() {
        return ranges;
    }

    public boolean isSargable() {
        return ranges.size() == 1;
    }

    @Nonnull
    @Override
    public PredicateWithValueAndRanges translateLeafPredicate(@Nonnull final TranslationMap translationMap) {
        return new PredicateWithValueAndRanges(value.translateCorrelations(translationMap),
                ranges.stream().map(range -> range.translateCorrelations(translationMap)).collect(ImmutableSet.toImmutableSet()));
    }

    @Nonnull
    public static PredicateWithValueAndRanges sargable(@Nonnull Value value, @Nonnull final RangeConstraints range) {
        return new PredicateWithValueAndRanges(value, ImmutableSet.of(range));
    }

    @Nonnull
    public static PredicateWithValueAndRanges ofRanges(@Nonnull final Value value, @Nonnull final Set<RangeConstraints> ranges) {
        return new PredicateWithValueAndRanges(value, ranges);
    }

    @Nonnull
    @Override
    public Optional<PredicateWithValueAndRanges> translateValueAndComparisonsMaybe(@Nonnull final UnaryOperator<Value> valueTranslator,
                                                                                   @Nonnull final Function<Comparisons.Comparison, Optional<Comparisons.Comparison>> comparisonTranslator) {
        final var newValue = Verify.verifyNotNull(valueTranslator.apply(this.getValue()));
        final var newRangesBuilder = ImmutableSet.<RangeConstraints>builder();
        for (final var range : ranges) {
            final var newRangeOptional = range.translateRanges(comparisonTranslator);
            if (newRangeOptional.isEmpty()) {
                return Optional.empty();
            }
            newRangesBuilder.add(newRangeOptional.get());
        }

        return Optional.of(withValueAndRanges(newValue, newRangesBuilder.build()));
    }

    @Nonnull
    public PredicateWithValueAndRanges withValueAndRanges(@Nonnull final Value value,
                                                          @Nonnull final Set<RangeConstraints> ranges) {
        return new PredicateWithValueAndRanges(value, ranges);
    }

    /**
     * Checks whether this predicate implies a {@code candidatePredicate}, if so, we return a {@link PredicateMapping}
     * reflecting the implication itself in addition to a context needed to effectively implement the mapping (i.e. the
     * necessity to apply a residual on top).
     * <br>
     * The implication is done by pattern matching the following cases:
     * <ul>
     *  <li>If {@code candidatePredicate} is a contradiction, we do not have an implication, so we return an empty
     *  mapping.</li>
     *  <li>If {@code candidatePredicate} is a tautology, we always have an implication, so we return a mapping with an application
     *  of residual on top</li>
     *  <li>If {@code candidatePredicate} is a {@link PredicateWithValueAndRanges} and the values on both sides are semantically equal
     *  to each other and the candidate's domain is unbounded, we have an implication, so
     *  we return a mapping with a residual application on top depending on whether the candidate's alias can be used in
     *  the index scan prefix or not.</li>
     *  <li>If {@code candidatePredicate} is a {@link PredicateWithValueAndRanges} and the values on both sides are semantically equal
     *  to each other and the candidate domain is bound, then we check if {@code this} range is enclosed by the candidate's
     *  range, if so, we have an implication and we proceed to create a mapping similar to the above logic.</li>
     * </ul>
     *
     * @param valueEquivalence the current value equivalence
     * @param originalQueryPredicate the original (untranslated) predicate on the query side
     * @param candidatePredicate another predicate (usually in a match candidate)
     * @param evaluationContext the evaluation context used to evaluate any compile-time constants when examining predicate
     * implication.
     *
     * @return an optional {@link PredicateMapping} representing the result of the implication.
     */
    @Nonnull
    @Override
    public Optional<PredicateMapping> impliesCandidatePredicateMaybe(@NonNull final ValueEquivalence valueEquivalence,
                                                                     @Nonnull final QueryPredicate originalQueryPredicate,
                                                                     @Nonnull final QueryPredicate candidatePredicate,
                                                                     @Nonnull final EvaluationContext evaluationContext) {
        if (candidatePredicate.isContradiction()) {
            return Optional.empty();
        }

        if (candidatePredicate instanceof PredicateWithValueAndRanges) {
            final var candidatePredicateWithValuesAndRanges = (PredicateWithValueAndRanges)candidatePredicate;

            final var matchPairOptional =
                    getValue().matchAndCompensateComparisonMaybe(candidatePredicateWithValuesAndRanges.getValue(),
                            valueEquivalence);
            if (matchPairOptional.isEmpty()) {
                return Optional.empty();
            }
            final var matchPair = matchPairOptional.get();
            final var comparisonCompensation = matchPair.getLeft();
            final var constraint = matchPair.getRight();

            final var compensatedQueryPredicateOptional =
                    translateValueAndComparisonsMaybe(comparisonCompensation::applyToValue,
                            comparisonCompensation::applyToComparisonMaybe);
            if (compensatedQueryPredicateOptional.isEmpty()) {
                return Optional.empty();
            }
            final var compensatedQueryPredicate = compensatedQueryPredicateOptional.get();

            // candidate has no ranges (i.e. it is not filtered).
            if (candidatePredicateWithValuesAndRanges.getRanges().isEmpty()) {
                if (candidatePredicateWithValuesAndRanges instanceof WithAlias) {
                    final var alias = ((WithAlias)candidatePredicateWithValuesAndRanges).getParameterAlias();
                    final var predicateMappingBuilder =
                            PredicateMapping.regularMappingBuilder(originalQueryPredicate, this, candidatePredicate)
                                    .setPredicateCompensation((ignore, boundParameterPrefixMap) -> {
                                        if (boundParameterPrefixMap.containsKey(alias)) {
                                            return PredicateCompensationFunction.noCompensationNeeded();
                                        }
                                        return reapplyPredicateCompensationFunction();
                                    })
                                    .setParameterAlias(alias)
                                    .setConstraint(constraint);

                    Verify.verify(isSargable() == compensatedQueryPredicate.isSargable());
                    if (compensatedQueryPredicate.isSargable()) {
                        predicateMappingBuilder.setParameterAlias(alias);
                        predicateMappingBuilder.setComparisonRange(
                                Iterables.getOnlyElement(compensatedQueryPredicate.getRanges())
                                        .asComparisonRange());
                    }
                    return Optional.of(predicateMappingBuilder.build());
                } else {
                    return Optional.empty();
                }
            }

            final var candidateRanges = candidatePredicateWithValuesAndRanges.getRanges();
            if (compensatedQueryPredicate.getRanges()
                    .stream()
                    .allMatch(range -> candidateRanges.stream()
                            .anyMatch(candidateRange -> candidateRange.encloses(range, evaluationContext).coalesce()))) {
                if (candidatePredicateWithValuesAndRanges instanceof WithAlias) {
                    final var alias = ((WithAlias)candidatePredicateWithValuesAndRanges).getParameterAlias();
                    final var predicateMappingBuilder =
                            PredicateMapping.regularMappingBuilder(originalQueryPredicate, this, candidatePredicate)
                                    .setPredicateCompensation((ignore, boundParameterPrefixMap) -> {
                                        if (boundParameterPrefixMap.containsKey(alias)) {
                                            return PredicateCompensationFunction.noCompensationNeeded();
                                        }
                                        return reapplyPredicateCompensationFunction();
                                    })
                                    .setConstraint(constraint.compose(captureConstraint(candidatePredicateWithValuesAndRanges)));
                    Verify.verify(isSargable() == compensatedQueryPredicate.isSargable());
                    if (compensatedQueryPredicate.isSargable()) {
                        predicateMappingBuilder.setParameterAlias(alias);
                        predicateMappingBuilder.setComparisonRange(
                                Iterables.getOnlyElement(compensatedQueryPredicate.getRanges())
                                        .asComparisonRange());
                    }
                    return Optional.of(predicateMappingBuilder.build());
                } else {
                    return Optional.of(
                            PredicateMapping.regularMappingBuilder(originalQueryPredicate, this, candidatePredicate)
                                    .setPredicateCompensation((ignore, alsoIgnore) -> {
                                        // no need for compensation if range boundaries match between candidate constraint and query sargable
                                        if (candidateRanges.stream()
                                                .allMatch(candidateRange -> getRanges().stream()
                                                        .anyMatch(range -> range.encloses(candidateRange, evaluationContext).coalesce()))) {
                                            return PredicateCompensationFunction.noCompensationNeeded();
                                        }

                                        //
                                        // Check if ranges are semantically equal. Note that the constraint is actually captured
                                        // outside of this lambda.
                                        //
                                        if (getRanges().stream()
                                                .allMatch(left -> candidateRanges.stream()
                                                        .anyMatch(right -> left.semanticEquals(right, valueEquivalence).isTrue()))) {
                                            return PredicateCompensationFunction.noCompensationNeeded();
                                        }
                                        return reapplyPredicateCompensationFunction();
                                    })
                                    .setConstraint(constraint.compose(captureConstraint(candidatePredicateWithValuesAndRanges)))
                                    .build());
                }
            }
        }

        if (candidatePredicate.isTautology()) {
            return Optional.of(
                    PredicateMapping.regularMappingBuilder(originalQueryPredicate, this, candidatePredicate)
                            .setPredicateCompensation((ignore, alsoIgnore) -> reapplyPredicateCompensationFunction())
                            .build());
        }

        //
        // The candidate predicate is not a placeholder which means that the match candidate can not be
        // parameterized by a mapping of this to the candidate predicate. Therefore, in order to match at all,
        // it must be semantically equivalent.
        //
        final var semanticEquals = semanticEquals(candidatePredicate, valueEquivalence);
        if (semanticEquals.isFalse()) {
            return Optional.empty();
        }

        // Note that we never have to reapply the predicate as both sides are always semantically
        // equivalent.
        return Optional.of(
                PredicateMapping.regularMappingBuilder(originalQueryPredicate, this, candidatePredicate)
                        .setConstraint(semanticEquals.getConstraint())
                        .build());
    }

    @Nonnull
    @Override
    public PredicateCompensationFunction computeCompensationFunction(@Nonnull final PartialMatch partialMatch,
                                                                     @Nonnull final Map<CorrelationIdentifier, ComparisonRange> boundParameterPrefixMap,
                                                                     @Nonnull final List<PredicateCompensationFunction> childrenResults) {
        Verify.verify(childrenResults.isEmpty());
        return reapplyPredicateCompensationFunction();
    }

    private PredicateCompensationFunction reapplyPredicateCompensationFunction() {
        return PredicateCompensationFunction.of(translationMap -> LinkedIdentitySet.of(toResidualPredicate().translateCorrelations(translationMap)));
    }

    /**
     * transforms this Sargable into a conjunction of equality and non-equality predicates.
     * @return a conjunction of equality and non-equality predicates.
     */
    @Override
    @Nonnull
    public QueryPredicate toResidualPredicate() {
        // todo: check if we have single range and no ranges.
        final ImmutableList.Builder<QueryPredicate> dnfParts = ImmutableList.builder();
        for (final var range : ranges) {
            final ImmutableList.Builder<QueryPredicate> residuals = ImmutableList.builder();
            residuals.addAll(range.getComparisons().stream().map(c -> getValue().withComparison(c)).collect(ImmutableList.toImmutableList()));
            dnfParts.add(AndPredicate.and(residuals.build()));
        }
        return OrPredicate.or(dnfParts.build());
    }

    @Nonnull
    @Override
    public Optional<PredicateWithValueAndRanges> toValueWithRangesMaybe(final @Nonnull EvaluationContext evaluationContext) {
        return Optional.of(compileTimeEvalRanges(evaluationContext));
    }

    @Override
    public String toString() {
        return "(" + getValue() +
                (ranges.isEmpty() ? "" : " " + ranges.stream().map(RangeConstraints::toString).collect(Collectors.joining("||"))) +
                ")";
    }

    @Nonnull
    private PredicateWithValueAndRanges compileTimeEvalRanges(@Nonnull final EvaluationContext evaluationContext) {
        if (rangesCompileTimeChecker.get()) {
            return this;
        }
        final var newRanges = ImmutableSet.<RangeConstraints>builder();
        for (final var range : ranges) {
            newRanges.add(range.compileTimeEval(evaluationContext));
        }
        return withValueAndRanges(value, newRanges.build());
    }

    /**
     * Captures a given candidate predicate into a plan constraint that is added to the corresponding physical plan operator.
     * This is important to make sure a logical expression's eligibility of using a physical plan containing this candidate
     * predicate.
     * <br>
     * The construction is done by pulling _each_ stripped literal on the query predicate (i.e. a {@link ConstantObjectValue})
     * and construct, for each one of them, a {@link PredicateWithValueAndRanges} having it on LHS, and the candidate predicate
     * ranges on the RHS.
     * The logical grouping of the {@link ConstantObjectValue} is preserved. Here is an example:
     * <br>
     * Query Predicate: (Value1, ( ((GT,#COV1) AND (LT,#COV4)) OR ((EQ,#COV5)) ) (note that the ranges in {@link PredicateWithValueAndRanges} is in DNF format).
     * <br>
     * Candidate Predicate: (Value1, ((LTE,1000) AND (LTE,2000)))
     * <br>
     * The resulting constraint: ((#COV1, ((LTE,1000) AND (LTE,2000))) AND (#COV4, ((LTE,1000) AND (LTE,2000))) OR (#COV5,((LTE,1000) AND (LTE,2000)))
     * <br>
     * Any candidate range that is exclusive is turned into inclusive, this is necessary, so we can match, for example, query
     * predicates with exactly the same range boundaries.
     *
     * @param candidatePredicate The candidate predicate to capture as a {@link QueryPlanConstraint}.
     * @return The resulting {@link QueryPlanConstraint}.
     */
    @Nonnull
    private QueryPlanConstraint captureConstraint(@Nonnull final PredicateWithValueAndRanges candidatePredicate) {
        final var candidateRanges = candidatePredicate.getRanges().stream().map(constraint -> {
            final var builder = RangeConstraints.newBuilder();
            constraint.getComparisons().stream().map(PredicateWithValueAndRanges::exclusiveToInclusive).forEach(builder::addComparisonMaybe);
            return builder.build();
        }).flatMap(Optional::stream).collect(Collectors.toSet());
        final ImmutableList.Builder<QueryPredicate> conjunctions = ImmutableList.builder();
        for (final var queryRange : getRanges()) {
            conjunctions.add(AndPredicate.and(queryRange.getComparisons()
                    .stream()
                    .filter(comparison -> comparison instanceof Comparisons.ValueComparison)
                    .map(valueComparison -> ((Comparisons.ValueComparison)valueComparison).getComparandValue())
                    .map(constant -> PredicateWithValueAndRanges.ofRanges(constant, candidateRanges))
                    .collect(Collectors.toList())));
        }
        final var orPredicate = OrPredicate.or(conjunctions.build());
        return QueryPlanConstraint.ofPredicate(orPredicate);
    }

    @Nonnull
    private static Comparisons.Comparison exclusiveToInclusive(@Nonnull final Comparisons.Comparison comparison) {
        switch (comparison.getType()) {
            case LESS_THAN:
                return comparison.withType(Comparisons.Type.LESS_THAN_OR_EQUALS);
            case GREATER_THAN:
                return comparison.withType(Comparisons.Type.GREATER_THAN_OR_EQUALS);
            case NOT_EQUALS: // fallthrough
            case LESS_THAN_OR_EQUALS: // fallthrough
            case EQUALS: // fallthrough
            case GREATER_THAN_OR_EQUALS: // fallthrough
            case STARTS_WITH: // fallthrough
            case NOT_NULL: // fallthrough
            case IS_NULL: // fallthrough
            case IN: // fallthrough
            case TEXT_CONTAINS_ALL: // fallthrough
            case TEXT_CONTAINS_ALL_WITHIN: // fallthrough
            case TEXT_CONTAINS_ANY: // fallthrough
            case TEXT_CONTAINS_PHRASE: // fallthrough
            case TEXT_CONTAINS_PREFIX: // fallthrough
            case TEXT_CONTAINS_ALL_PREFIXES: // fallthrough
            case TEXT_CONTAINS_ANY_PREFIX: // fallthrough
            case SORT: // fallthrough
            default:
                return comparison;
        }
    }

    @Nullable
    @Override
    public <M extends Message> Boolean eval(@Nullable final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context) {
        if (!(value instanceof Value.RangeMatchableValue)) {
            throw new RecordCoreException("attempt to compile-time predicate with non-compile-time value.");
        }
        final var valueObject = value.eval(store, context);
        if (valueObject == null) {
            return null;
        }
        // lift value object to singleton range.x
        final var builder = RangeConstraints.newBuilder();
        builder.addComparisonMaybe(new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, valueObject));
        final var valueRange = builder.build().orElseThrow();
        for (final var range : getRanges()) {
            final var compiledRange = range.compileTimeEval(context);
            if (!compiledRange.isCompileTimeEvaluable()) {
                continue;
            }
            if (compiledRange.encloses(valueRange, context).coalesce()) {
                return true;
            }
        }
        return false;
    }

    @Nonnull
    @Override
    public PPredicateWithValueAndRanges toProto(@Nonnull final PlanSerializationContext serializationContext) {
        final PPredicateWithValueAndRanges.Builder builder =
                PPredicateWithValueAndRanges.newBuilder()
                        .setSuper(toAbstractQueryPredicateProto(serializationContext))
                        .setValue(value.toValueProto(serializationContext));
        for (final RangeConstraints range : ranges) {
            builder.addRanges(range.toProto(serializationContext));
        }
        return builder.build();
    }

    @Nonnull
    @Override
    public PQueryPredicate toQueryPredicateProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PQueryPredicate.newBuilder().setPredicateWithValueAndRanges(toProto(serializationContext)).build();
    }

    @Nonnull
    public static PredicateWithValueAndRanges fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                        @Nonnull final PPredicateWithValueAndRanges predicateWithValueAndRangesProto) {
        return new PredicateWithValueAndRanges(serializationContext, predicateWithValueAndRangesProto);
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PPredicateWithValueAndRanges, PredicateWithValueAndRanges> {
        @Nonnull
        @Override
        public Class<PPredicateWithValueAndRanges> getProtoMessageClass() {
            return PPredicateWithValueAndRanges.class;
        }

        @Nonnull
        @Override
        public PredicateWithValueAndRanges fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                     @Nonnull final PPredicateWithValueAndRanges predicateWithValueAndRangesProto) {
            return PredicateWithValueAndRanges.fromProto(serializationContext, predicateWithValueAndRangesProto);
        }
    }
}
