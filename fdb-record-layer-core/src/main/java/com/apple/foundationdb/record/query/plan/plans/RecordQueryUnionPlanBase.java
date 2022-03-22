/*
 * RecordQueryUnionPlanBase.java
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

package com.apple.foundationdb.record.query.plan.plans;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.AvailableFields;
import com.apple.foundationdb.record.query.plan.temp.AliasMap;
import com.apple.foundationdb.record.query.plan.temp.GroupExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.Quantifier;
import com.apple.foundationdb.record.query.plan.temp.Quantifiers;
import com.apple.foundationdb.record.query.plan.temp.RelationalExpression;
import com.apple.foundationdb.record.query.predicates.MergeValue;
import com.apple.foundationdb.record.query.predicates.Value;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Common base class for plans that perform stream union operations.
 */
@API(API.Status.INTERNAL)
public abstract class RecordQueryUnionPlanBase implements RecordQueryPlanWithChildren, RecordQuerySetPlan {
    public static final Logger LOGGER = LoggerFactory.getLogger(RecordQueryUnionPlanBase.class);

    protected static final String UNION = "∪";    // U+222A
    /* The current implementations of equals() and hashCode() treat RecordQueryUnionPlan as if it were isomorphic under
     * a reordering of its children. In particular, all of the tests assume that a RecordQueryUnionPlan with its children
     * reordered is identical. This is accurate in the current implementation (except that the continuation might no longer
     * be valid); if this ever changes, equals() and hashCode() must be updated.
     */
    @Nonnull
    private final List<Quantifier.Physical> quantifiers;
    private final boolean reverse;
    @Nonnull
    private final Value resultValue;

    protected RecordQueryUnionPlanBase(@Nonnull RecordQueryPlan left, @Nonnull RecordQueryPlan right, boolean reverse) {
        this(Quantifiers.fromPlans(ImmutableList.of(GroupExpressionRef.of(left), GroupExpressionRef.of(right))), reverse);
    }

    protected RecordQueryUnionPlanBase(@Nonnull final List<Quantifier.Physical> quantifiers,
                                       final boolean reverse) {
        Verify.verify(!quantifiers.isEmpty());
        this.quantifiers = ImmutableList.copyOf(quantifiers);
        this.reverse = reverse;
        this.resultValue = MergeValue.pivotAndMergeValues(quantifiers);
    }

    @Nonnull
    abstract <M extends Message> RecordCursor<FDBQueriedRecord<M>> createUnionCursor(@Nonnull FDBRecordStoreBase<M> store,
                                                                                     @Nonnull List<Function<byte[], RecordCursor<FDBQueriedRecord<M>>>> childCursorFunctions,
                                                                                     @Nullable byte[] continuation);

    @Nonnull
    @Override
    public <M extends Message> RecordCursor<QueryResult> executePlan(@Nonnull final FDBRecordStoreBase<M> store,
                                                                     @Nonnull final EvaluationContext context,
                                                                     @Nullable final byte[] continuation,
                                                                     @Nonnull final ExecuteProperties executeProperties) {
        final ExecuteProperties childExecuteProperties;
        // Can pass the limit down to all sides, since that is the most we'll take total.
        if (executeProperties.getSkip() > 0) {
            childExecuteProperties = executeProperties.clearSkipAndAdjustLimit();
        } else {
            childExecuteProperties = executeProperties;
        }
        final List<Function<byte[], RecordCursor<FDBQueriedRecord<M>>>> childCursorFunctions = getChildStream()
                .map(childPlan -> (Function<byte[], RecordCursor<FDBQueriedRecord<M>>>)
                        ((byte[] childContinuation) -> childPlan
                                .executePlan(store, context, childContinuation, childExecuteProperties)
                                .map(result -> result.getQueriedRecord(0))))
                .collect(Collectors.toList());
        return createUnionCursor(store, childCursorFunctions, continuation).skipThenLimit(executeProperties.getSkip(), executeProperties.getReturnedRowLimit())
                .map(QueryResult::of);
    }

    @Override
    public boolean isReverse() {
        return reverse;
    }

    @Nonnull
    private Stream<RecordQueryPlan> getChildStream() {
        return quantifiers.stream().map(Quantifier.Physical::getRangesOverPlan);
    }

    @Override
    @Nonnull
    public List<RecordQueryPlan> getChildren() {
        return getChildStream().collect(Collectors.toList());
    }

    @Nonnull
    @Override
    public List<? extends Quantifier> getQuantifiers() {
        return quantifiers;
    }

    @Nonnull
    @Override
    public AvailableFields getAvailableFields() {
        return AvailableFields.intersection(quantifiers.stream()
                .map(child -> child.getRangesOverPlan().getAvailableFields())
                .collect(Collectors.toList()));
    }

    @Nonnull
    @Override
    public Value getResultValue() {
        return resultValue;
    }

    @Override
    public boolean equalsWithoutChildren(@Nonnull final RelationalExpression otherExpression,
                                         @Nonnull final AliasMap equivalencesMap) {
        if (this == otherExpression) {
            return true;
        }
        if (getClass() != otherExpression.getClass()) {
            return false;
        }
        return reverse == ((RecordQueryUnionPlanBase)otherExpression).reverse;
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @Override
    public boolean equals(final Object other) {
        return structuralEquals(other);
    }

    @Override
    public int hashCode() {
        return structuralHashCode();
    }

    @Override
    public int hashCodeWithoutChildren() {
        return Objects.hash(reverse);
    }

    /**
     * Base implementation of {@link #planHash}.
     * This implementation makes each concrete subclass implement its own version of {@link #planHash} so that they are
     * guided to add their own class modifier (See {@link com.apple.foundationdb.record.ObjectPlanHash ObjectPlanHash}).
     * This implementation is meant to give subclasses common functionality for their own implementation.
     * @param hashKind the plan hash kind to use
     * @param baseHash the subclass' base hash (concrete identifier)
     * @param hashables the rest of the subclass' hashable parameters (if any)
     * @return the plan hash value calculated
     */
    protected int basePlanHash(@Nonnull final PlanHashKind hashKind, ObjectPlanHash baseHash, Object... hashables) {
        switch (hashKind) {
            case LEGACY:
                return PlanHashable.planHash(hashKind, getQueryPlanChildren()) + (reverse ? 1 : 0);
            case FOR_CONTINUATION:
            case STRUCTURAL_WITHOUT_LITERALS:
                return PlanHashable.objectsPlanHash(hashKind, baseHash, getQueryPlanChildren(), reverse, hashables);
            default:
                throw new UnsupportedOperationException("Hash kind " + hashKind.name() + " is not supported");
        }
    }

    @Nonnull
    abstract String getDelimiter();

    @Nonnull
    @Override
    public String toString() {
        return getChildStream().map(RecordQueryPlan::toString).collect(Collectors.joining(getDelimiter()));
    }

    @Nonnull
    abstract StoreTimer.Count getPlanCount();

    @Override
    public void logPlanStructure(StoreTimer timer) {
        timer.increment(getPlanCount());
        for (Quantifier.Physical quantifier : quantifiers) {
            quantifier.getRangesOverPlan().logPlanStructure(timer);
        }
    }

    @Override
    public int getComplexity() {
        int complexity = 1;
        for (Quantifier.Physical quantifier : quantifiers) {
            complexity += quantifier.getRangesOverPlan().getComplexity();
        }
        return complexity;
    }

    @Override
    public int getRelationalChildCount() {
        return quantifiers.size();
    }

    @Override
    public boolean isStrictlySorted() {
        return getChildren().stream().allMatch(RecordQueryPlan::isStrictlySorted);
    }

    @Override
    public QueryPlan<FDBQueriedRecord<Message>> strictlySorted() {
        return withChildrenReferences(getChildren().stream().map(p -> GroupExpressionRef.of((RecordQueryPlan)p.strictlySorted())).collect(Collectors.toList()));
    }

    public static boolean isReversed(@Nonnull List<Quantifier.Physical> quantifiers) {
        return quantifiers
                .stream()
                .map(Quantifier.Physical::getRangesOver)
                .flatMap(reference -> reference.getMembers().stream())
                .map(expression -> {
                    Verify.verify(expression instanceof RecordQueryPlan);
                    return (RecordQueryPlan)expression;
                })
                .map(QueryPlan::isReverse)
                .findAny()
                .orElseThrow(() -> new RecordCoreException("unable to determine reversed-ness"));
    }
}
