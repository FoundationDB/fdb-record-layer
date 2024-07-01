/*
 * RecordQuerySelectorPlan.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2022 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorContinuation;
import com.apple.foundationdb.record.RecordCursorProto;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.RecordCursorVisitor;
import com.apple.foundationdb.record.planprotos.PRecordQueryPlan;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.query.plan.PlanStringRepresentation;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Quantifiers;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.explain.Attribute;
import com.apple.foundationdb.record.query.plan.cascades.explain.NodeInfo;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraph;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.apple.foundationdb.tuple.ByteArrayUtil2;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.ZeroCopyByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * A {@link RecordQueryChooserPlanBase} that selects one of its children to be executed.
 * This plan can be used to allow the planner to have a few optional plans to be executed where the decision on which
 * one actually gets run is deferred to execution time.
 * The selector can be used with a given lambda that selects among the plans, and as a convenience, a relative probability
 * selector is also provided. The relative probability selector takes a list of probabilities (each associated with a plan)
 * and selects a plan at random with relative probability as given by the probability associated with the plan.
 */
public class RecordQuerySelectorPlan extends RecordQueryChooserPlanBase {
    public static final Logger LOGGER = LoggerFactory.getLogger(RecordQuerySelectorPlan.class);
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Record-Query-Selector-Plan");

    @Nonnull
    private final PlanSelector planSelector;

    private RecordQuerySelectorPlan(@Nonnull final List<Quantifier.Physical> quantifiers, @Nonnull final PlanSelector planSelector) {
        super(quantifiers);
        this.planSelector = planSelector;
    }

    /**
     * Factory method that takes in a list of relative probabilities and created a selection policy based on them.
     *
     * @param children the list of sub plans
     * @param relativePlanProbabilities a list (of the same length as the children) that determines the relative
     * probability for selecting the plan. Sum of all the probabilities must be 100.
     * @return newly created plan
     */
    public static RecordQuerySelectorPlan from(@Nonnull List<? extends RecordQueryPlan> children, @Nonnull final List<Integer> relativePlanProbabilities) {
        if (children.size() != relativePlanProbabilities.size()) {
            throw new RecordCoreArgumentException("Number of plans and number of relative probabilities should be the same");
        }
        return RecordQuerySelectorPlan.from(children, new RelativeProbabilityPlanSelector(relativePlanProbabilities));
    }

    /**
     * Factory method that takes in a {@link PlanSelector}.
     *
     * @param children the list of sub-plans
     * @param planSelector the plan selector to use
     * @return newly created plan
     */
    public static RecordQuerySelectorPlan from(@Nonnull List<? extends RecordQueryPlan> children, @Nonnull final PlanSelector planSelector) {
        if (children.isEmpty()) {
            throw new RecordCoreArgumentException("Selector plan should have at least one plan");
        }
        final ImmutableList.Builder<Reference> childRefsBuilder = ImmutableList.builder();
        for (RecordQueryPlan child : children) {
            childRefsBuilder.add(Reference.of(child));
        }
        return new RecordQuerySelectorPlan(Quantifiers.fromPlans(childRefsBuilder.build()), planSelector);
    }

    /**
     * Select a plan and execute it.
     * The selection of the plan gives priority to the continuation: If the given continuation has a selected plan
     * already,
     * that same plan continues to be executed (this would ensure proper plan continuation). In case the given
     * continuation
     * is empty, the planSelector would be used to select the plan.
     *
     * @param store record store from which to fetch records
     * @param context evaluation context containing parameter bindings
     * @param continuation continuation from a previous execution of this same plan
     * @param executeProperties limits on execution
     * @param <M> the type of records in the store
     *
     * @return {@link RecordCursor} that iterates through the results of the execution
     */
    @Nonnull
    @Override
    @SuppressWarnings("PMD.CloseResource")
    public <M extends Message> RecordCursor<QueryResult> executePlan(@Nonnull final FDBRecordStoreBase<M> store,
                                                                     @Nonnull final EvaluationContext context,
                                                                     @Nullable final byte[] continuation,
                                                                     @Nonnull final ExecuteProperties executeProperties) {
        // The continuation should dictate which plan to select. In case the execution does not have a continuation,
        // select a plan from the available using the selection criteria.
        // Note that we are not doing any special validation of the continuation. The assumption is that the entire
        // continuation was verified upfront and that the SelectorPlan being executed is the same as the one that generated
        // the continuation in the first place.
        SelectorContinuation selectorContinuation = new SelectorContinuation(continuation);
        int selectedPlanIndex = selectPlanIndex(selectorContinuation);
        RecordQueryPlan selectedPlan = getChild(selectedPlanIndex);
        RecordCursor<QueryResult> innerCursor = selectedPlan.executePlan(store, context, selectorContinuation.getInnerContinuation(), executeProperties);
        // Create a wrapper cursor over the inner one, to encode the continuation values
        return new SelectorPlanCursor(selectedPlanIndex, innerCursor, store.getTimer());
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        return PlanHashable.objectsPlanHash(mode, BASE_HASH, getChildren(), isReverse(), planSelector);
    }

    @Override
    public String toString() {
        return PlanStringRepresentation.toString(this);
    }

    @Override
    public void logPlanStructure(final StoreTimer timer) {
        timer.increment(FDBStoreTimer.Counts.PLAN_SELECTOR);
        for (final Quantifier.Physical quantifier : quantifiers) {
            quantifier.getRangesOverPlan().logPlanStructure(timer);
        }
    }

    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public boolean equalsWithoutChildren(@Nonnull final RelationalExpression otherExpression, @Nonnull final AliasMap equivalences) {
        if (this == otherExpression) {
            return true;
        }
        if (getClass() != otherExpression.getClass()) {
            return false;
        }
        final RecordQuerySelectorPlan other = (RecordQuerySelectorPlan)otherExpression;
        return ((isReverse() == other.isReverse()) && planSelector.equals(other.planSelector));
    }

    @Override
    public int hashCodeWithoutChildren() {
        return Objects.hash(isReverse(), planSelector);
    }

    @Nonnull
    @Override
    public PlannerGraph rewritePlannerGraph(@Nonnull final List<? extends PlannerGraph> childGraphs) {
        return PlannerGraph.fromNodeAndChildGraphs(
                new PlannerGraph.OperatorNodeWithInfo(this,
                        NodeInfo.SELECTOR_OPERATOR,
                        List.of("SELECT BY {{planSelector}}"),
                        Map.of("planSelector", Attribute.gml(planSelector.toString()))),
                childGraphs);
    }

    @Nonnull
    @Override
    public RecordQuerySelectorPlan translateCorrelations(@Nonnull final TranslationMap translationMap, @Nonnull final List<? extends Quantifier> translatedQuantifiers) {
        return new RecordQuerySelectorPlan(
                Quantifiers.narrow(Quantifier.Physical.class, translatedQuantifiers), planSelector);
    }

    private int selectPlanIndex(final SelectorContinuation continuation) {
        if (!continuation.isEmpty()) {
            // Continuation overrides the lambda selection.
            return (int)continuation.getSelectedPlanIndex();
        } else {
            // Select a plan using the plan selector
            return planSelector.selectPlan(getChildren());
        }
    }

    @Nonnull
    @Override
    public Message toProto(@Nonnull final PlanSerializationContext serializationContext) {
        throw new RecordCoreException("serialization of this plan is not supported");
    }

    @Nonnull
    @Override
    public PRecordQueryPlan toRecordQueryPlanProto(@Nonnull final PlanSerializationContext serializationContext) {
        throw new RecordCoreException("serialization of this plan is not supported");
    }

    private static class SelectorPlanCursor implements RecordCursor<QueryResult> {
        // The index of the selected plan within the parent selector plan
        private final long selectedPlanIndex;
        // Inner cursor to provide record inflow
        @Nonnull
        private final RecordCursor<QueryResult> inner;
        @Nullable FDBStoreTimer timer;

        public SelectorPlanCursor(final long selectedPlanIndex, @Nonnull final RecordCursor<QueryResult> inner, @Nullable FDBStoreTimer timer) {
            this.inner = inner;
            this.selectedPlanIndex = selectedPlanIndex;
            this.timer = timer;
        }

        @Nonnull
        @Override
        public CompletableFuture<RecordCursorResult<QueryResult>> onNext() {
            return inner.onNext().thenApply(this::calculateCursorResult);
        }

        @Override
        public void close() {
            inner.close();
        }

        @Override
        public boolean isClosed() {
            return inner.isClosed();
        }

        @Nonnull
        @Override
        public Executor getExecutor() {
            return inner.getExecutor();
        }

        @Override
        public boolean accept(@Nonnull RecordCursorVisitor visitor) {
            if (visitor.visitEnter(this)) {
                inner.accept(visitor);
            }
            return visitor.visitLeave(this);
        }

        private RecordCursorResult<QueryResult> calculateCursorResult(final RecordCursorResult<QueryResult> innerResult) {
            final long startTime = System.nanoTime();
            if (innerResult.hasNext()) {
                SelectorContinuation continuation = new SelectorContinuation(selectedPlanIndex, innerResult.getContinuation().toBytes(), false);
                logTimer(startTime);
                return RecordCursorResult.withNextValue(innerResult.get(), continuation);
            } else {
                // Inner is done - return a no-next result with wrapped continuation and the same no-next reason and isEnd.
                SelectorContinuation continuation = new SelectorContinuation(selectedPlanIndex, innerResult.getContinuation().toBytes(), innerResult.getContinuation().isEnd());
                logTimer(startTime);
                return RecordCursorResult.withoutNextValue(continuation, innerResult.getNoNextReason());
            }
        }

        private void logTimer(final long startTime) {
            if (timer != null) {
                timer.record(FDBStoreTimer.Events.QUERY_SELECTOR, System.nanoTime() - startTime);
            }
        }
    }

    private static class SelectorContinuation implements RecordCursorContinuation {
        private long selectedPlanIndex;
        @Nullable
        private ByteString innerContinuation = null;
        private boolean isEnd;
        @Nullable
        private RecordCursorProto.SelectorPlanContinuation cachedProto;

        public SelectorContinuation(byte[] rawBytes) {
            try {
                if (rawBytes != null) {
                    RecordCursorProto.SelectorPlanContinuation continuation = RecordCursorProto.SelectorPlanContinuation.parseFrom(rawBytes);
                    if (continuation.hasSelectedPlan()) {
                        selectedPlanIndex = continuation.getSelectedPlan();
                    }
                    if (continuation.hasInnerContinuation()) {
                        innerContinuation = continuation.getInnerContinuation();
                    }
                }
            } catch (InvalidProtocolBufferException ex) {
                throw new RecordCoreException("error parsing continuation", ex)
                        .addLogInfo("raw_bytes", ByteArrayUtil2.loggable(rawBytes));
            }
        }

        public SelectorContinuation(long selectedPlanIndex, @Nullable byte[] innerContinuation, final boolean isEnd) {
            this.selectedPlanIndex = selectedPlanIndex;
            this.isEnd = isEnd;
            if (innerContinuation != null) {
                this.innerContinuation = ZeroCopyByteString.wrap(innerContinuation);
            } else {
                this.innerContinuation = null;
            }
        }

        private RecordCursorProto.SelectorPlanContinuation toProto() {
            if (cachedProto == null) {
                cachedProto = RecordCursorProto.SelectorPlanContinuation.newBuilder()
                        .setSelectedPlan(selectedPlanIndex)
                        .setInnerContinuation(innerContinuation)
                        .build();
            }
            return cachedProto;
        }

        @Nonnull
        @Override
        public ByteString toByteString() {
            if (isEnd()) {
                return ByteString.EMPTY;
            } else {
                return toProto().toByteString();
            }
        }

        @Nullable
        @Override
        public byte[] toBytes() {
            if (isEnd()) {
                return null;
            } else {
                return toProto().toByteArray();
            }
        }

        @Override
        public boolean isEnd() {
            return isEnd;
        }

        public boolean isEmpty() {
            return innerContinuation == null;
        }

        public long getSelectedPlanIndex() {
            return selectedPlanIndex;
        }

        @Nullable
        public byte[] getInnerContinuation() {
            return (innerContinuation == null) ? null : innerContinuation.toByteArray();
        }
    }
}
