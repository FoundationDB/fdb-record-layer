/*
 * ComposedBitmapIndexQueryPlan.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2020 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.bitmap;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.planprotos.PRecordQueryPlan;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.query.plan.AvailableFields;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.explain.NodeInfo;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraph;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.values.QueriedValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.apple.foundationdb.record.query.plan.plans.QueryResult;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryCoveringIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlanWithNoChildren;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A query plan implementing a bit-wise merge of two or more covering index scans of {@code BITMAP_VALUE} indexes.
 */
@API(API.Status.EXPERIMENTAL)
public class ComposedBitmapIndexQueryPlan implements RecordQueryPlanWithNoChildren {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Composed-Bitmap-Index-Query-Plan");

    @Nonnull
    // NOTE: These aren't children in the sense of RecordQueryPlanWithChildren
    // for the same reason as RecordQueryCoveringIndexPlan isn't RecordQueryPlanWithChild.
    private final List<RecordQueryCoveringIndexPlan> indexPlans;
    @Nonnull
    private final ComposerBase composer;

    ComposedBitmapIndexQueryPlan(@Nonnull List<RecordQueryCoveringIndexPlan> indexPlans, @Nonnull ComposerBase composer) {
        this.indexPlans = indexPlans;
        this.composer = composer;
    }

    @Nonnull
    public List<RecordQueryCoveringIndexPlan> getIndexPlans() {
        return indexPlans;
    }

    @Nonnull
    public ComposerBase getComposer() {
        return composer;
    }

    @Nonnull
    @Override
    public <M extends Message> RecordCursor<QueryResult> executePlan(@Nonnull final FDBRecordStoreBase<M> store,
                                                                     @Nonnull final EvaluationContext context,
                                                                     @Nullable final byte[] continuation,
                                                                     @Nonnull final ExecuteProperties executeProperties) {
        final ExecuteProperties scanExecuteProperties = executeProperties.getSkip() > 0 ? executeProperties.clearSkipAndAdjustLimit() : executeProperties;
        final List<Function<byte[], RecordCursor<IndexEntry>>> cursorFunctions = indexPlans.stream()
                .map(RecordQueryCoveringIndexPlan::getIndexPlan)
                .map(scan -> (Function<byte[], RecordCursor<IndexEntry>>) childContinuation -> scan.executeEntries(store, context, childContinuation, scanExecuteProperties))
                .collect(Collectors.toList());
        return ComposedBitmapIndexCursor.create(cursorFunctions, composer, continuation, store.getTimer())
                // Composers can return null bitmaps when empty, which is then left out of the result set.
                .filter(indexEntry -> indexEntry.getValue().get(0) != null)
                .map(indexPlans.get(0).indexEntryToQueriedRecord(store))
                .map(QueryResult::fromQueriedRecord);
    }

    @Override
    public boolean isReverse() {
        return false;
    }

    @Override
    public boolean hasRecordScan() {
        return false;
    }

    @Override
    public boolean hasFullRecordScan() {
        return false;
    }

    @Override
    public boolean hasIndexScan(@Nonnull String indexName) {
        return indexPlans.stream().anyMatch(p -> p.hasIndexScan(indexName));
    }

    @Nonnull
    @Override
    public Set<String> getUsedIndexes() {
        return indexPlans.stream().map(RecordQueryPlan::getUsedIndexes).flatMap(Set::stream).collect(Collectors.toSet());
    }

    @Override
    public boolean hasLoadBykeys() {
        return false;
    }

    @Nonnull
    @Override
    public AvailableFields getAvailableFields() {
        return AvailableFields.ALL_FIELDS;
    }

    @Override
    public void logPlanStructure(StoreTimer timer) {
        timer.increment(FDBStoreTimer.Counts.PLAN_COMPOSED_BITMAP_INDEX);
        for (RecordQueryPlan indexPlan : indexPlans) {
            indexPlan.logPlanStructure(timer);
        }
    }

    @Override
    public int getComplexity() {
        int complexity = 1;
        for (RecordQueryPlan child : indexPlans) {
            complexity += child.getComplexity();
        }
        return complexity;
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        switch (mode.getKind()) {
            case LEGACY:
                return PlanHashable.planHash(mode, indexPlans) + composer.planHash(mode);
            case FOR_CONTINUATION:
                return PlanHashable.objectsPlanHash(mode, BASE_HASH, indexPlans, composer);
            default:
                throw new UnsupportedOperationException("Hash kind " + mode.getKind() + " is not supported");
        }
    }

    @Nonnull
    @Override
    public PlannerGraph rewritePlannerGraph(@Nonnull List<? extends PlannerGraph> childGraphs) {
        return PlannerGraph.fromNodeAndChildGraphs(
                new PlannerGraph.OperatorNodeWithInfo(this, NodeInfo.COMPOSED_BITMAP_OPERATOR),
                childGraphs);
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedTo() {
        return indexPlans.stream().map(RecordQueryPlan::getCorrelatedTo).flatMap(Set::stream).collect(Collectors.toSet());
    }

    @Nonnull
    @Override
    public ComposedBitmapIndexQueryPlan translateCorrelations(@Nonnull final TranslationMap translationMap,
                                                              @Nonnull final List<? extends Quantifier> translatedQuantifiers) {
        final var translatedIndexPlansBuilder = ImmutableList.<RecordQueryCoveringIndexPlan>builder();
        boolean allAreSame = true;
        for (final var indexPlan : indexPlans) {
            final var translatedIndexPlan = indexPlan.translateCorrelations(translationMap, translatedQuantifiers);
            if (translatedIndexPlan != indexPlan) {
                allAreSame = false;
            }
            translatedIndexPlansBuilder.add(translatedIndexPlan);
        }

        if (!allAreSame) {
            return new ComposedBitmapIndexQueryPlan(translatedIndexPlansBuilder.build(), composer);
        }
        return this;
    }

    @Nonnull
    @Override
    public Value getResultValue() {
        return new QueriedValue();
    }
    
    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public boolean equalsWithoutChildren(@Nonnull RelationalExpression other, @Nonnull AliasMap equivalences) {
        if (this == other) {
            return true;
        }
        if (getClass() != other.getClass()) {
            return false;
        }
        ComposedBitmapIndexQueryPlan that = (ComposedBitmapIndexQueryPlan) other;
        if (!composer.equals(that.composer)) {
            return false;
        }
        List<RecordQueryCoveringIndexPlan> otherIndexPlans = that.indexPlans;
        if (indexPlans.size() != otherIndexPlans.size()) {
            return false;
        }
        for (int i = 0; i < indexPlans.size(); i++) {
            if (!indexPlans.get(i).equalsWithoutChildren(otherIndexPlans.get(i), equivalences)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCodeWithoutChildren() {
        return Objects.hash(indexPlans, composer);
    }

    @Override
    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    public boolean equals(Object other) {
        return structuralEquals(other);
    }

    @Override
    public int hashCode() {
        return structuralHashCode();
    }

    @Override
    public String toString() {
        return composer.toString(indexPlans);
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

    /**
     * Plan extension of {@link ComposedBitmapIndexCursor.Composer}.
     */
    public abstract static class ComposerBase implements ComposedBitmapIndexCursor.Composer, PlanHashable {
        @Nonnull
        abstract String toString(@Nullable List<?> sources);

        @Override
        public String toString() {
            return toString(null);
        }
    }

    static class IndexComposer extends ComposerBase {
        private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Index-Composer");

        private final int position;

        IndexComposer(int position) {
            this.position = position;
        }

        @Nonnull
        @Override
        String toString(@Nullable List<?> sources) {
            if (sources == null) {
                return "[" + position + "]";
            } else {
                return sources.get(position).toString();
            }
        }

        @Nullable
        @Override
        public byte[] compose(@Nonnull List<byte[]> bitmaps, int size) {
            return bitmaps.get(position);
        }

        @Override
        public int planHash(@Nonnull final PlanHashMode mode) {
            switch (mode.getKind()) {
                case LEGACY:
                    return position;
                case FOR_CONTINUATION:
                    return PlanHashable.objectsPlanHash(mode, BASE_HASH, position);
                default:
                    throw new UnsupportedOperationException("Hash kind " + mode.getKind() + " is not supported");
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
            IndexComposer that = (IndexComposer) o;
            return position == that.position;
        }

        @Override
        public int hashCode() {
            return Objects.hash(position);
        }
    }

    abstract static class OperatorComposer extends ComposerBase {
        private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Operator-Composer");

        @Nonnull
        private final List<ComposerBase> children;

        OperatorComposer(@Nonnull List<ComposerBase> children) {
            this.children = children;
        }

        @Nonnull
        abstract String operator();

        @Nonnull
        @Override
        String toString(@Nullable List<?> sources) {
            return children.stream().map(child -> child.toString(sources)).collect(Collectors.joining(" " + operator() + " "));
        }

        @Nullable
        @Override
        public byte[] compose(@Nonnull List<byte[]> bitmaps, int size) {
            final List<byte[]> operands = new ArrayList<>(children.size());
            for (ComposerBase child : children) {
                operands.add(child.compose(bitmaps, size));
            }
            return operate(operands, new byte[size]);
        }

        @Nullable
        abstract byte[] operate(@Nonnull List<byte[]> operands, @Nonnull byte[] result);

        @Override
        public int planHash(@Nonnull final PlanHashMode mode) {
            switch (mode.getKind()) {
                case LEGACY:
                    return PlanHashable.planHash(mode, children) + operator().hashCode();
                case FOR_CONTINUATION:
                    return  PlanHashable.objectsPlanHash(mode, BASE_HASH, children, operator());
                default:
                    throw new UnsupportedOperationException("Hash kind " + mode.getKind() + " is not supported");
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
            OperatorComposer that = (OperatorComposer) o;
            return children.equals(that.children) &&
                    operator().equals(that.operator());
        }

        @Override
        public int hashCode() {
            return Objects.hash(children) + operator().hashCode();
        }
    }

    // The specific binary operators are mostly the same, except that AND bails out early on empty and they use a different
    // bit operator in the inner loop. There could be an abstract method for that operation, but it would be invoked
    // inside the loop, which seems to less the chances for the whole being compiled well.

    static class AndComposer extends OperatorComposer {
        public AndComposer(@Nonnull List<ComposerBase> children) {
            super(children);
        }

        @Nonnull
        @Override
        String operator() {
            return "BITAND";
        }

        @Nullable
        @Override
        byte[] operate(@Nonnull List<byte[]> operands, @Nonnull byte[] result) {
            boolean first = true;
            boolean empty = true;
            for (final byte[] operand : operands) {
                if (operand == null) {
                    return null;
                }
                if (first) {
                    System.arraycopy(operand, 0, result, 0, result.length);
                    empty = first = false;
                } else {
                    empty = true;
                    for (int j = 0; j < result.length; j++) {
                        final byte b = (byte) (result[j] & operand[j]);
                        result[j] = b;
                        if (empty && b != 0) {
                            empty = false;
                        }
                    }
                }
            }
            return empty ? null : result;
        }
    }

    static class OrComposer extends OperatorComposer {
        public OrComposer(@Nonnull List<ComposerBase> children) {
            super(children);
        }

        @Nonnull
        @Override
        String operator() {
            return "BITOR";
        }

        @Nullable
        @Override
        byte[] operate(@Nonnull List<byte[]> operands, @Nonnull byte[] result) {
            boolean first = true;
            boolean empty = true;
            for (final byte[] operand : operands) {
                if (operand == null) {
                    continue;
                }
                if (first) {
                    System.arraycopy(operand, 0, result, 0, result.length);
                    empty = first = false;
                } else {
                    empty = true;
                    for (int j = 0; j < result.length; j++) {
                        final byte b = (byte) (result[j] | operand[j]);
                        result[j] = b;
                        if (empty && b != 0) {
                            empty = false;
                        }
                    }
                }
            }
            return empty ? null : result;
        }
    }

    static class XorComposer extends OperatorComposer {
        public XorComposer(@Nonnull List<ComposerBase> children) {
            super(children);
        }

        @Nonnull
        @Override
        String operator() {
            return "BITXOR";
        }

        @Nullable
        @Override
        byte[] operate(@Nonnull List<byte[]> operands, @Nonnull byte[] result) {
            boolean first = true;
            boolean empty = true;
            for (final byte[] operand : operands) {
                if (operand == null) {
                    continue;
                }
                if (first) {
                    System.arraycopy(operand, 0, result, 0, result.length);
                    empty = first = false;
                } else {
                    empty = true;
                    for (int j = 0; j < result.length; j++) {
                        final byte b = (byte) (result[j] ^ operand[j]);
                        result[j] = b;
                        if (empty && b != 0) {
                            empty = false;
                        }
                    }
                }
            }
            return empty ? null : result;
        }
    }

    static class NotComposer extends ComposerBase {
        private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Not-Composer");

        @Nonnull
        private final ComposerBase child;

        NotComposer(@Nonnull ComposerBase child) {
            this.child = child;
        }

        @Nonnull
        @Override
        String toString(@Nullable List<?> sources) {
            return "BITNOT " + child.toString(sources);
        }

        @Nullable
        @Override
        public byte[] compose(@Nonnull List<byte[]> bitmaps, int size) {
            final byte[] operand = child.compose(bitmaps, size);
            final byte[] result = new byte[size];
            if (operand == null) {
                Arrays.fill(result, (byte)0xFF);
            } else {
                for (int i = 0; i < result.length; i++) {
                    result[i] = (byte)~operand[i];
                }
            }
            return result;
        }

        @Override
        public int planHash(@Nonnull final PlanHashMode mode) {
            switch (mode.getKind()) {
                case LEGACY:
                    return child.planHash(mode);
                case FOR_CONTINUATION:
                    return PlanHashable.objectsPlanHash(mode, BASE_HASH, child);
                default:
                    throw new UnsupportedOperationException("Hash kind " + mode.getKind() + " is not supported");
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
            NotComposer that = (NotComposer) o;
            return child.equals(that.child);
        }

        @Override
        public int hashCode() {
            return child.hashCode();
        }
    }

}
