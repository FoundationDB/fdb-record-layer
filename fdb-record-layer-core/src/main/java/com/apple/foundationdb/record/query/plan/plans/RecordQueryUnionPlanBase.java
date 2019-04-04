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
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.temp.ExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.PlannerExpression;
import com.apple.foundationdb.record.query.plan.temp.SingleExpressionRef;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.google.protobuf.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@API(API.Status.INTERNAL)
abstract class RecordQueryUnionPlanBase implements RecordQueryPlanWithChildren {
    public static final Logger LOGGER = LoggerFactory.getLogger(RecordQueryUnionPlanBase.class);

    protected static final String UNION = "∪";    // U+222A
    /* The current implementations of equals() and hashCode() treat RecordQueryUnionPlan as if it were isomorphic under
     * a reordering of its children. In particular, all of the tests assume that a RecordQueryUnionPlan with its children
     * reordered is identical. This is accurate in the current implementation (except that the continuation might no longer
     * be valid); if this ever changes, equals() and hashCode() must be updated.
     */
    @Nonnull
    private final List<ExpressionRef<RecordQueryPlan>> children;
    private final boolean reverse;

    public RecordQueryUnionPlanBase(@Nonnull RecordQueryPlan left, @Nonnull RecordQueryPlan right, boolean reverse) {
        this(ImmutableList.of(left, right), reverse);
    }

    public RecordQueryUnionPlanBase(@Nonnull List<RecordQueryPlan> children, boolean reverse) {
        final ImmutableList.Builder<ExpressionRef<RecordQueryPlan>> childrenBuilder = ImmutableList.builder();
        for (RecordQueryPlan child : children) {
            ExpressionRef<RecordQueryPlan> childRef = SingleExpressionRef.of(child);
            childrenBuilder.add(childRef);
        }
        this.children = childrenBuilder.build();
        this.reverse = reverse;
    }

    @Nonnull
    abstract <M extends Message> RecordCursor<FDBQueriedRecord<M>> createUnionCursor(@Nonnull FDBRecordStoreBase<M> store,
                                                                                     @Nonnull List<Function<byte[], RecordCursor<FDBQueriedRecord<M>>>> childCursorFunctions,
                                                                                     @Nullable byte[] continuation);

    @Nonnull
    @Override
    @SuppressWarnings("squid:S2095") // SonarQube doesn't realize that the union cursor is wrapped and returned
    public <M extends Message> RecordCursor<FDBQueriedRecord<M>> execute(@Nonnull FDBRecordStoreBase<M> store,
                                                                         @Nonnull EvaluationContext context,
                                                                         @Nullable byte[] continuation,
                                                                         @Nonnull ExecuteProperties executeProperties) {
        final ExecuteProperties childExecuteProperties;
        // Can pass the limit down to all sides, since that is the most we'll take total.
        if (executeProperties.getSkip() > 0) {
            childExecuteProperties = executeProperties.clearSkipAndAdjustLimit();
        } else {
            childExecuteProperties = executeProperties;
        }
        final List<Function<byte[], RecordCursor<FDBQueriedRecord<M>>>> childCursorFunctions = getChildStream()
                .map(childPlan -> (Function<byte[], RecordCursor<FDBQueriedRecord<M>>>)
                        ((byte[] childContinuation) -> childPlan.execute(store, context, childContinuation, childExecuteProperties)))
                .collect(Collectors.toList());
        return createUnionCursor(store, childCursorFunctions, continuation).skipThenLimit(executeProperties.getSkip(), executeProperties.getReturnedRowLimit());
    }

    @Override
    public boolean isReverse() {
        return reverse;
    }

    @Override
    public boolean hasRecordScan() {
        return getChildStream().anyMatch(RecordQueryPlan::hasRecordScan);
    }

    @Override
    public boolean hasFullRecordScan() {
        return getChildStream().anyMatch(RecordQueryPlan::hasFullRecordScan);
    }

    @Override
    public boolean hasIndexScan(@Nonnull String indexName) {
        return getChildStream().anyMatch(child -> child.hasIndexScan(indexName));
    }

    @Nonnull
    private Stream<RecordQueryPlan> getChildStream() {
        return children.stream().map(ExpressionRef::get);
    }

    @Override
    @Nonnull
    public List<RecordQueryPlan> getChildren() {
        return getChildStream().collect(Collectors.toList());
    }

    @Nonnull
    @Override
    public Set<String> getUsedIndexes() {
        HashSet<String> usedIndexes = new HashSet<>();
        for (ExpressionRef<RecordQueryPlan> child : children) {
            usedIndexes.addAll(child.get().getUsedIndexes());
        }
        return usedIndexes;
    }

    @Nonnull
    @Override
    @API(API.Status.EXPERIMENTAL)
    public Iterator<? extends ExpressionRef<? extends PlannerExpression>> getPlannerExpressionChildren() {
        return children.iterator();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RecordQueryUnionPlanBase that = (RecordQueryUnionPlanBase) o;
        return reverse == that.reverse &&
               Objects.equals(Sets.newHashSet(getQueryPlanChildren()), Sets.newHashSet(that.getQueryPlanChildren()));  // isomorphic under re-ordering of children
    }

    @Override
    public int hashCode() {
        return Objects.hash(Sets.newHashSet(getQueryPlanChildren()), reverse); // isomorphic under re-ordering of children
    }

    @Override
    public int planHash() {
        return PlanHashable.planHash(getQueryPlanChildren()) + (reverse ? 1 : 0);
    }

    @Nonnull
    abstract String getDelimiter();

    @Nonnull
    @Override
    public String toString() {
        return String.join(getDelimiter(), getChildStream().map(RecordQueryPlan::toString).collect(Collectors.toList()));
    }

    @Nonnull
    abstract StoreTimer.Count getPlanCount();

    @Override
    public void logPlanStructure(StoreTimer timer) {
        timer.increment(getPlanCount());
        for (ExpressionRef<RecordQueryPlan> child : children) {
            child.get().logPlanStructure(timer);
        }
    }

    @Override
    public int getComplexity() {
        int complexity = 1;
        for (ExpressionRef<RecordQueryPlan> child : children) {
            complexity += child.get().getComplexity();
        }
        return complexity;
    }

    @Override
    public int getRelationalChildCount() {
        return children.size();
    }
}
