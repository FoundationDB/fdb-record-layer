/*
 * JoinedRecordPlanner.java
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

package com.apple.foundationdb.record.query.plan.synthetic;

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.metadata.JoinedRecordType;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.FieldKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.InvertibleFunctionKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.NestingKeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.expressions.FieldWithComparison;
import com.apple.foundationdb.record.query.expressions.NestedField;
import com.apple.foundationdb.record.query.expressions.OneOfThemWithComparison;
import com.apple.foundationdb.record.query.expressions.OneOfThemWithComponent;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.record.query.plan.RecordQueryPlanner;
import com.apple.foundationdb.record.query.plan.cascades.TranslationMap;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.util.HashUtils;
import com.google.protobuf.Descriptors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A planner for {@link JoinedRecordPlan}.
 *
 * <p>
 * Anticipates, in a very rough way, some of the things that join plan generation will do in the regular {@link RecordQueryPlanner}.
 * </p>
 *
 * <p>
 * Since joined record types are synthesized from both sides of the join, depending on what record has been modified,
 * there really need to be indexes in both directions, which means that join ordering isn't as much of an issue.
 * </p>
 *
 */
class JoinedRecordPlanner {
    @Nonnull
    private final JoinedRecordType joinedRecordType;
    @Nonnull
    private final RecordQueryPlanner queryPlanner;
    @Nonnull
    private final List<PendingType> pendingTypes;
    @Nonnull
    private final Set<PendingJoin> pendingJoins;
    @Nonnull
    private final List<JoinedRecordPlan.JoinedType> joinedTypes;
    @Nonnull
    private final List<RecordQueryPlan> queries;

    private int bindingCounter;

    static class PendingType {
        protected final JoinedRecordType.JoinConstituent joinConstituent;
        protected final List<PendingJoin> pendingJoins;

        PendingType(JoinedRecordType.JoinConstituent joinConstituent) {
            this.joinConstituent = joinConstituent;

            pendingJoins = new ArrayList<>();
        }

        public boolean allJoinsBound() {
            return pendingJoins.stream().allMatch(this::isJoinBound);
        }

        public long countJoinsBound() {
            return pendingJoins.stream().filter(this::isJoinBound).count();
        }

        @SuppressWarnings("PMD.CompareObjectsWithEquals")
        public boolean isJoinBound(@Nonnull PendingJoin pendingJoin) {
            if (pendingJoin.pendingLeft == this) {
                return pendingJoin.rightBound;
            } else if (pendingJoin.pendingRight == this) {
                return pendingJoin.leftBound;
            } else {
                throw notFoundEitherSide();
            }
        }
    }

    static RecordCoreException notFoundEitherSide() {
        return new RecordCoreException("did not find pending join on either side");
    }

    static class PendingJoin {
        protected final JoinedRecordType.Join join;
        protected final PendingType pendingLeft;
        protected final PendingType pendingRight;
        protected final String bindingName;
        protected boolean singleton;
        protected boolean leftBound;
        protected boolean rightBound;

        PendingJoin(JoinedRecordType.Join join, PendingType pendingLeft, PendingType pendingRight, String bindingName) {
            this.join = join;
            this.pendingLeft = pendingLeft;
            this.pendingRight = pendingRight;
            this.bindingName = bindingName;
        }
    }

    JoinedRecordPlanner(@Nonnull JoinedRecordType joinedRecordType, @Nonnull RecordQueryPlanner queryPlanner) {
        this.joinedRecordType = joinedRecordType;
        this.queryPlanner = queryPlanner;

        this.pendingTypes = joinedRecordType.getConstituents().stream().map(this::createPendingType).collect(Collectors.toCollection(ArrayList::new));
        this.pendingJoins = joinedRecordType.getJoins().stream().map(this::createPendingJoin).collect(Collectors.toCollection(HashSet::new));

        joinedTypes = new ArrayList<>(pendingTypes.size());
        queries = new ArrayList<>(pendingTypes.size() - 1);
    }

    @Nonnull
    private PendingType createPendingType(@Nonnull JoinedRecordType.JoinConstituent joinConstituent) {
        return new PendingType(joinConstituent);
    }

    @Nonnull
    private PendingJoin createPendingJoin(@Nonnull JoinedRecordType.Join join) {
        final PendingType pendingLeft = findPendingType(join.getLeft());
        final PendingType pendingRight = findPendingType(join.getRight());
        final PendingJoin pendingJoin = new PendingJoin(join, pendingLeft, pendingRight, String.format("_j%d", ++bindingCounter));
        pendingLeft.pendingJoins.add(pendingJoin);
        pendingRight.pendingJoins.add(pendingJoin);
        return pendingJoin;
    }

    @Nonnull
    private PendingType findPendingType(@Nonnull JoinedRecordType.JoinConstituent joinConstituent) {
        // Only works before removing from pending, so during construction and first thing in plan.
        return pendingTypes.get(joinedRecordType.getConstituents().indexOf(joinConstituent));
    }

    @Nonnull
    public JoinedRecordPlan plan(@Nonnull JoinedRecordType.JoinConstituent joinConstituent) {
        PendingType pendingType = findPendingType(joinConstituent);
        bindAndRemove(pendingType);
        while (!pendingTypes.isEmpty()) {
            if (pendingTypes.size() == 1) {
                pendingType = pendingTypes.get(0);
            } else {
                pendingType = pendingTypes.stream().filter(PendingType::allJoinsBound).findFirst()
                        .orElseGet(() -> pendingTypes.stream().max(Comparator.comparing(PendingType::countJoinsBound))
                                .orElseThrow(() -> new RecordCoreException("did not find any pending types")));
            }
            queries.add(queryPlanner.plan(buildQuery(pendingType)));
            bindAndRemove(pendingType);
        }
        if (!pendingJoins.isEmpty()) {
            // pendingJoins are removed by buildQuery as both sides become bound, so the plan must be incomplete.
            throw new RecordCoreException("did not perform all joins");
        }
        return new JoinedRecordPlan(joinedRecordType, joinedTypes, queries);
    }

    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    private void bindAndRemove(@Nonnull PendingType pendingType) {
        final List<JoinedRecordPlan.BindingPlan> bindingPlans = new ArrayList<>();
        for (PendingJoin pendingJoin : pendingType.pendingJoins) {
            if (pendingJoins.contains(pendingJoin)) {
                final KeyExpression expression;
                if (pendingJoin.pendingLeft == pendingType) {
                    expression = pendingJoin.join.getLeftExpression();
                    pendingJoin.leftBound = true;
                } else if (pendingJoin.pendingRight == pendingType) {
                    expression = pendingJoin.join.getRightExpression();
                    pendingJoin.rightBound = true;
                } else {
                    throw notFoundEitherSide();
                }
                pendingJoin.singleton = !expression.createsDuplicates();
                bindingPlans.add(new JoinedRecordPlan.BindingPlan(pendingJoin.bindingName, expression, pendingJoin.singleton));
            }
        }
        // The list is not meaningfully ordered, so impose a canonical ordering so it's invariant.
        bindingPlans.sort(Comparator.comparing(JoinedRecordPlan.BindingPlan::getName));
        joinedTypes.add(new JoinedRecordPlan.JoinedType(pendingType.joinConstituent, bindingPlans));
        pendingTypes.remove(pendingType);
    }

    @Nonnull
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    private RecordQuery buildQuery(@Nonnull PendingType pendingType) {
        final List<QueryComponent> conditions = new ArrayList<>();
        for (PendingJoin pendingJoin : pendingType.pendingJoins) {
            final boolean bound;
            final KeyExpression expression;
            if (pendingJoin.pendingLeft == pendingType) {
                bound = pendingJoin.rightBound;
                expression = pendingJoin.join.getLeftExpression();
            } else if (pendingJoin.pendingRight == pendingType) {
                bound = pendingJoin.leftBound;
                expression = pendingJoin.join.getRightExpression();
            } else {
                throw notFoundEitherSide();
            }
            if (bound) {
                final Comparisons.Comparison comparison = new Comparisons.ParameterComparison(pendingJoin.singleton ? Comparisons.Type.EQUALS : Comparisons.Type.IN, pendingJoin.bindingName);
                conditions.add(buildCondition(expression, comparison));
                pendingJoins.remove(pendingJoin);
            }
        }
        RecordQuery.Builder builder = RecordQuery.newBuilder();
        builder.setRecordType(pendingType.joinConstituent.getRecordType().getName());
        if (!conditions.isEmpty()) {
            builder.setFilter(conditions.size() > 1 ? Query.and(conditions) : conditions.get(0));
        }
        // An alternative would be to add duplicate elimination on the result of the join before forming the synthetic records.
        // But until the planner is able to push that down / eliminate it when the scan does not produce duplicates, it's better to give it here,
        // since the planner at least understands the simple cases of the elimination.
        builder.setRemoveDuplicates(true);
        return builder.build();
    }

    @Nonnull
    private static QueryComponent buildCondition(@Nonnull KeyExpression expression, @Nonnull Comparisons.Comparison comparison) {
        if (expression instanceof FieldKeyExpression) {
            final FieldKeyExpression field = (FieldKeyExpression)expression;
            switch (field.getFanType()) {
                case None:
                    return new FieldWithComparison(field.getFieldName(), comparison);
                case FanOut:
                    return new OneOfThemWithComparison(field.getFieldName(), comparison);
                default:
                    throw new RecordCoreException("unsupported fan type in join key expression: " + expression);
            }
        } else if (expression instanceof NestingKeyExpression) {
            final NestingKeyExpression nesting = (NestingKeyExpression)expression;
            final QueryComponent condition = buildCondition(nesting.getChild(), comparison);
            final String fieldName = nesting.getParent().getFieldName();
            switch (nesting.getParent().getFanType()) {
                case None:
                    return new NestedField(fieldName, condition);
                case FanOut:
                    return new OneOfThemWithComponent(fieldName, condition);
                default:
                    throw new RecordCoreException("unsupported fan type in join key expression: " + expression);
            }
        } else if (expression instanceof InvertibleFunctionKeyExpression) {
            final InvertibleFunctionKeyExpression function = (InvertibleFunctionKeyExpression)expression;
            final Comparisons.Comparison inverted = invertFunctionComparison(function, comparison);
            return buildCondition(function.getArguments(), inverted);
        } else {
            throw new RecordCoreException("unsupported join key expression: " + expression);
        }
    }

    @Nonnull
    private static Comparisons.Comparison invertFunctionComparison(@Nonnull InvertibleFunctionKeyExpression function, @Nonnull Comparisons.Comparison comparison) {
        // Limit function comparisons (for now) to unary functions
        if (function.getColumnSize() != 1) {
            throw new RecordCoreException("invertible functions in joins must return a single element");
        }
        if (function.getMinArguments() != 1 || function.getMaxArguments() != 1) {
            throw new RecordCoreException("invertible functions in joins must operate on a single element");
        }
        return new InvertFunctionComparisonWithParameter(function, comparison);
    }

    private static class InvertFunctionComparisonWithParameter implements Comparisons.Comparison {
        @Nonnull
        private final InvertibleFunctionKeyExpression function;
        @Nonnull
        private final Comparisons.Comparison underlyingComparison;

        private InvertFunctionComparisonWithParameter(@Nonnull InvertibleFunctionKeyExpression function,
                                                      @Nonnull Comparisons.Comparison underlyingComparison) {
            this.function = function;
            this.underlyingComparison = underlyingComparison;
        }

        @Override
        public int planHash(@Nonnull final PlanHashKind hashKind) {
            return PlanHashable.planHash(hashKind, function, underlyingComparison);
        }

        @Override
        public int queryHash(@Nonnull final QueryHashKind hashKind) {
            return HashUtils.queryHash(hashKind, function, underlyingComparison);
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final InvertFunctionComparisonWithParameter that = (InvertFunctionComparisonWithParameter)o;
            return Objects.equals(function, that.function) && Objects.equals(underlyingComparison, that.underlyingComparison);
        }

        @Override
        public int hashCode() {
            return Objects.hash(function, underlyingComparison);
        }

        @Nullable
        @Override
        public Boolean eval(@Nonnull final FDBRecordStoreBase<?> store, @Nonnull final EvaluationContext context, @Nullable final Object value) {
            Object comparand = getComparand(store, context);
            return Comparisons.evalComparison(getType(), value, comparand);
        }

        @Override
        public void validate(@Nonnull final Descriptors.FieldDescriptor descriptor, final boolean fannedOut) {
            underlyingComparison.validate(descriptor, fannedOut);
        }

        @Nonnull
        @Override
        public Comparisons.Type getType() {
            return function.isInjective() ? underlyingComparison.getType() : Comparisons.Type.IN;
        }

        @Nullable
        @Override
        public Object getComparand(@Nullable final FDBRecordStoreBase<?> store, @Nullable final EvaluationContext context) {
            Object underlyingComparandValue = underlyingComparison.getComparand(store, context);
            if (underlyingComparandValue instanceof List<?>) {
                List<?> underlyingList = (List<?>) underlyingComparandValue;
                List<Object> finalValues = new ArrayList<>(underlyingList.size());
                for (Object obj : underlyingList) {
                    Key.Evaluated evaluated = Key.Evaluated.scalar(obj);
                    List<Key.Evaluated> inverse = function.evaluateInverse(evaluated);
                    inverse.stream()
                            .filter(eval -> eval.size() == 1)
                            .forEach(eval -> finalValues.add(eval.getObject(0)));
                }
                return finalValues;
            } else {
                Key.Evaluated evaluated = Key.Evaluated.scalar(underlyingComparandValue);
                List<Key.Evaluated> inverse = function.evaluateInverse(evaluated);
                if (getType() == Comparisons.Type.IN) {
                    return inverse.stream()
                            .filter(eval -> eval.size() == 1)
                            .map(eval -> eval.getObject(0))
                            .collect(Collectors.toList());
                } else {
                    Key.Evaluated preImage = inverse.get(0);
                    if (preImage.size() != 1) {
                        throw new RecordCoreException("unable to get singleton pre-image for function");
                    }
                    return preImage.getObject(0);
                }
            }
        }

        @Nonnull
        @Override
        public String typelessString() {
            return function.getName() + "^-1(" + underlyingComparison.typelessString() + ")";
        }

        @Override
        public String toString() {
            return getType() + " " + typelessString();
        }

        @Nonnull
        @Override
        @SuppressWarnings({"PMD.CompareObjectsWithEquals"}) // used here for referential equality
        public Comparisons.Comparison translateCorrelations(@Nonnull final TranslationMap translationMap) {
            Comparisons.Comparison translated = underlyingComparison.translateCorrelations(translationMap);
            if (translated == underlyingComparison) {
                return this;
            } else {
                return new InvertFunctionComparisonWithParameter(function, translated);
            }
        }
    }
}
