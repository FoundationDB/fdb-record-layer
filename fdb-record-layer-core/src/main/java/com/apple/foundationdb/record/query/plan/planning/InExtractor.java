/*
 * InExtractor.java
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

package com.apple.foundationdb.record.query.plan.planning;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.Bindings;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.FieldKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.NestingKeyExpression;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.expressions.ComponentWithChildren;
import com.apple.foundationdb.record.query.expressions.ComponentWithComparison;
import com.apple.foundationdb.record.query.expressions.ComponentWithNoChildren;
import com.apple.foundationdb.record.query.expressions.ComponentWithSingleChild;
import com.apple.foundationdb.record.query.expressions.FieldWithComparison;
import com.apple.foundationdb.record.query.expressions.LuceneQueryComponent;
import com.apple.foundationdb.record.query.expressions.NestedField;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.record.query.plan.PlanOrderingKey;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryInParameterJoinPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryInValuesJoinPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

/**
 * Extract {@code IN} predicates in a query filter by replacing them with equality comparisons with a bound parameter that will take on the values of the comparison list in turn.
 */
@API(API.Status.INTERNAL)
public class InExtractor {

    private final QueryComponent filter;
    private final List<InClause> inClauses;
    private QueryComponent subFilter;

    public InExtractor(QueryComponent filter) {
        this.filter = filter;
        inClauses = new ArrayList<>();
        subFilter = extractInClauses();
    }

    @SuppressWarnings("unchecked")
    private QueryComponent extractInClauses() {
        final AtomicInteger bindingIndex = new AtomicInteger();
        return mapClauses(filter, (withComparison, fields) -> {
            if (withComparison.getComparison().getType() == Comparisons.Type.IN) {
                String bindingName = Bindings.Internal.IN.bindingName(
                        withComparison.getName() + "__" + bindingIndex.getAndIncrement());
                List<FieldKeyExpression> nestedFields = null;
                if (fields != null && withComparison instanceof FieldWithComparison) {
                    nestedFields = new ArrayList<>(fields);
                    nestedFields.add(Key.Expressions.field(((FieldWithComparison) withComparison).getFieldName()));
                }
                KeyExpression orderingKey = getOrderingKey(nestedFields);

                if (withComparison.getComparison() instanceof Comparisons.ParameterComparison) {
                    final String parameterName = ((Comparisons.ParameterComparison)withComparison.getComparison()).getParameter();
                    inClauses.add(new InParameterClause(bindingName, parameterName, orderingKey));
                } else {
                    final List<Object> comparand = (List<Object>) withComparison.getComparison().getComparand();
                    // ListComparison does not allow empty/null
                    if (comparand != null && comparand.size() == 1) {
                        return withComparison.withOtherComparison(new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, comparand.get(0)));
                    }
                    inClauses.add(new InValuesClause(bindingName, comparand, orderingKey));
                }
                return withComparison.withOtherComparison(new Comparisons.ParameterComparison(Comparisons.Type.EQUALS, bindingName, Bindings.Internal.IN));
            } else {
                return withComparison;
            }
        }, Collections.emptyList());
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    public QueryComponent asOr() {
        return mapClauses(filter, (withComparison, fields) -> {
            if (withComparison.getComparison().getType() == Comparisons.Type.IN) {
                if (withComparison.getComparison() instanceof Comparisons.ParameterComparison) {
                    return withComparison;
                } else {
                    final List<Object> comparands = (List<Object>) withComparison.getComparison().getComparand();
                    final List<QueryComponent> orBranches = new ArrayList<>();
                    for (Object comparand : comparands) {
                        orBranches.add(withComparison.withOtherComparison(new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, comparand)));
                    }

                    // OR must have at least two branches.
                    if (orBranches.size() == 1) {
                        return orBranches.get(0);
                    } else {
                        return Query.or(orBranches);
                    }
                }
            } else {
                return withComparison;
            }

        }, Collections.emptyList());
    }

    private QueryComponent mapClauses(QueryComponent filter, BiFunction<ComponentWithComparison, List<FieldKeyExpression>, QueryComponent> mapper, @Nullable List<FieldKeyExpression> fields) {
        if (filter instanceof ComponentWithComparison) {
            final ComponentWithComparison withComparison = (ComponentWithComparison) filter;
            return mapper.apply(withComparison, fields);
        } else if (filter instanceof ComponentWithChildren) {
            ComponentWithChildren componentWithChildren = (ComponentWithChildren) filter;
            return componentWithChildren.withOtherChildren(
                    componentWithChildren.getChildren().stream()
                            .map(component -> mapClauses(component, mapper, fields))
                            .collect(Collectors.toList())
            );
        } else if (filter instanceof ComponentWithSingleChild) {
            ComponentWithSingleChild componentWithSingleChild = (ComponentWithSingleChild) filter;
            List<FieldKeyExpression> nestedFields = null;
            if (fields != null && componentWithSingleChild instanceof NestedField) {
                nestedFields = new ArrayList<>(fields);
                nestedFields.add(Key.Expressions.field(((NestedField) componentWithSingleChild).getFieldName()));
            }
            return componentWithSingleChild.withOtherChild(
                    mapClauses(componentWithSingleChild.getChild(), mapper, nestedFields));
        } else if (filter instanceof ComponentWithNoChildren || filter instanceof LuceneQueryComponent) {
            return filter;
        } else {
            throw new Query.InvalidExpressionException("Unsupported query type " + filter.getClass());
        }
    }

    @Nullable
    private static KeyExpression getOrderingKey(List<FieldKeyExpression> fields) {
        if (fields == null || fields.isEmpty()) {
            return null;
        }
        KeyExpression key = fields.get(fields.size() - 1);
        for (int i = fields.size() - 2; i >= 0; i--) {
            key = new NestingKeyExpression(fields.get(i), key);
        }
        return key;
    }

    public QueryComponent subFilter() {
        return subFilter;
    }

    public boolean setSort(@Nonnull KeyExpression key, boolean reverse) {
        if (inClauses.isEmpty()) {
            return true;
        }
        final List<KeyExpression> sortComponents = key.normalizeKeyForPositions();
        int i = 0;
        while (i < sortComponents.size() && i < inClauses.size()) {
            final KeyExpression sortComponent = sortComponents.get(i);
            boolean found = false;
            for (int j = i; j < inClauses.size(); j++) {
                final InClause inClause = inClauses.get(j);
                if (sortComponent.equals(inClause.orderingKey)) {
                    if (i != j) {
                        inClauses.remove(j);
                        inClauses.add(i, inClause);
                    }
                    inClause.sortValues = true;
                    inClause.sortReverse = reverse;
                    found = true;
                    break;
                }
            }
            if (!found) {
                // There is a requested sort ahead of the ones from the IN's, so we can't do it.
                cancel();
                return false;
            }
            i++;
        }
        return true;
    }

    public void sortByClauses() {
        for (InClause inClause : inClauses) {
            inClause.sortValues = true;
        }
    }

    public void cancel() {
        inClauses.clear();
        subFilter = filter;
    }

    @Nonnull
    public RecordQueryPlan wrap(RecordQueryPlan plan) {
        for (int i = inClauses.size() - 1; i >= 0; i--) {
            plan = inClauses.get(i).wrap(plan);
        }
        return plan;
    }

    @Nullable
    public PlanOrderingKey adjustOrdering(@Nullable PlanOrderingKey ordering) {
        if (ordering == null || inClauses.isEmpty()) {
            return ordering;
        }
        // All the ordering keys from the IN joins look like non-prefix ordering and come before the others.
        final List<KeyExpression> keys = new ArrayList<>(ordering.getKeys());
        int prefixSize = ordering.getPrefixSize();
        final int primaryKeyStart = ordering.getPrimaryKeyStart();
        final int primaryKeyTailFromEnd = keys.size() - ordering.getPrimaryKeyTail();
        for (int i = 0; i < inClauses.size(); i++) {
            final KeyExpression inOrdering = inClauses.get(i).orderingKey;
            if (inOrdering == null) {
                return null;
            }
            final int position = keys.indexOf(inOrdering);
            if (position >= 0) {
                if (position < prefixSize) {
                    prefixSize--;   // No longer an equality.
                }
                keys.remove(position);
            }
            keys.add(prefixSize + i, inOrdering);
        }
        return new PlanOrderingKey(keys, prefixSize, primaryKeyStart, keys.size() - primaryKeyTailFromEnd);
    }

    abstract static class InClause {
        @Nonnull
        protected final String bindingName;
        @Nullable
        protected final KeyExpression orderingKey;
        protected boolean sortValues;
        protected boolean sortReverse;

        protected InClause(@Nonnull String bindingName, @Nullable KeyExpression orderingKey) {
            this.bindingName = bindingName;
            this.orderingKey = orderingKey;
        }

        protected abstract RecordQueryPlan wrap(RecordQueryPlan inner);
    }

    static class InValuesClause extends InClause {
        @Nullable
        private final List<Object> values;

        protected InValuesClause(@Nonnull String bindingName, @Nullable List<Object> values, @Nullable KeyExpression orderingKey) {
            super(bindingName, orderingKey);
            this.values = values;
        }

        @Override
        protected RecordQueryPlan wrap(RecordQueryPlan inner) {
            return new RecordQueryInValuesJoinPlan(inner, bindingName, values, sortValues, sortReverse);
        }
    }

    static class InParameterClause extends InClause {
        @Nonnull
        private final String parameterName;

        protected InParameterClause(@Nonnull String bindingName, @Nonnull String parameterName, @Nullable KeyExpression orderingKey) {
            super(bindingName, orderingKey);
            this.parameterName = parameterName;
        }

        @Override
        protected RecordQueryPlan wrap(RecordQueryPlan inner) {
            return new RecordQueryInParameterJoinPlan(inner, bindingName, parameterName, sortValues, sortReverse);
        }
    }
}
