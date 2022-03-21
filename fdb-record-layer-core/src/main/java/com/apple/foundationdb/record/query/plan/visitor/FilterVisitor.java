/*
 * FilterVisitor.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2020 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.visitor;

import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.FieldKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.FunctionKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.LiteralKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.QueryableKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.ThenKeyExpression;
import com.apple.foundationdb.record.query.expressions.AndOrComponent;
import com.apple.foundationdb.record.query.expressions.FieldWithComparison;
import com.apple.foundationdb.record.query.expressions.NestedField;
import com.apple.foundationdb.record.query.expressions.NotComponent;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.record.query.expressions.QueryKeyExpressionWithComparison;
import com.apple.foundationdb.record.query.expressions.QueryKeyExpressionWithOneOfComparison;
import com.apple.foundationdb.record.query.plan.AvailableFields;
import com.apple.foundationdb.record.query.plan.PlannableIndexTypes;
import com.apple.foundationdb.record.query.plan.plans.TranslateValueFunction;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryFetchFromPartialRecordPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryFilterPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.temp.Type;
import com.google.common.base.Verify;
import com.google.common.collect.Lists;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * A substitution visitor that pushes a filter below a record fetch if all of the (non-repeated) field are available
 * in a covering scan.
 */
public class FilterVisitor extends RecordQueryPlannerSubstitutionVisitor {
    public FilterVisitor(@Nonnull final RecordMetaData recordMetadata, @Nonnull final PlannableIndexTypes indexTypes, @Nullable final KeyExpression commonPrimaryKey) {
        super(recordMetadata, indexTypes, commonPrimaryKey);
    }

    @Nonnull
    @Override
    public RecordQueryPlan postVisit(@Nonnull RecordQueryPlan recordQueryPlan) {
        if (recordQueryPlan instanceof RecordQueryFilterPlan) {
            final RecordQueryFilterPlan filterPlan = (RecordQueryFilterPlan)recordQueryPlan;

            final List<QueryComponent> filters = filterPlan.getFilters();
            final AvailableFields availableFields = availableFields(((RecordQueryFilterPlan)recordQueryPlan).getInnerPlan());

            // Partition the filters according to whether they can be evaluated using just the fields from the index or
            // if they need a full record.
            final List<QueryComponent> indexFilters = Lists.newArrayListWithCapacity(filters.size());
            final List<QueryComponent> residualFilters = Lists.newArrayListWithCapacity(filters.size());
            final Set<KeyExpression> allReferencedFields = new HashSet<>();

            partitionFilters(filters, availableFields, indexFilters, residualFilters, allReferencedFields);

            Verify.verify(indexFilters.size() + residualFilters.size() == filters.size());

            // We now know the set of index filters and true residuals. Create a plan:
            // a) if there are no index filters: filter(index_scan(...), residuals) (leave plan unchanged)
            // b) if there are index filters but no residuals: fetch(filter(covering_index_scan(...), index_filters))
            // c) if there are index filters and residuals: filter(fetch(filter(covering_index_scan(...), index_filters)), residuals)

            if (indexFilters.isEmpty()) {
                return recordQueryPlan;
            }

            @Nullable RecordQueryPlan removedFetchPlan = removeIndexFetch(filterPlan.getChild(), allReferencedFields);
            if (removedFetchPlan == null) {
                return recordQueryPlan;
            }

            recordQueryPlan = new RecordQueryFetchFromPartialRecordPlan(
                    new RecordQueryFilterPlan(removedFetchPlan, indexFilters),
                    TranslateValueFunction.unableToTranslate(),
                    new Type.Any());

            if (!residualFilters.isEmpty()) {
                recordQueryPlan = new RecordQueryFilterPlan(recordQueryPlan, residualFilters);
            }
        }
        return recordQueryPlan;
    }

    public static void partitionFilters(@Nonnull final List<QueryComponent> filters,
                                        @Nonnull final AvailableFields availableFields,
                                        @Nonnull final List<QueryComponent> indexFilters,
                                        @Nonnull final List<QueryComponent> residualFilters,
                                        @Nullable final Set<KeyExpression> allReferencedFields) {
        for (final QueryComponent filter : filters) {
            final Set<KeyExpression> referencedFields = new HashSet<>();
            if (findFilterReferencedFields(filter, referencedFields)) {
                if (availableFields.containsAll(referencedFields)) {
                    indexFilters.add(filter);
                    if (allReferencedFields != null) {
                        allReferencedFields.addAll(referencedFields);
                    }
                    continue;
                }
            }
            residualFilters.add(filter);
        }
    }

    // Find equivalent key expressions for fields used by the given filter.
    // Does not attempt to deal with OneOfThemWithComparison/OneOfThemWithComponent, as the repeated nested field will be spread across multiple
    // index entries. Reconstituting that as a singleton in a partial record might work for the simplest case, but
    // could not for multiple such filter conditions.
    // QueryKeyExpressionWithOneOfComparison is okay if a scalar field produces a repeated result.
    public static boolean findFilterReferencedFields(@Nonnull QueryComponent filter, @Nonnull Set<KeyExpression> filterFields) {
        if (filter instanceof FieldWithComparison) {
            filterFields.add(Key.Expressions.field(((FieldWithComparison)filter).getFieldName()));
            return true;
        }
        if (filter instanceof AndOrComponent) {
            for (QueryComponent child : ((AndOrComponent)filter).getChildren()) {
                if (!findFilterReferencedFields(child, filterFields)) {
                    return false;
                }
            }
            return true;
        }
        if (filter instanceof NotComponent) {
            final QueryComponent child = ((NotComponent)filter).getChild();
            if (!findFilterReferencedFields(child, filterFields)) {
                return false;
            }
            return true;
        }
        if (filter instanceof QueryKeyExpressionWithComparison) {
            final QueryableKeyExpression keyExpression = ((QueryKeyExpressionWithComparison)filter).getKeyExpression();
            return findFilterReferencedFields(keyExpression, filterFields);
        }
        if (filter instanceof QueryKeyExpressionWithOneOfComparison) {
            final QueryableKeyExpression keyExpression = ((QueryKeyExpressionWithOneOfComparison)filter).getKeyExpression();
            return findFilterReferencedFields(keyExpression, filterFields);
        }
        if (filter instanceof NestedField) {
            final Set<KeyExpression> childFilterFields = new HashSet<>();
            if (findFilterReferencedFields(((NestedField)filter).getChild(), childFilterFields)) {
                final FieldKeyExpression parent = Key.Expressions.field(((NestedField)filter).getFieldName());
                for (KeyExpression childFilterField : childFilterFields) {
                    filterFields.add(parent.nest(childFilterField));
                }
                return true;
            } else {
                return false;
            }
        }
        return false;
    }

    private static boolean findFilterReferencedFields(@Nonnull KeyExpression expression, @Nonnull Set<KeyExpression> filterFields) {
        if (expression instanceof ThenKeyExpression) {
            for (KeyExpression child : ((ThenKeyExpression)expression).getChildren()) {
                if (!findFilterReferencedFields(child, filterFields)) {
                    return false;
                }
            }
            return true;
        }
        // TODO: This isn't quite optimal, since an index might just as well have f(x) as a indexed field as x itself.
        //   But that isn't expressible with the current partial record implementation.
        if (expression instanceof FunctionKeyExpression) {
            return findFilterReferencedFields(((FunctionKeyExpression)expression).getArguments(), filterFields);
        }
        if (expression instanceof LiteralKeyExpression) {
            return true;
        }
        filterFields.add(expression);
        return true;
    }
}
