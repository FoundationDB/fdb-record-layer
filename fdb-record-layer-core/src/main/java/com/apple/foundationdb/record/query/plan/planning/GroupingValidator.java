/*
 * GroupingValidator.java
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
import com.apple.foundationdb.record.metadata.expressions.FieldKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.NestingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.ThenKeyExpression;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.expressions.FieldWithComparison;
import com.apple.foundationdb.record.query.expressions.NestedField;
import com.apple.foundationdb.record.query.expressions.QueryComponent;

import javax.annotation.Nonnull;
import java.util.List;

/**
 * A utility class for determining if the grouping portion of a key expression
 * is satisfied by a query's predicate. In particular, this is useful planning queries using index
 * types (such as the {@value com.apple.foundationdb.record.metadata.IndexTypes#RANK}
 * or {@value com.apple.foundationdb.record.metadata.IndexTypes#TEXT} indexes) where queries
 * cannot span multiple values of the grouping key.
 */
@API(API.Status.INTERNAL)
class GroupingValidator {
    // TODO: Use QueryToKeyMatcher when planning groups (https://github.com/FoundationDB/fdb-record-layer/issues/21)
    //  Most of what's done here is also done in the QueryToKeyMatcher class, it just also needs information
    //  about what filters were used.

    /**
     * If the grouping key can be restricted to a single value by the filters specified, put them into <code>groupFilters</code>
     * and the corresponding comparisons into <code>groupComparisons</code> and return <code>true</code>.
     * If the grouping key cannot be satisfied, return <code>false</code>.
     *
     * @param filters the list of filters in this query
     * @param groupKey the grouping key
     * @param groupFilters a list into which the filters used to satisfy the group key will be placed
     * @param groupComparisons a list into which the comparisons used to satisfy the group key will be placed
     * @return <code>true</code> if the key is restricted to a single value by a subset of the filters provided or <code>false</code> otherwise
     */
    static boolean findGroupKeyFilters(@Nonnull List<QueryComponent> filters, @Nonnull KeyExpression groupKey,
                                       @Nonnull List<QueryComponent> groupFilters, @Nonnull List<Comparisons.Comparison> groupComparisons) {
        if (groupKey.getColumnSize() == 0) {
            return true;
        }
        if (groupKey instanceof ThenKeyExpression) {
            for (KeyExpression subKey : ((ThenKeyExpression)groupKey).getChildren()) {
                if (!findGroupKeyFilters(filters, subKey, groupFilters, groupComparisons)) {
                    return false;
                }
            }
            return true;
        }
        return (groupKey instanceof FieldKeyExpression &&
                findGroupFieldFilter(filters, (FieldKeyExpression)groupKey, groupFilters, groupComparisons)) ||
               (groupKey instanceof NestingKeyExpression &&
                findNestingFilter(filters, (NestingKeyExpression)groupKey, groupFilters, groupComparisons));
    }

    private static boolean findGroupFieldFilter(@Nonnull List<QueryComponent> filters, @Nonnull FieldKeyExpression groupField,
                                                @Nonnull List<QueryComponent> groupFilters, @Nonnull List<Comparisons.Comparison> groupComparisons) {
        for (QueryComponent filter : filters) {
            if (filter instanceof FieldWithComparison) {
                FieldWithComparison comparisonFilter = (FieldWithComparison)filter;
                if (comparisonFilter.getFieldName().equals(groupField.getFieldName()) &&
                        (comparisonFilter.getComparison().getType() == Comparisons.Type.EQUALS ||
                         comparisonFilter.getComparison().getType() == Comparisons.Type.IS_NULL)) {
                    groupFilters.add(filter);
                    groupComparisons.add(comparisonFilter.getComparison());
                    return true;
                }
            }
        }
        return false;
    }

    private static boolean findNestingFilter(@Nonnull List<QueryComponent> filters, @Nonnull NestingKeyExpression nesting,
                                             @Nonnull List<QueryComponent> groupFilters, @Nonnull List<Comparisons.Comparison> groupComparisons) {
        if (nesting.getChild() instanceof FieldKeyExpression) {
            FieldKeyExpression parentField = nesting.getParent();
            FieldKeyExpression childField = (FieldKeyExpression)nesting.getChild();
            for (QueryComponent filter : filters) {
                if (filter instanceof NestedField) {
                    NestedField nested = (NestedField)filter;
                    if (nested.getFieldName().equals(parentField.getFieldName()) &&
                            nested.getChild() instanceof FieldWithComparison) {
                        FieldWithComparison comparisonFilter = (FieldWithComparison)nested.getChild();
                        if (comparisonFilter.getFieldName().equals(childField.getFieldName()) &&
                                (comparisonFilter.getComparison().getType() == Comparisons.Type.EQUALS ||
                                 comparisonFilter.getComparison().getType() == Comparisons.Type.IS_NULL)) {
                            groupFilters.add(filter);
                            groupComparisons.add(comparisonFilter.getComparison());
                            return true;
                        }
                    }
                }
            }
        }
        return false;
    }

    private GroupingValidator() {
    }
}
