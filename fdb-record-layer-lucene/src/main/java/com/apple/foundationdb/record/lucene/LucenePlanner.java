/*
 * LucenePlanner.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2021 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.lucene;

import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordStoreState;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.query.QueryToKeyMatcher;
import com.apple.foundationdb.record.query.expressions.AndComponent;
import com.apple.foundationdb.record.query.expressions.AndOrComponent;
import com.apple.foundationdb.record.query.expressions.BaseField;
import com.apple.foundationdb.record.query.expressions.ComponentWithSingleChild;
import com.apple.foundationdb.record.query.expressions.FieldWithComparison;
import com.apple.foundationdb.record.query.expressions.NestedField;
import com.apple.foundationdb.record.query.expressions.OneOfThemWithComponent;
import com.apple.foundationdb.record.query.expressions.OrComponent;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.record.query.plan.PlannableIndexTypes;
import com.apple.foundationdb.record.query.plan.RecordQueryPlanner;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.planning.FilterSatisfiedMask;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import org.apache.lucene.search.BooleanClause;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * A planner to implement lucene query planning so that we can isolate the lucene functionality to
 * a distinct package. This was necessary because of the need to pass the sort key expression into the
 * plan created and from there into the record cursor which creates a Lucene sort object.
 */
public class LucenePlanner extends RecordQueryPlanner {

    public LucenePlanner(@Nonnull final RecordMetaData metaData, @Nonnull final RecordStoreState recordStoreState, final PlannableIndexTypes indexTypes, final FDBStoreTimer timer) {
        super(metaData, recordStoreState, indexTypes, timer);
    }

    @Override
    protected ScoredPlan planOther(@Nonnull CandidateScan candidateScan,
                                   @Nonnull Index index, @Nonnull QueryComponent filter,
                                   @Nullable KeyExpression sort) {
        if (index.getType().equals(LuceneIndexTypes.LUCENE)) {
            return planLucene(candidateScan, index, filter);
        } else {
            return super.planOther(candidateScan, index, filter, sort);
        }
    }

    private ScoredPlan planLucene(@Nonnull CandidateScan candidateScan,
                                  @Nonnull Index index, @Nonnull QueryComponent filter) {

        final FilterSatisfiedMask filterMask = FilterSatisfiedMask.of(filter);

        final KeyExpression rootExp = index.getRootExpression();
        final ScanComparisons groupingComparisons;

        // Getting grouping information from the index key and query filter
        if (rootExp instanceof GroupingKeyExpression) {
            final KeyExpression groupingKey = ((GroupingKeyExpression)rootExp).getGroupingSubKey();
            final QueryToKeyMatcher.Match groupingMatch = new QueryToKeyMatcher(filter).matchesCoveringKey(groupingKey, filterMask);
            if (!groupingMatch.getType().equals((QueryToKeyMatcher.MatchType.EQUALITY))) {
                return null;
            }
            groupingComparisons = new ScanComparisons(groupingMatch.getEqualityComparisons(), Collections.emptySet());
        } else {
            groupingComparisons = ScanComparisons.EMPTY;
        }

        LucenePlanState state = new LucenePlanState(index, groupingComparisons, filter);
        getFieldDerivations(state);

        // Special scans like auto-complete cannot be combined with regular queries.
        LuceneScanParameters scanParameters = getSpecialScan(state, filterMask);
        if (scanParameters == null) {
            // Scan by means of normal Lucene search API.
            LuceneQueryClause query = getQueryForFilter(state, filter, null, filterMask);
            if (query == null) {
                return null;
            }
            scanParameters = new LuceneScanQueryParameters(groupingComparisons, query);
        }

        // Wrap in plan.
        LuceneIndexQueryPlan lucenePlan = new LuceneIndexQueryPlan(index.getName(), scanParameters, false);
        RecordQueryPlan plan = lucenePlan;
        plan = addTypeFilterIfNeeded(candidateScan, plan, getPossibleTypes(index));
        if (filterMask.allSatisfied()) {
            filterMask.setSatisfied(true);
        }
        return new ScoredPlan(plan, filterMask.getUnsatisfiedFilters(), Collections.emptyList(),  11 - filterMask.getUnsatisfiedFilters().size(),
                state.repeated, null);
    }

    static class LucenePlanState {
        @Nonnull
        final Index index;
        @Nonnull
        final ScanComparisons groupingComparisons;
        @Nonnull
        final QueryComponent filter;

        Map<String, LuceneIndexExpressions.DocumentFieldDerivation> documentFields;
        boolean repeated;   // Matching a repeated field may introduce duplicates

        LucenePlanState(@Nonnull final Index index, @Nonnull final ScanComparisons groupingComparisons, @Nonnull final QueryComponent filter) {
            this.index = index;
            this.groupingComparisons = groupingComparisons;
            this.filter = filter;
        }
    }

    private LuceneScanParameters getSpecialScan(@Nonnull LucenePlanState state, @Nonnull FilterSatisfiedMask filterMask) {
        QueryComponent queryComponent = state.groupingComparisons.isEmpty() ? state.filter : filterMask.getUnsatisfiedFilter();
        if (queryComponent instanceof LuceneQueryComponent) {
            LuceneQueryComponent luceneQueryComponent = (LuceneQueryComponent)queryComponent;
            for (String field : luceneQueryComponent.getFields()) {
                if (!validateIndexField(state, field)) {
                    return null;
                }
            }
            LuceneScanParameters scanParameters;
            switch (luceneQueryComponent.getType()) {
                case AUTO_COMPLETE:
                    scanParameters = new LuceneScanAutoCompleteParameters(state.groupingComparisons,
                            luceneQueryComponent.getQuery(), luceneQueryComponent.isQueryIsParameter());
                    break;
                case SPELL_CHECK:
                    scanParameters = new LuceneScanSpellCheckParameters(state.groupingComparisons,
                            luceneQueryComponent.getQuery(), luceneQueryComponent.isQueryIsParameter());
                    break;
                default:
                    scanParameters = null;
                    break;
            }
            if (scanParameters != null) {
                if (queryComponent != state.filter) {
                    filterMask = filterMask.getChild(queryComponent);
                }
                filterMask.setSatisfied(true);
                return scanParameters;
            }
        }
        return null;
    }

    private LuceneQueryClause getQueryForFilter(@Nonnull LucenePlanState state, @Nonnull QueryComponent filter,
                                                @Nullable String parentFieldName, @Nullable FilterSatisfiedMask filterMask) {
        if (filter instanceof LuceneQueryComponent) {
            return getQueryForLuceneComponent(state, (LuceneQueryComponent)filter, filterMask);
        } else if (filter instanceof AndOrComponent) {
            return getQueryForAndOr(state, (AndOrComponent) filter, parentFieldName, filterMask);
        } else if (filter instanceof FieldWithComparison) {
            return getQueryForFieldWithComparison(state, (FieldWithComparison) filter, parentFieldName, filterMask);
        } else if (filter instanceof OneOfThemWithComponent) {
            return getQueryForNestedField(state, (OneOfThemWithComponent)filter, true, parentFieldName, filterMask);
        } else if (filter instanceof NestedField) {
            return getQueryForNestedField(state, (NestedField) filter, false, parentFieldName, filterMask);
        }
        return null;
    }

    private LuceneQueryClause getQueryForLuceneComponent(@Nonnull LucenePlanState state, @Nonnull LuceneQueryComponent filter,
                                                         @Nullable FilterSatisfiedMask filterMask) {
        //TODO figure out how to take into account the parentField name here. Or maybe disallow this if its contained within a
        // oneOfThem. Not sure if thats even allowed via the metadata validation on the query at the start of the planner.
        for (String field : filter.getFields()) {
            if (!validateIndexField(state, field)) {
                return null;
            }
        }
        if (filterMask != null) {
            filterMask.setSatisfied(true);
        }

        if (filter.isMultiFieldSearch()) {
            return new LuceneQueryMultiFieldSearchClause(filter.getQuery(), filter.isQueryIsParameter());
        } else {
            return new LuceneQuerySearchClause(filter.getQuery(), filter.isQueryIsParameter());
        }
    }

    @SuppressWarnings("java:S3776")
    private LuceneQueryClause getQueryForAndOr(@Nonnull LucenePlanState state, @Nonnull AndOrComponent filter,
                                               @Nullable String parentFieldName, @Nullable FilterSatisfiedMask filterMask) {
        final Iterator<FilterSatisfiedMask> subFilterMasks = filterMask != null ? filterMask.getChildren().iterator() : null;
        final List<QueryComponent> filters = filter.getChildren();
        final List<LuceneQueryClause> childClauses = new ArrayList<>(filters.size());
        for (QueryComponent subFilter : filters) {
            final FilterSatisfiedMask childMask = subFilterMasks != null ? subFilterMasks.next() : null;
            LuceneQueryClause childClause = getQueryForFilter(state, subFilter, parentFieldName, childMask);
            if (childClause == null) {
                if (filter == state.filter && filter instanceof AndComponent) {
                    continue;    // Partially unsatisfied at top-level is okay.
                }
                return null;
            }
            if (childMask != null) {
                childMask.setSatisfied(true);
            }
            childClauses.add(childClause);
        }
        if (filterMask != null && filterMask.getUnsatisfiedFilters().isEmpty()) {
            filterMask.setSatisfied(true);
        }
        if (childClauses.isEmpty()) {
            return null;
        }
        final BooleanClause.Occur occur = filter instanceof OrComponent ? BooleanClause.Occur.SHOULD : BooleanClause.Occur.MUST;
        return new LuceneBooleanQuery(childClauses, occur);
    }

    private LuceneQueryClause getQueryForFieldWithComparison(@Nonnull LucenePlanState state, @Nonnull FieldWithComparison filter,
                                                             @Nullable String parentFieldName, @Nullable FilterSatisfiedMask filterSatisfiedMask) {
        if (filterSatisfiedMask != null && filterSatisfiedMask.isSatisfied()) {
            return null;        // Already done as part of group comparisons
        }
        String completeFieldName = filter.getFieldName();
        if (parentFieldName != null) {
            completeFieldName = parentFieldName + "_" + completeFieldName;
        }
        LuceneIndexExpressions.DocumentFieldType fieldType = getIndexFieldType(state, completeFieldName);
        if (fieldType == null) {
            return null;
        }
        if (filterSatisfiedMask != null) {
            filterSatisfiedMask.setSatisfied(true);
        }
        return LuceneQueryFieldComparisonClause.create(completeFieldName, fieldType, filter.getComparison());
    }

    // TODO Better implementation of nesting that actually takes into account
    //  positioning of the fields in relation to each other
    // This should use the multiField query parser. Very much a TODO
    private LuceneQueryClause getQueryForNestedField(@Nonnull LucenePlanState state, @Nonnull BaseField filter, boolean repeated,
                                                     @Nullable String parentFieldName, @Nullable FilterSatisfiedMask mask) {
        String fieldName = filter.getFieldName();
        if (parentFieldName != null) {
            fieldName = parentFieldName + "_" + fieldName;
        }
        QueryComponent child = ((ComponentWithSingleChild)filter).getChild();
        LuceneQueryClause comparison = getQueryForFilter(state, child, fieldName, (mask != null) ? mask.getChild(child) : null);
        if (comparison != null ) {
            if (mask != null) {
                mask.setSatisfied(true);
            }
            if (repeated) {
                state.repeated = true;
            }
        }
        return comparison;
    }

    @Nonnull
    private void getFieldDerivations(@Nonnull LucenePlanState state) {
        Map<String, LuceneIndexExpressions.DocumentFieldDerivation> combined = null;
        for (RecordType recordType : getRecordMetaData().recordTypesForIndex(state.index)) {
            Map<String, LuceneIndexExpressions.DocumentFieldDerivation> documentFields =
                    LuceneIndexExpressions.getDocumentFieldDerivations(state.index.getRootExpression(), recordType.getDescriptor());
            if (combined == null) {
                combined = documentFields;
            } else {
                combined.putAll(documentFields);
            }
        }
        if (combined == null) {
            combined = Collections.emptyMap();
        }
        state.documentFields = combined;
    }

    @Nullable
    private LuceneIndexExpressions.DocumentFieldType getIndexFieldType(@Nonnull LucenePlanState state, @Nonnull String field) {
        // For a filter made from components, we have the complete path of record fields and so the actual document field name.
        final LuceneIndexExpressions.DocumentFieldDerivation fieldDerivation = state.documentFields.get(field);
        if (fieldDerivation != null) {
            return fieldDerivation.getType();
        }
        return null;
    }

    private boolean validateIndexField(@Nonnull LucenePlanState state, @Nonnull String field) {
        // Can only check that the field name given corresponds to some top-level branch to an indexed field.
        for (LuceneIndexExpressions.DocumentFieldDerivation fieldDerivation : state.documentFields.values()) {
            if (fieldDerivation.getRecordFieldPath().get(0).equals(field)) {
                return true;
            }
        }
        return false;
    }

}
