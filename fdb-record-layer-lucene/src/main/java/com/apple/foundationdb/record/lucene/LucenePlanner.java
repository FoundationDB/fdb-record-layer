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

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordStoreState;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.metadata.expressions.EmptyKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.FieldKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.FunctionKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.NestingKeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.query.QueryToKeyMatcher;
import com.apple.foundationdb.record.query.expressions.AndComponent;
import com.apple.foundationdb.record.query.expressions.AndOrComponent;
import com.apple.foundationdb.record.query.expressions.BaseField;
import com.apple.foundationdb.record.query.expressions.ComponentWithSingleChild;
import com.apple.foundationdb.record.query.expressions.FieldWithComparison;
import com.apple.foundationdb.record.query.expressions.NestedField;
import com.apple.foundationdb.record.query.expressions.NotComponent;
import com.apple.foundationdb.record.query.expressions.OneOfThemWithComponent;
import com.apple.foundationdb.record.query.expressions.OrComponent;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.record.query.plan.PlanOrderingKey;
import com.apple.foundationdb.record.query.plan.PlannableIndexTypes;
import com.apple.foundationdb.record.query.plan.RecordQueryPlanner;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.planning.FilterSatisfiedMask;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.google.common.collect.ImmutableList;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * A planner to implement lucene query planning so that we can isolate the lucene functionality to
 * a distinct package. This was necessary because of the need to pass the sort key expression into the
 * plan created and from there into the record cursor which creates a Lucene sort object.
 */
public class LucenePlanner extends RecordQueryPlanner {
    private static final Logger logger = LoggerFactory.getLogger(LucenePlanner.class);

    public LucenePlanner(@Nonnull final RecordMetaData metaData, @Nonnull final RecordStoreState recordStoreState, final PlannableIndexTypes indexTypes, final FDBStoreTimer timer) {
        super(metaData, recordStoreState, indexTypes, timer);
    }

    @Override
    @Nullable
    protected ScoredPlan planOther(@Nonnull CandidateScan candidateScan,
                                   @Nonnull Index index, @Nonnull QueryComponent filter,
                                   @Nullable KeyExpression sort, boolean sortReverse,
                                   @Nullable KeyExpression commonPrimaryKey) {
        if (index.getType().equals(LuceneIndexTypes.LUCENE)) {
            return planLucene(candidateScan, index, filter, sort, sortReverse, commonPrimaryKey);
        } else {
            return super.planOther(candidateScan, index, filter, sort, sortReverse, commonPrimaryKey);
        }
    }

    @Nullable
    private ScoredPlan planLucene(@Nonnull CandidateScan candidateScan,
                                  @Nonnull Index index, @Nonnull QueryComponent filter,
                                  @Nullable KeyExpression sort, boolean sortReverse,
                                  @Nullable KeyExpression commonPrimaryKey) {
        final RecordMetaData metaData = getRecordMetaData();
        final Collection<RecordType> recordTypes = metaData.recordTypesForIndex(index);
        if (recordTypes.size() != 1) {
            return null;
        }

        final FilterSatisfiedMask filterMask = FilterSatisfiedMask.of(filter);

        final KeyExpression rootExp = index.getRootExpression();
        final KeyExpression groupingKey;
        final ScanComparisons groupingComparisons;

        // Getting grouping information from the index key and query filter
        if (rootExp instanceof GroupingKeyExpression) {
            groupingKey = ((GroupingKeyExpression)rootExp).getGroupingSubKey();
            final QueryToKeyMatcher.Match groupingMatch = new QueryToKeyMatcher(filter).matchesCoveringKey(groupingKey, filterMask);
            if (!groupingMatch.getType().equals((QueryToKeyMatcher.MatchType.EQUALITY))) {
                return null;
            }
            if (filterMask.allSatisfied()) {
                // If filter is only group predicates, can skip trying to find non-trivial Lucene scan.
                return null;
            }
            groupingComparisons = new ScanComparisons(groupingMatch.getEqualityComparisons(), Collections.emptySet());
        } else {
            groupingKey = null;
            groupingComparisons = ScanComparisons.EMPTY;
        }

        LucenePlanState state = new LucenePlanState(index, groupingComparisons, filter);
        state.documentFields = LuceneIndexExpressions.getDocumentFieldDerivations(index, metaData);

        QueryComponent queryComponent = state.groupingComparisons.isEmpty() ? state.filter : filterMask.getUnsatisfiedFilter();
        // Special scans like auto-complete cannot be combined with regular queries.
        LuceneScanParameters scanParameters = getSpecialScan(state, filterMask, queryComponent);
        if (scanParameters == null) {
            // Scan by means of normal Lucene search API.
            LuceneQueryClause query = getQueryForFilter(LuceneQueryType.QUERY, state, filter, new ArrayList<>(), filterMask);
            if (query == null) {
                return null;
            }
            if (!getSort(state, sort, sortReverse, commonPrimaryKey, groupingKey)) {
                return null;
            }
            if (!filterMask.allSatisfied() && index.getBooleanOption("tryBitmapValueIndexes", false)) {
                LuceneQueryClause bitmapQuery = LuceneBitmapValueQuery.tryBuild(this,
                        recordTypes.iterator().next(), filterMask, groupingKey);
                if (bitmapQuery != null) {
                    if (query instanceof LuceneBooleanQuery && ((LuceneBooleanQuery)query).getOccur() == BooleanClause.Occur.MUST) {
                        ((LuceneBooleanQuery)query).getChildren().add(bitmapQuery);
                    } else {
                        query = new LuceneBooleanQuery(query.getQueryType(), List.of(query, bitmapQuery), BooleanClause.Occur.MUST);
                    }
                }
            }
            getStoredFields(state);
            LuceneScanQueryParameters.LuceneQueryHighlightParameters highlightParameters = getHighlightParameters(queryComponent);
            scanParameters = new LuceneScanQueryParameters(groupingComparisons, query,
                    state.sort, state.storedFields, state.storedFieldTypes, highlightParameters);
        }

        // Wrap in plan.
        RecordQueryPlan plan = LuceneIndexQueryPlan.of(index.getName(), scanParameters,
                resolveFetchIndexRecords(candidateScan.getPlanContext()), false,
                state.planOrderingKey, state.storedFieldExpressions);
        plan = addTypeFilterIfNeeded(candidateScan, plan, getPossibleTypes(index));
        if (filterMask.allSatisfied()) {
            filterMask.setSatisfied(true);
        }
        return new ScoredPlan(plan, filterMask.getUnsatisfiedFilters(), Collections.emptyList(),
                computeSargedComparisons(plan),  11 - filterMask.getUnsatisfiedFilters().size(),
                state.repeated, false, false, null);
    }

    private static LuceneScanQueryParameters.LuceneQueryHighlightParameters getHighlightParameters(@Nonnull QueryComponent queryComponent) {
        if (queryComponent instanceof LuceneQueryComponent) {
            LuceneQueryComponent luceneQueryComponent = (LuceneQueryComponent)queryComponent;
            return luceneQueryComponent.getLuceneQueryHighlightParameters();
        } else if (queryComponent instanceof NestedField) {
            return getHighlightParameters(((NestedField)queryComponent).getChild());
        } else if (queryComponent instanceof AndOrComponent) {
            for (QueryComponent child : ((AndOrComponent) queryComponent).getChildren()) {
                LuceneScanQueryParameters.LuceneQueryHighlightParameters parameters = getHighlightParameters(child);
                if (parameters != null) {
                    return parameters;
                }
            }
        }
        return null;
    }

    static class LucenePlanState {
        @Nonnull
        final Index index;
        @Nonnull
        final ScanComparisons groupingComparisons;
        @Nonnull
        final QueryComponent filter;
        @Nullable
        Sort sort;
        @Nullable
        List<String> storedFields;
        @Nullable
        List<LuceneIndexExpressions.DocumentFieldType> storedFieldTypes;
        @Nullable
        List<KeyExpression> storedFieldExpressions;
        @Nullable
        PlanOrderingKey planOrderingKey;

        Map<String, LuceneIndexExpressions.DocumentFieldDerivation> documentFields;
        boolean repeated;   // Matching a repeated field may introduce duplicates

        LucenePlanState(@Nonnull final Index index, @Nonnull final ScanComparisons groupingComparisons, @Nonnull final QueryComponent filter) {
            this.index = index;
            this.groupingComparisons = groupingComparisons;
            this.filter = filter;
        }
    }

    @Nullable
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    private LuceneScanParameters getSpecialScan(@Nonnull LucenePlanState state, @Nonnull FilterSatisfiedMask filterMask, @Nonnull QueryComponent queryComponent) {
        QueryComponent component = queryComponent;
        final ImmutableList.Builder<String> prefixComponentsBuilder = ImmutableList.builder();
        // find the prefix of the special scan (if it exists)
        while (component instanceof NestedField) {
            final var nestedField = (NestedField)component;
            prefixComponentsBuilder.add(nestedField.getFieldName());
            component = nestedField.getChild();
        }
        final var prefixComponents = prefixComponentsBuilder.build();
        final String prefix = prefixComponents.isEmpty() ? null : String.join("_", prefixComponents);

        if (!(component instanceof LuceneQueryComponent)) {
            return null;
        }

        final LuceneQueryComponent luceneQueryComponent = (LuceneQueryComponent)component;
        for (String field : luceneQueryComponent.getFields()) {
            final String fullyPrefixedField = prefix == null ? field : prefix + "_" + field;
            if (!validateIndexField(state, fullyPrefixedField)) {
                return null;
            }
        }
        if (luceneQueryComponent.getType() != LuceneQueryType.SPELL_CHECK) {
            return null;
        }

        final LuceneScanParameters scanParameters =
                new LuceneScanSpellCheckParameters(state.groupingComparisons,
                        luceneQueryComponent.getQuery(), luceneQueryComponent.isQueryIsParameter());

        if (queryComponent != state.filter) {
            filterMask = filterMask.getChild(queryComponent);
        }
        filterMask.setSatisfied(true);
        return scanParameters;
    }

    @Nonnull
    private LuceneQueryComponent prefixFieldNames(@Nonnull LuceneQueryComponent luceneQueryComponent, @Nonnull final List<String> prefix) {
        if (prefix.isEmpty()) {
            return luceneQueryComponent;
        }
        final var name = String.join("_", prefix);
        final var newFieldNames = luceneQueryComponent.getFields().stream().map(field -> name + "_" + field).collect(Collectors.toList());
        final var newExplicitFieldNames = luceneQueryComponent.getExplicitFieldNames() == null
                ? null : luceneQueryComponent.getExplicitFieldNames().stream().map(field -> name + "_" + field).collect(Collectors.toSet());
        return luceneQueryComponent.withNewFields(newFieldNames, newExplicitFieldNames);
    }

    @Nullable
    private LuceneQueryClause getQueryForFilter(@Nonnull final LuceneQueryType queryType,
                                                @Nonnull final LucenePlanState state,
                                                @Nonnull final QueryComponent filter,
                                                @Nonnull final List<String> parentFieldPath,
                                                @Nullable final FilterSatisfiedMask filterMask) {
        if (filter instanceof LuceneQueryComponent) {
            return getQueryForLuceneComponent(state, (LuceneQueryComponent)filter, parentFieldPath, filterMask);
        } else if (filter instanceof AndOrComponent) {
            return getQueryForAndOr(queryType, state, (AndOrComponent) filter, parentFieldPath, filterMask);
        } else if (filter instanceof NotComponent) {
            return getQueryForNot(queryType, state, (NotComponent) filter, parentFieldPath, filterMask);
        } else if (filter instanceof FieldWithComparison) {
            return getQueryForFieldWithComparison(queryType, state, (FieldWithComparison) filter, parentFieldPath, filterMask);
        } else if (filter instanceof OneOfThemWithComponent) {
            return getQueryForNestedField(queryType, state, (OneOfThemWithComponent)filter, true, parentFieldPath, filterMask);
        } else if (filter instanceof NestedField) {
            return getQueryForNestedField(queryType, state, (NestedField) filter, false, parentFieldPath, filterMask);
        }
        return null;
    }

    @Nullable
    private LuceneQueryClause getQueryForLuceneComponent(@Nonnull LucenePlanState state, @Nonnull LuceneQueryComponent filter,
                                                         @Nonnull List<String> parentFieldPath, @Nullable FilterSatisfiedMask filterMask) {
        filter = prefixFieldNames(filter, parentFieldPath);

        for (String field : filter.getFields()) {
            if (!validateIndexField(state, field)) {
                return null;
            }
        }
        if (filterMask != null) {
            filterMask.setSatisfied(true);
        }

        switch (filter.getType()) {
            case AUTO_COMPLETE:
                final var resolvedFields = filter.getExplicitFieldNames() == null
                                           ? filter.getFields()
                                           : filter.getExplicitFieldNames();
                return new LuceneAutoCompleteQueryClause(filter.getQuery(), filter.isQueryIsParameter(), resolvedFields);
            case QUERY:
            case QUERY_HIGHLIGHT:
            case SPELL_CHECK:
                if (filter.isMultiFieldSearch()) {
                    return new LuceneQueryMultiFieldSearchClause(filter.getType(), filter.getQuery(), filter.isQueryIsParameter());
                } else {
                    return new LuceneQuerySearchClause(filter.getType(), filter.getQuery(), filter.isQueryIsParameter());
                }
            default:
                throw new RecordCoreException("unknown type of lucene query component");
        }
    }

    @Nullable
    @SuppressWarnings({"java:S3776", "PMD.CompareObjectsWithEquals"})
    private LuceneQueryClause getQueryForAndOr(@Nonnull final LuceneQueryType queryType, @Nonnull final LucenePlanState state,
                                               @Nonnull final AndOrComponent filter, @Nonnull final List<String> parentFieldPath,
                                               @Nullable final FilterSatisfiedMask filterMask) {
        final Iterator<FilterSatisfiedMask> subFilterMasks = filterMask != null ? filterMask.getChildren().iterator() : null;
        final List<QueryComponent> filters = filter.getChildren();
        final List<LuceneQueryClause> childClauses = new ArrayList<>(filters.size());
        final List<LuceneQueryClause> negatedChildren = new ArrayList<>(0);
        final BooleanClause.Occur occur = filter instanceof OrComponent ? BooleanClause.Occur.SHOULD : BooleanClause.Occur.MUST;
        for (QueryComponent subFilter : filters) {
            final FilterSatisfiedMask childMask = subFilterMasks != null ? subFilterMasks.next() : null;
            LuceneQueryClause childClause = getQueryForFilter(queryType, state, subFilter, parentFieldPath, childMask);
            if (childClause == null) {
                if (filter == state.filter && filter instanceof AndComponent) {
                    continue;    // Partially unsatisfied at top-level is okay.
                }
                return null;
            }
            if (childMask != null) {
                childMask.setSatisfied(true);
            }
            if (childClause instanceof LuceneBooleanQuery && ((LuceneBooleanQuery)childClause).getOccur() == occur) {
                childClauses.addAll(((LuceneBooleanQuery)childClause).getChildren());
                if (childClause instanceof LuceneNotQuery) {
                    negatedChildren.addAll(((LuceneNotQuery)childClause).getNegatedChildren());
                }
            } else {
                childClauses.add(childClause);
            }
        }
        if (filterMask != null && filterMask.getUnsatisfiedFilters().isEmpty()) {
            filterMask.setSatisfied(true);
        }
        if (!negatedChildren.isEmpty()) {
            return new LuceneNotQuery(queryType, childClauses, negatedChildren);
        }
        // Don't do Lucene scan if none are satisfied, though.
        if (childClauses.isEmpty()) {
            return null;
        }
        return new LuceneBooleanQuery(queryType, childClauses, occur);
    }

    @Nullable
    private LuceneQueryClause getQueryForNot(@Nonnull final LuceneQueryType queryType,
                                             @Nonnull final LucenePlanState state,
                                             @Nonnull final NotComponent filter,
                                             @Nonnull final List<String> parentFieldPath,
                                             @Nullable final FilterSatisfiedMask filterMask) {
        final LuceneQueryClause childClause = getQueryForFilter(queryType, state, filter.getChild(), parentFieldPath,
                filterMask == null ? null : filterMask.getChildren().get(0));
        if (childClause == null) {
            return null;
        }
        if (filterMask != null) {
            filterMask.setSatisfied(true);
        }
        return negate(childClause);
    }

    @Nonnull
    private static LuceneQueryClause negate(@Nonnull LuceneQueryClause clause) {
        if (clause instanceof LuceneBooleanQuery) {
            final LuceneBooleanQuery booleanQuery = (LuceneBooleanQuery)clause;
            switch (booleanQuery.getOccur()) {
                case MUST:
                    List<LuceneQueryClause> clauses = new ArrayList<>();
                    for (LuceneQueryClause child : booleanQuery.getChildren()) {
                        clauses.add(negate(child));
                    }
                    if (clause instanceof LuceneNotQuery) {
                        LuceneNotQuery notQuery = (LuceneNotQuery)clause;
                        if (clauses.isEmpty() && notQuery.getNegatedChildren().size() == 1) {
                            return notQuery.getNegatedChildren().get(0);
                        }
                        clauses.addAll(notQuery.getNegatedChildren());
                    }
                    return new LuceneBooleanQuery(clause.getQueryType(), clauses, BooleanClause.Occur.SHOULD);
                case SHOULD:
                    List<LuceneQueryClause> positive = new ArrayList<>();
                    List<LuceneQueryClause> negative = new ArrayList<>();
                    for (LuceneQueryClause child : booleanQuery.getChildren()) {
                        if (child instanceof LuceneBooleanQuery) {
                            positive.add(negate(child));
                        } else {
                            negative.add(child);
                        }
                    }
                    if (negative.isEmpty()) {
                        return new LuceneBooleanQuery(clause.getQueryType(), positive, BooleanClause.Occur.MUST);
                    } else {
                        return new LuceneNotQuery(clause.getQueryType(), positive, negative);
                    }
                default:
                    throw new RecordCoreException("Unsupported boolean query occur: " + booleanQuery);
            }
        }
        return new LuceneNotQuery(clause.getQueryType(), clause);
    }

    @Nullable
    private LuceneQueryClause getQueryForFieldWithComparison(@Nonnull final LuceneQueryType queryType,
                                                             @Nonnull final LucenePlanState state,
                                                             @Nonnull final FieldWithComparison filter,
                                                             @Nonnull final List<String> fieldPath,
                                                             @Nullable final FilterSatisfiedMask filterSatisfiedMask) {
        if (filterSatisfiedMask != null && filterSatisfiedMask.isSatisfied()) {
            return null;        // Already done as part of group comparisons
        }
        fieldPath.add(filter.getFieldName());
        LuceneIndexExpressions.DocumentFieldDerivation fieldDerivation = findIndexField(state, fieldPath);
        fieldPath.remove(fieldPath.size() - 1);
        if (fieldDerivation == null) {
            return null;
        }
        if (filterSatisfiedMask != null) {
            filterSatisfiedMask.setSatisfied(true);
        }
        try {
            // Lucene map support is unique in the sense that the field name is concatenated with the evaluated original
            // map key. The fact that we used lucene_field_name function allowed us to determine the correct way to substitute
            // the metadata field name with the Lucene field name in the document (see fieldDerivation.isFieldNameOverride()).
            // We currently support equality on the key ("map-has-key", ignoring the map value, to be treated as a wildcard). In the future
            // we should also support equality on both key and value ("map-has-entry")
            // This is where we can tell whether the query is for map-has-key (only one value) or map-has-entry (two values)
            // TODO: detect the map-has-entry and throw an exception

            return LuceneQueryFieldComparisonClause.create(queryType, fieldDerivation.getDocumentField(),
                    fieldDerivation.getType(), fieldDerivation.isFieldNameOverride(), fieldDerivation.getNamedFieldSuffix(), filter.getComparison());
        } catch (RecordCoreException ex) {
            if (logger.isDebugEnabled()) {
                logger.debug("no query for comparison " + filter, ex);
            }
            return null;
        }
    }

    @Nullable
    private LuceneQueryClause getQueryForNestedField(@Nonnull final LuceneQueryType queryType,
                                                     @Nonnull final LucenePlanState state,
                                                     @Nonnull final BaseField filter,
                                                     final boolean repeated,
                                                     @Nonnull final List<String> fieldPath,
                                                     @Nullable final FilterSatisfiedMask mask) {
        QueryComponent child = ((ComponentWithSingleChild)filter).getChild();
        fieldPath.add(filter.getFieldName());
        LuceneQueryClause comparison = getQueryForFilter(queryType, state, child, fieldPath, (mask != null) ? mask.getChild(child) : null);
        fieldPath.remove(fieldPath.size() - 1);
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

    @Nullable
    private LuceneIndexExpressions.DocumentFieldDerivation findIndexField(@Nonnull LucenePlanState state, @Nonnull List<String> fieldPath) {
        if (fieldPath.size() == 1) {
            // Quickly check simple case by name to save looking through all document fields.
            final LuceneIndexExpressions.DocumentFieldDerivation fieldDerivation = state.documentFields.get(fieldPath.get(0));
            if (fieldDerivation != null && fieldDerivation.getRecordFieldPath().equals(fieldPath)) {
                return fieldDerivation;
            }
        }
        for (LuceneIndexExpressions.DocumentFieldDerivation fieldDerivation : state.documentFields.values()) {
            if (fieldDerivation.getRecordFieldPath().equals(fieldPath)) {
                return fieldDerivation;
            }
        }
        return null;
    }

    private boolean validateIndexField(@Nonnull LucenePlanState state, @Nonnull String field) {
        // Can only check that the field name given corresponds to some top-level branch to an indexed field.
        for (LuceneIndexExpressions.DocumentFieldDerivation fieldDerivation : state.documentFields.values()) {
            if (fieldMatchesPath(fieldDerivation, field)) {
                return true;
            }
        }
        return false;
    }

    private boolean getSort(@Nonnull LucenePlanState state,
                            @Nullable KeyExpression sort, boolean sortReverse,
                            @Nullable KeyExpression commonPrimaryKey, @Nullable KeyExpression groupingKey) {
        final List<KeyExpression> sorts = new ArrayList<>();
        if (sort == null) {
            // TODO: This isn't really right for a plan ordering key.
            //  score depends on the query and so likely isn't the same for the same document.
            //  doc is stable for the same IndexReader, that is, snapshot of a particular index.
            sorts.add(FunctionKeyExpression.create(LuceneFunctionNames.LUCENE_SORT_BY_RELEVANCE, EmptyKeyExpression.EMPTY));
            if (commonPrimaryKey != null) {
                sorts.addAll(commonPrimaryKey.normalizeKeyForPositions());
            }
        } else {
            sorts.addAll(sort.normalizeKeyForPositions());
        }
        if (groupingKey != null) {
            sorts.removeAll(groupingKey.normalizeKeyForPositions());
        }
        final List<SortField> sortFields = new ArrayList<>(sorts.size());
        List<KeyExpression> primaryKeys = null;
        int primaryKeyPosition = 0;
        boolean hasBuiltInSort = false;
        for (KeyExpression sortItem : sorts) {
            if (primaryKeys != null && primaryKeyPosition < primaryKeys.size()) {
                // Once we have started with the primary key, we need to finish all its fields in order.
                if (!sortItem.equals(primaryKeys.get(primaryKeyPosition))) {
                    return false;
                }
                primaryKeyPosition++;
                continue;
            }
            SortField sortField = null;
            if (sortItem instanceof LuceneFunctionKeyExpression.LuceneSortBy) {
                sortField = ((LuceneFunctionKeyExpression.LuceneSortBy)sortItem).isRelevance() ? SortField.FIELD_SCORE : SortField.FIELD_DOC;
                hasBuiltInSort = true;
            } else {
                for (LuceneIndexExpressions.DocumentFieldDerivation documentField : state.documentFields.values()) {
                    if (documentField.isSorted() && recordFieldPathMatches(sortItem, documentField.getRecordFieldPath())) {
                        final SortField.Type type;
                        switch (documentField.getType()) {
                            case STRING:
                            case BOOLEAN:
                                type = SortField.Type.STRING;
                                break;
                            case INT:
                                type = SortField.Type.INT;
                                break;
                            case LONG:
                                type = SortField.Type.LONG;
                                break;
                            case DOUBLE:
                                type = SortField.Type.DOUBLE;
                                break;
                            default:
                                throw new RecordCoreException("Unsupported document field type for sorting: " + documentField.getType() + ":" + documentField.getDocumentField());
                        }
                        sortField = new SortField(documentField.getDocumentField(), type, sortReverse);
                        break;
                    }
                }
            }
            if (sortField != null) {
                sortFields.add(sortField);
            } else {
                if (primaryKeys == null && commonPrimaryKey != null) {
                    primaryKeys = commonPrimaryKey.normalizeKeyForPositions();
                    if (groupingKey != null) {
                        primaryKeys.removeAll(groupingKey.normalizeKeyForPositions());
                    }
                    if (sortItem.equals(primaryKeys.get(primaryKeyPosition))) {
                        // First part of primary key, sort by internal field (binary encoding of entire pkey).
                        // Note that is it okay if the entire primary key is not all matched later; the query sort will just be overspecified.
                        sortFields.add(new SortField(LuceneIndexMaintainer.PRIMARY_KEY_SEARCH_NAME, SortField.Type.STRING, sortReverse));
                        primaryKeyPosition++;
                        continue;
                    }
                }
                return false;
            }
        }
        state.sort = new Sort(sortFields.toArray(new SortField[0]));
        // Unlike normal indexes, entries are not additionally ordered by the primary key,
        // except in the case where the query asked for a prefix of it and we used the whole internal field.
        List<KeyExpression> orderingKeys = new ArrayList<>(sorts.size());
        int prefixSize = 0;
        if (groupingKey != null) {
            orderingKeys.addAll(groupingKey.normalizeKeyForPositions());
            prefixSize = orderingKeys.size();
        }
        orderingKeys.addAll(sorts);
        int primaryKeyStart = orderingKeys.size();
        if (primaryKeys != null && primaryKeyPosition < primaryKeys.size()) {
            orderingKeys.addAll(primaryKeys.subList(primaryKeyPosition, primaryKeys.size()));
        }
        if (!hasBuiltInSort) {
            // These are only sometimes compatible from one search scan to another; for now be conservative and don't claim anything.
            state.planOrderingKey = new PlanOrderingKey(orderingKeys, prefixSize, primaryKeyStart, orderingKeys.size());
        }
        return true;
    }

    private void getStoredFields(@Nonnull LucenePlanState state) {
        final List<KeyExpression> fields = state.index.getRootExpression().normalizeKeyForPositions();
        for (LuceneIndexExpressions.DocumentFieldDerivation documentField : state.documentFields.values()) {
            if (documentField.isStored()) {
                if (state.storedFields == null) {
                    state.storedFields = new ArrayList<>(Collections.nCopies(fields.size(), null));
                    state.storedFieldTypes = new ArrayList<>(Collections.nCopies(fields.size(), null));
                    state.storedFieldExpressions = new ArrayList<>();
                }
                for (int i = 0; i < fields.size(); i++) {
                    final KeyExpression fieldExpression = fields.get(i);
                    if (recordFieldPathMatches(fieldExpression, documentField.getRecordFieldPath())) {
                        state.storedFields.set(i, documentField.getDocumentField());
                        state.storedFieldTypes.set(i, documentField.getType());
                        state.storedFieldExpressions.add(fieldExpression);
                        break;
                    }
                }
            }
        }
    }

    private boolean recordFieldPathMatches(@Nonnull KeyExpression keyExpression, @Nonnull List<String> recordFieldPath) {
        int i = 0;
        while (true) {
            if (keyExpression instanceof FieldKeyExpression) {
                return i == recordFieldPath.size() - 1 && ((FieldKeyExpression)keyExpression).getFieldName().equals(recordFieldPath.get(i));
            } else if (keyExpression instanceof NestingKeyExpression) {
                if (i < recordFieldPath.size() && ((NestingKeyExpression)keyExpression).getParent().getFieldName().equals(recordFieldPath.get(i))) {
                    keyExpression = ((NestingKeyExpression)keyExpression).getChild();
                    i++;
                } else {
                    return false;
                }
            } else if (keyExpression instanceof LuceneFunctionKeyExpression.LuceneSorted) {
                keyExpression = ((LuceneFunctionKeyExpression.LuceneSorted)keyExpression).getSortedExpression();
            } else if (keyExpression instanceof LuceneFunctionKeyExpression.LuceneStored) {
                keyExpression = ((LuceneFunctionKeyExpression.LuceneStored)keyExpression).getStoredExpression();
            } else {
                return false;
            }
        }
    }

    static boolean fieldMatchesPath(@Nonnull LuceneIndexExpressions.DocumentFieldDerivation fieldDerivation,
                                    @Nonnull String field) {
        StringBuilder path = null;
        for (String pathElement : fieldDerivation.getRecordFieldPath()) {
            if (path == null) {
                path = new StringBuilder(pathElement);
            } else {
                path.append("_").append(pathElement);
            }
            if (path.length() > field.length()) {
                break;
            }
            if (path.toString().equals(field)) {
                return true;
            }
        }
        return false;
    }
}
