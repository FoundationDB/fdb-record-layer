/*
 * LuceneScanQueryParametersTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.cascades.explain.Attribute;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraphVisitor;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryFetchFromPartialRecordPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.lucene.search.Sort;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;

import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.metadata.Key.Expressions.function;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests of {@link LuceneScanQueryParameters} that do not need a database.
 */
class LuceneScanQueryParametersTest {
    private static final String INCLUDE_SCORE_VALUE_DETAIL = "includeScoreValue";

    private static final LuceneQueryClause CLAUSE = new LuceneQueryMultiFieldSearchClause(LuceneQueryType.QUERY, "vision", false);

    @Test
    void constructorTests() {
        final LuceneScanQueryParameters withoutQueryOptions = new LuceneScanQueryParameters(ScanComparisons.EMPTY, CLAUSE);
        final LuceneScanQueryParameters withQueryOptions = new LuceneScanQueryParameters(ScanComparisons.EMPTY, CLAUSE, null, null, null, null);
        final LuceneScanQueryParameters explicitlyWithoutScore = createParameters(null, null, null, false);
        final LuceneScanQueryParameters explicitlyWithScore = createParameters(null, null, null, true);

        assertThat(withoutQueryOptions.isIncludeScoreValue()).isFalse();
        assertThat(withQueryOptions.isIncludeScoreValue()).isFalse();

        assertThat(explicitlyWithScore.isIncludeScoreValue()).isTrue();
        assertThat(explicitlyWithoutScore.isIncludeScoreValue()).isFalse();

        for (final LuceneScanQueryParameters withoutScore : List.of(withoutQueryOptions, withQueryOptions)) {
            assertThat(withoutScore).isEqualTo(explicitlyWithoutScore).hasSameHashCodeAs(explicitlyWithoutScore);
            assertThat(withoutScore.planHash(PlanHashable.CURRENT_FOR_CONTINUATION))
                    .isEqualTo(explicitlyWithoutScore.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
            assertThat(withoutScore).isNotEqualTo(explicitlyWithScore);
            assertThat(withoutScore.planHash(PlanHashable.CURRENT_FOR_CONTINUATION))
                    .isNotEqualTo(explicitlyWithScore.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        }
    }

    @Test
    void getPlannerGraphDetailsWithoutOptionsListsOnlyScanAndQuery() {
        final var parameters = createParameters(null, null, null, false);

        assertThat(graphDetails(parameters)).containsExactly("scan type: {{scanType}}", "search: {{search}}");
        assertThat(graphAttributes(parameters)).containsOnlyKeys("scanType", "search");
    }

    @Test
    void getPlannerGraphDetailsWithIncludeScoreValueAddsDetailOnly() {
        final var withoutScore = createParameters(null, null, null, false);
        final var withScore = createParameters(null, null, null, true);

        assertThat(graphDetails(withScore))
                .containsExactlyElementsOf(ImmutableList.<String>builder().addAll(graphDetails(withoutScore)).add(INCLUDE_SCORE_VALUE_DETAIL).build());
    }

    @Test
    void plannerGraphOfIndexPlanShowsIncludeScoreValueOnlyWhenIncluded() {
        final String withoutScoreGraph = PlannerGraphVisitor.internalGraphicalExplain(indexPlan(createParameters(null, null, null, false)));
        final String withScoreGraph = PlannerGraphVisitor.internalGraphicalExplain(indexPlan(createParameters(null, null, null, true)));

        assertThat(withoutScoreGraph).contains("vision").doesNotContain(INCLUDE_SCORE_VALUE_DETAIL);
        assertThat(withScoreGraph).contains("vision").contains(INCLUDE_SCORE_VALUE_DETAIL);
    }

    @Nonnull
    private LuceneScanQueryParameters createParameters(@Nullable final Sort sort,
                                                       @Nullable final List<String> storedFields,
                                                       @Nullable final List<LuceneIndexExpressions.DocumentFieldType> storedFieldTypes,
                                                       final boolean includeScoreValue) {
        return new LuceneScanQueryParameters(ScanComparisons.EMPTY, CLAUSE, sort, storedFields, storedFieldTypes, null, includeScoreValue);
    }

    @Nonnull
    private ImmutableList<String> graphDetails(@Nonnull final LuceneScanQueryParameters parameters) {
        final ImmutableList.Builder<String> detailsBuilder = ImmutableList.builder();
        parameters.getPlannerGraphDetails(detailsBuilder, ImmutableMap.builder());
        return detailsBuilder.build();
    }

    @Nonnull
    private ImmutableMap<String, Attribute> graphAttributes(@Nonnull final LuceneScanQueryParameters parameters) {
        final ImmutableMap.Builder<String, Attribute> attributeMapBuilder = ImmutableMap.builder();
        parameters.getPlannerGraphDetails(ImmutableList.builder(), attributeMapBuilder);
        return attributeMapBuilder.build();
    }

    @Nonnull
    private RecordQueryPlan indexPlan(@Nonnull final LuceneScanQueryParameters parameters) {
        return LuceneIndexQueryPlan.of("lucene_index",
                parameters,
                RecordQueryFetchFromPartialRecordPlan.FetchIndexRecords.PRIMARY_KEY,
                false,
                null,
                List.of(function(LuceneFunctionNames.LUCENE_TEXT, field("text"))));
    }
}
