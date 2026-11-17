/*
 * AstNormalizerAutoQATest.java
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

package com.apple.foundationdb.relational.recordlayer.query;

import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.query.plan.QueryPlanConstraint;
import com.apple.foundationdb.relational.api.EmbeddedRelationalArray;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.metadata.DataType;
import com.apple.foundationdb.relational.recordlayer.ddl.NoOpMetadataOperationsFactory;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerColumn;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerSchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerTable;
import com.apple.foundationdb.relational.recordlayer.query.cache.NoOpMetricCollector;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Additional tests for {@link AstNormalizer}, focused on named parameters that are declared but carry no value.
 */
@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
class AstNormalizerAutoQATest {

    @Nonnull
    private static final PlannerConfiguration plannerConfiguration = PlannerConfiguration.ofAllAvailableIndexes();

    @Test
    void normalizeAstValueFreeParameterInConstantInListSharesCacheKeyWithBoundValue() throws Exception {
        // a declared-but-unbound parameter inside an all-constant IN list must canonicalize like a bound one
        final var query = "select title from books where id in (?param_a, 2)";
        final var valueFree = normalize(query, PreparedParams.empty().withDeclaredTypeParams(Map.of("param_a", "BIGINT")));
        final var bound = normalize(query, PreparedParams.ofNamed(Map.of("param_a", 1L)));
        assertThat(valueFree.getQueryCacheKey()).isEqualTo(bound.getQueryCacheKey());
    }

    @Test
    void normalizeAstValueFreeArrayParameterInListSharesCacheKeyWithBoundArray() throws Exception {
        // a declared-but-unbound parameter used as the whole IN list must canonicalize like a bound array
        final var query = "select title from books where id in ?param_a";
        final var valueFree = normalize(query, PreparedParams.empty().withDeclaredTypeParams(Map.of("param_a", "BIGINT ARRAY")));
        final var bound = normalize(query, PreparedParams.ofNamed(Map.of("param_a",
                EmbeddedRelationalArray.newBuilder().addAll(1L, 2L).build())));
        assertThat(valueFree.getQueryCacheKey()).isEqualTo(bound.getQueryCacheKey());
    }

    @Test
    void valueFreeStringParameterConstrainsAsABoundValueDoes() throws Exception {
        // a declared STRING parameter must warm the same constraint as a bound non-null string
        final var query = "select id from books where title = ?param_a";
        final var warmed = planConstraint(query, PreparedParams.empty().withDeclaredTypeParams(Map.of("param_a", "STRING")));
        final var fromValue = planConstraint(query, PreparedParams.ofNamed(Map.of("param_a", "abc")));
        assertThat(warmed).isEqualTo(fromValue);
    }

    @Nonnull
    private static AstNormalizer.NormalizationResult normalize(@Nonnull final String query,
                                                               @Nonnull final PreparedParams preparedParams) throws RelationalException {
        return AstNormalizer.normalizeAst(booksTemplate(), QueryParser.parse(query), PreparedParams.copyOf(preparedParams),
                plannerConfiguration, false, PlanHashable.PlanHashMode.VC0, query, QueryEvaluationConstants.empty());
    }

    @Nonnull
    private static QueryPlanConstraint planConstraint(@Nonnull final String query,
                                                      @Nonnull final PreparedParams preparedParams) throws Exception {
        return PlanGenerator.create(
                        booksTemplate(),
                        booksTemplate().toRecordMetadata(),
                        NoOpMetadataOperationsFactory.INSTANCE,
                        NoOpMetricCollector.INSTANCE,
                        Options.NONE,
                        preparedParams)
                .getPlan(query)
                .getConstraint();
    }

    @Nonnull
    private static RecordLayerSchemaTemplate booksTemplate() {
        return RecordLayerSchemaTemplate.newBuilder()
                .setName("BOOKS_TEMPLATE")
                .setVersion(1)
                .addTable(RecordLayerTable.newBuilder(false)
                        .setName("BOOKS")
                        .addColumn(RecordLayerColumn.newBuilder()
                                .setName("ID")
                                .setDataType(DataType.Primitives.NULLABLE_LONG.type())
                                .build())
                        .addColumn(RecordLayerColumn.newBuilder()
                                .setName("TITLE")
                                .setDataType(DataType.Primitives.NULLABLE_STRING.type())
                                .build())
                        .addPrimaryKeyPart(List.of("ID"))
                        .build())
                .build();
    }
}
