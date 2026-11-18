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
import com.apple.foundationdb.record.provider.foundationdb.FormatVersion;
import com.apple.foundationdb.record.query.plan.QueryPlanConstraint;
import com.apple.foundationdb.relational.api.EmbeddedRelationalArray;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.api.RelationalPreparedStatement;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.metadata.DataType;
import com.apple.foundationdb.relational.api.metrics.RelationalMetric;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalExtension;
import com.apple.foundationdb.relational.recordlayer.ddl.NoOpMetadataOperationsFactory;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerColumn;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerSchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerTable;
import com.apple.foundationdb.relational.recordlayer.query.cache.NoOpMetricCollector;
import com.apple.foundationdb.relational.utils.ConnectionUtils;
import com.apple.foundationdb.relational.utils.Ddl;
import com.codahale.metrics.MetricFilter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import javax.annotation.Nonnull;
import java.net.URI;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Additional tests for {@link AstNormalizer}, focused on named parameters that are declared but carry no value, and on
 * queries that share a plan cache entry because parameter names are not part of the canonical query string.
 */
@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
class AstNormalizerAutoQATest {

    @Nonnull
    private static final PlannerConfiguration plannerConfiguration = PlannerConfiguration.ofAllAvailableIndexes();

    @Nonnull
    private static final String RANGE_TEMPLATE = "CREATE TABLE t1(id bigint, col1 bigint, PRIMARY KEY(id))";

    @Nonnull
    private static final String STORED_SAME_PARAMETER_TEMPLATE = RANGE_TEMPLATE +
            " CREATE STORED QUERY in_range(p bigint)" +
            "   PREPARE FOR (p IS NOT NULL)" +
            " AS select id from t1 where id >= p and id <= p";

    @RegisterExtension
    @Order(0)
    public final EmbeddedRelationalExtension relationalExtension = new EmbeddedRelationalExtension();

    @BeforeEach
    void clearMetrics() {
        relationalExtension.getMetricRegistry().removeMatching(MetricFilter.ALL);
    }

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

    @Test
    void storedQueryWithRepeatedParameterServesClientWithTwoDistinctNames() throws Exception {
        // a plan warmed value-free for `id >= p and id <= p` must bind each place on its own for `?lo` / `?hi`
        final String dbUri = "/TEST/AQA_SQ_SAME_PARAM_DB";
        try (var ddl = Ddl.builder().database(URI.create(dbUri)).relationalExtension(relationalExtension)
                .schemaTemplate(STORED_SAME_PARAMETER_TEMPLATE).build()) {
            final var connection = ddl.setSchemaAndGetConnection();
            insertRange(connection);
            final var connectionUtils = new ConnectionUtils(relationalExtension.getDriver(FormatVersion.getDefaultFormatVersion()));
            final List<Long> ids = new ArrayList<>();
            connectionUtils.runAgainstConnection(dbUri, connection.getSchema(), c -> {
                try (var ps = c.prepareStatement("select id from t1 where id >= ?lo and id <= ?hi")) {
                    ps.setLong("lo", 2L);
                    ps.setLong("hi", 4L);
                    ids.addAll(queryIds(ps));
                }
            });
            assertThat(ids).containsExactly(2L, 3L, 4L);
            assertThat(relationalExtension.getMetricRegistry()
                    .counter(RelationalMetric.RelationalCount.PLAN_CACHE_TERTIARY_HIT.title()).getCount()).isEqualTo(1L);
        }
    }

    @Test
    void storedQueryWithRepeatedParameterServesClientWithUnnamedParameters() throws Exception {
        // the stored query docs say a plain `?` at the same place reuses the warmed plan; each `?` keeps its own value
        final String dbUri = "/TEST/AQA_SQ_SAME_PARAM_UNNAMED_DB";
        try (var ddl = Ddl.builder().database(URI.create(dbUri)).relationalExtension(relationalExtension)
                .schemaTemplate(STORED_SAME_PARAMETER_TEMPLATE).build()) {
            final var connection = ddl.setSchemaAndGetConnection();
            insertRange(connection);
            final var connectionUtils = new ConnectionUtils(relationalExtension.getDriver(FormatVersion.getDefaultFormatVersion()));
            final List<Long> ids = new ArrayList<>();
            connectionUtils.runAgainstConnection(dbUri, connection.getSchema(), c -> {
                try (var ps = c.prepareStatement("select id from t1 where id >= ? and id <= ?")) {
                    ps.setLong(1, 3L);
                    ps.setLong(2, 5L);
                    ids.addAll(queryIds(ps));
                }
            });
            assertThat(ids).containsExactly(3L, 4L, 5L);
            assertThat(relationalExtension.getMetricRegistry()
                    .counter(RelationalMetric.RelationalCount.PLAN_CACHE_TERTIARY_HIT.title()).getCount()).isEqualTo(1L);
        }
    }

    @Test
    void namedAndUnnamedParametersInSwappedPlacesBindByPosition() throws Exception {
        // `? .. ?hi` and `?lo .. ?` share a canonical string; each run must still use its own values
        try (var ddl = Ddl.builder().database(URI.create("/TEST/AQA_MIXED_PARAMS_DB")).relationalExtension(relationalExtension)
                .schemaTemplate(RANGE_TEMPLATE).build()) {
            final var connection = ddl.setSchemaAndGetConnection();
            insertRange(connection);
            try (var ps = connection.prepareStatement("select id from t1 where id > ? and id < ?hi")) {
                ps.setLong(1, 1L);
                ps.setLong("hi", 4L);
                assertThat(queryIds(ps)).containsExactly(2L, 3L);
            }
            try (var ps = connection.prepareStatement("select id from t1 where id > ?lo and id < ?")) {
                ps.setLong("lo", 2L);
                ps.setLong(1, 5L);
                assertThat(queryIds(ps)).containsExactly(3L, 4L);
            }
            assertThat(relationalExtension.getMetricRegistry()
                    .counter(RelationalMetric.RelationalCount.PLAN_CACHE_TERTIARY_HIT.title()).getCount()).isEqualTo(1L);
        }
    }

    @Test
    void temporaryFunctionWithSwappedParameterNamesUsesItsOwnValues() throws Exception {
        // two temp functions that differ only in which name sits where share a key; values must follow position
        try (var ddl = Ddl.builder().database(URI.create("/TEST/AQA_TF_SWAPPED_DB")).relationalExtension(relationalExtension)
                .schemaTemplate(RANGE_TEMPLATE).build()) {
            final var connection = ddl.setSchemaAndGetConnection();
            insertRange(connection);
            assertThat(idsThroughFunction(connection, "id > ?a and id < ?b", 1L, 4L)).containsExactly(2L, 3L);
            assertThat(idsThroughFunction(connection, "id > ?b and id < ?a", 5L, 2L)).containsExactly(3L, 4L);
        }
    }

    @Nonnull
    private static List<Long> idsThroughFunction(@Nonnull final RelationalConnection connection,
                                                 @Nonnull final String predicate,
                                                 final long a, final long b) throws SQLException {
        connection.setAutoCommit(false);
        try {
            try (var ps = connection.prepareStatement("CREATE TEMPORARY FUNCTION f_range() ON COMMIT DROP FUNCTION AS " +
                    "SELECT id FROM t1 WHERE " + predicate)) {
                ps.setLong("a", a);
                ps.setLong("b", b);
                ps.execute();
            }
            try (var ps = connection.prepareStatement("SELECT id FROM f_range()")) {
                return queryIds(ps);
            }
        } finally {
            connection.rollback();
            connection.setAutoCommit(true);
        }
    }

    private static void insertRange(@Nonnull final RelationalConnection connection) throws SQLException {
        try (var statement = connection.createStatement()) {
            statement.execute("INSERT INTO t1 VALUES (1, 1), (2, 2), (3, 3), (4, 4), (5, 5)");
        }
    }

    @Nonnull
    private static List<Long> queryIds(@Nonnull final RelationalPreparedStatement ps) throws SQLException {
        final List<Long> ids = new ArrayList<>();
        try (RelationalResultSet resultSet = ps.executeQuery()) {
            while (resultSet.next()) {
                ids.add(resultSet.getLong("ID"));
            }
        }
        return ids;
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
