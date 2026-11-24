/*
 * SqlFunctionsConcurrencyTest.java
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

package com.apple.foundationdb.relational.recordlayer;

import com.apple.foundationdb.record.RecordStoreState;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.combinatorics.TopologicalSort;
import com.apple.foundationdb.record.query.plan.cascades.CallSiteArguments;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerInvokedRoutine;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerSchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.query.PlanGenerator;
import com.apple.foundationdb.relational.recordlayer.query.PreparedParams;
import com.apple.foundationdb.relational.recordlayer.query.cache.NoOpMetricCollector;
import com.apple.foundationdb.relational.utils.SchemaTemplateRule;
import com.apple.foundationdb.relational.utils.SimpleDatabaseRule;
import com.google.common.collect.Sets;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.apple.foundationdb.record.query.plan.cascades.properties.ReferencesAndDependenciesProperty.referencesAndDependencies;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests planning statements that call SQL functions or read from views from several threads at the same time. The
 * compiled functions of a schema template are shared by every planning that uses the template, so plannings must not
 * share any planner state through them.
 */
class SqlFunctionsConcurrencyTest {
    @Nonnull
    private static final String SCHEMA_TEMPLATE =
            """
            CREATE TABLE t1 (col1 BIGINT, col2 STRING, col3 INTEGER, PRIMARY KEY (col1))
            CREATE FUNCTION f1 (IN a BIGINT, IN b STRING)
              AS SELECT col1, col2 FROM t1 WHERE col1 < a AND col2 = b
            CREATE FUNCTION f2 (IN k BIGINT)
              AS SELECT col1, col2 FROM f1 (102, 'b')
            CREATE FUNCTION f3 (IN x BIGINT, IN y STRING)
              AS SELECT a.col1 AS col1a, a.col2 AS col2a, b.col1 AS col1b, b.col2 AS col2b FROM f2(x) AS a, f1(x, y) AS b
            CREATE FUNCTION f4 (IN x BIGINT, IN y STRING)
              AS SELECT col1a + col1b AS s, col2a, col2b FROM f3(x, y)
            CREATE FUNCTION f0 ()
              AS SELECT col1, col2 FROM t1 WHERE col1 < 100
            CREATE VIEW v1 AS SELECT col1, col2 FROM t1 WHERE col1 < 100
            """;

    private static final int THREADS = 8;
    private static final long TIMEOUT_SECONDS = 60;

    @RegisterExtension
    @Order(0)
    public final EmbeddedRelationalExtension relationalExtension = new EmbeddedRelationalExtension();

    @RegisterExtension
    @Order(1)
    public final SimpleDatabaseRule database = new SimpleDatabaseRule(SqlFunctionsConcurrencyTest.class,
            SCHEMA_TEMPLATE, new SchemaTemplateRule.SchemaTemplateOptions(true, true));

    @RegisterExtension
    @Order(2)
    public final RelationalConnectionRule connection = new RelationalConnectionRule(database::getConnectionUri)
            .withSchema("TEST_SCHEMA");

    private RecordLayerSchemaTemplate schemaTemplate;
    private RecordStoreState storeState;

    @BeforeEach
    void captureTemplateAndState() throws Exception {
        final var conn = (EmbeddedRelationalConnection) connection.connection;
        conn.setAutoCommit(false);
        conn.createNewTransaction();
        try {
            final var store = conn.getRecordLayerDatabase()
                    .loadSchema(connection.getSchema())
                    .loadStore().unwrap(FDBRecordStoreBase.class);
            schemaTemplate = (RecordLayerSchemaTemplate) conn.getSchemaTemplate();
            storeState = store.getRecordStoreState();
        } finally {
            conn.rollback();
            conn.setAutoCommit(true);
        }
    }

    @Nonnull
    private static Stream<Arguments> queries() {
        return Stream.of(
                Arguments.of("SELECT * FROM f1(1, 'a')", 5),
                Arguments.of("SELECT * FROM f2(1)", 5),
                Arguments.of("SELECT * FROM f4(103, 'b')", 3),
                Arguments.of("SELECT * FROM f0()", 5),
                Arguments.of("SELECT * FROM f0() AS a, f0() AS b WHERE a.col1 = b.col1", 5),
                Arguments.of("SELECT * FROM v1", 5));
    }

    /**
     * Tests that planning a statement that calls a function from several threads at the same time succeeds. All the
     * threads start planning the statement together in every round, and share the same schema template.
     */
    @ParameterizedTest
    @MethodSource("queries")
    void planFunctionCallConcurrently(@Nonnull final String query, final int rounds) throws Exception {
        final var failures = new ConcurrentLinkedQueue<Throwable>();
        final var barrier = new CyclicBarrier(THREADS);
        final Callable<Void> planRepeatedly = () -> {
            for (int round = 0; round < rounds; round++) {
                barrier.await(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                try {
                    PlanGenerator.create(Optional.empty(), schemaTemplate, schemaTemplate.toRecordMetadata(),
                                    storeState, NoOpMetricCollector.INSTANCE, connection.getOptions(),
                                    PreparedParams.empty())
                            .getPlan(query);
                } catch (Exception | AssertionError e) {
                    failures.add(e);
                }
            }
            return null;
        };

        final ExecutorService executor = Executors.newFixedThreadPool(THREADS);
        try {
            final List<Future<Void>> futures = new ArrayList<>();
            for (int thread = 0; thread < THREADS; thread++) {
                futures.add(executor.submit(planRepeatedly));
            }
            for (final Future<Void> future : futures) {
                future.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            }
        } finally {
            executor.shutdownNow();
        }

        assertThat(failures).isEmpty();
    }

    /**
     * Tests that two call sites of the same function, with and without parameters, get instances of the body that
     * share no reference and no quantifier alias.
     */
    @Test
    void encapsulateInstancesDoNotShareState() {
        for (final var call : List.of(
                new FunctionCall("F0", CallSiteArguments.ofPositional(List.<Value>of())),
                new FunctionCall("F1", CallSiteArguments.ofPositional(LiteralValue.ofScalar(103L),
                        LiteralValue.ofScalar("b"))))) {
            final var routine = (RecordLayerInvokedRoutine) schemaTemplate.findInvokedRoutineByName(call.name())
                    .orElseThrow();
            final var function = routine.getUserDefinedFunctionProvider().apply(false);

            final var instance1 = Reference.initialOf((RelationalExpression)function.encapsulate(call.arguments()));
            final var instance2 = Reference.initialOf((RelationalExpression)function.encapsulate(call.arguments()));

            assertThat(collectReferences(instance1)).doesNotContainAnyElementsOf(collectReferences(instance2));
            assertThat(collectAliases(instance1)).doesNotContainAnyElementsOf(collectAliases(instance2));
        }
    }

    private record FunctionCall(@Nonnull String name, @Nonnull CallSiteArguments arguments) {
    }

    @Nonnull
    private static Set<Reference> collectReferences(@Nonnull final Reference root) {
        final Set<Reference> references = Sets.newIdentityHashSet();
        references.addAll(TopologicalSort.anyTopologicalOrderPermutation(referencesAndDependencies().evaluate(root))
                .orElseThrow());
        return references;
    }

    @Nonnull
    private static Set<CorrelationIdentifier> collectAliases(@Nonnull final Reference root) {
        return collectReferences(root).stream()
                .flatMap(reference -> reference.getAllMemberExpressions().stream())
                .flatMap(expression -> expression.getQuantifiers().stream())
                .map(Quantifier::getAlias)
                .collect(Collectors.toSet());
    }
}
