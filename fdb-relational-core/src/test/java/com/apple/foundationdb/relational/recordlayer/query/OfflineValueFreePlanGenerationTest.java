/*
 * OfflineValueFreePlanGenerationTest.java
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

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.RecordStoreState;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
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
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests the {@link PlanGenerator} entry points that stored-query warm-up uses, i.e. the ones taking caller-supplied
 * {@link PreparedParams}. Warm-up threads a signature's declared types through them so a named parameter with no value
 * is planned <em>value-free</em>: it reserves a constant id and contributes a type constraint, but binds nothing.
 * <br>
 * These require no database, which is the point — the end-to-end warm-up coverage in {@code StoredQueriesTest} needs a
 * running FDB, so the value-free planning contract itself is pinned here.
 */
class OfflineValueFreePlanGenerationTest {

    private static final Type LONG_TYPE = Type.primitiveType(Type.TypeCode.LONG).notNullable();

    /**
     * A declared type is enough to plan a named parameter that has no value, and the resulting constraint cannot be
     * satisfied without a binding — which is precisely what makes a value-free plan-cache lookup a safe non-match
     * rather than a false hit.
     */
    @Test
    void declaredTypePlansNamedParameterValueFreeWithoutAStore() throws Exception {
        final var plan = PlanGenerator.create(
                        booksTemplate(),
                        NoOpMetadataOperationsFactory.INSTANCE,
                        NoOpMetricCollector.INSTANCE,
                        Options.NONE,
                        PreparedParams.empty().withDeclaredTypes(Map.of("param_a", LONG_TYPE)))
                .getPlan("select title from books where id = ?param_a");

        final var constraint = plan.getConstraint();

        // The parameter contributed a constraint (its declared type plus "is not null"), so the plan is specialized.
        assertThat(constraint.isConstrained()).isTrue();
        // With no value bound, the constraint cannot be shown to hold: dereferencing the value-free constant raises
        // Bindings.MissingBindingException, which compileTimeEval treats as unsatisfied.
        assertThat(constraint.compileTimeEval(EvaluationContext.forTypeRepository(ParseHelpers.EMPTY_TYPE_REPOSITORY)))
                .isFalse();
    }

    /**
     * The same for the cache-taking overload, which is what plans a stored query's body once its declared functions are
     * in scope.
     */
    @Test
    void declaredTypePlansNamedParameterValueFreeWithRecordStoreState() throws Exception {
        final var plan = PlanGenerator.create(
                        Optional.empty(),
                        booksTemplate(),
                        new RecordStoreState(null, null),
                        NoOpMetricCollector.INSTANCE,
                        Options.NONE,
                        PreparedParams.empty().withDeclaredTypes(Map.of("param_a", LONG_TYPE)))
                .getPlan("select title from books where id = ?param_a");

        assertThat(plan.getConstraint().isConstrained()).isTrue();
        assertThat(plan.getConstraint()
                .compileTimeEval(EvaluationContext.forTypeRepository(ParseHelpers.EMPTY_TYPE_REPOSITORY)))
                .isFalse();
    }

    /**
     * Without declared types the same query is a plain unbound named parameter, which is an error rather than a
     * value-free plan. This is what keeps value-free planning reachable only from warm-up, and it exercises the
     * delegating overload that supplies {@link PreparedParams#empty()}.
     */
    @Test
    void namedParameterWithoutDeclaredTypeOrValueIsRejected() {
        assertThatThrownBy(() -> PlanGenerator.create(
                        booksTemplate(),
                        NoOpMetadataOperationsFactory.INSTANCE,
                        NoOpMetricCollector.INSTANCE,
                        Options.NONE)
                .getPlan("select title from books where id = ?param_a"))
                .isInstanceOf(RelationalException.class)
                .hasMessageContaining("No value found for parameter param_a");
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
                                .setDataType(DataType.Primitives.LONG.type())
                                .build())
                        .addColumn(RecordLayerColumn.newBuilder()
                                .setName("TITLE")
                                .setDataType(DataType.Primitives.STRING.type())
                                .build())
                        .addPrimaryKeyPart(List.of("ID"))
                        .build())
                .build();
    }
}
