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
import com.apple.foundationdb.record.query.plan.QueryPlanConstraint;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ValuePredicate;
import com.apple.foundationdb.record.query.plan.cascades.values.ConstantObjectValue;
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
import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests the {@link PlanGenerator} entry points that take caller-supplied {@link PreparedParams}, where a named
 * parameter with a declared type and no value is planned value-free. These need no database; the end-to-end warm-up
 * coverage that does is in {@code StoredQueriesTest}.
 */
class OfflineValueFreePlanGenerationTest {


    /**
     * The resulting constraint cannot be satisfied without a binding, which is what makes a value-free cache lookup a
     * non-match rather than a false hit.
     */
    @Test
    void declaredTypePlansNamedParameterValueFreeWithoutAStore() throws Exception {
        final var plan = PlanGenerator.create(
                        booksTemplate(),
                        NoOpMetadataOperationsFactory.INSTANCE,
                        NoOpMetricCollector.INSTANCE,
                        Options.NONE,
                        PreparedParams.empty().withDeclaredTypeParams(Map.of("param_a", "BIGINT")))
                .getPlan("select title from books where id = ?param_a");

        final var constraint = plan.getConstraint();

        assertThat(constraint.isConstrained()).isTrue();
        // Dereferencing the value-free constant raises MissingBindingException, which compileTimeEval treats as
        // unsatisfied.
        assertThat(constraint.compileTimeEval(EvaluationContext.forTypeRepository(ParseHelpers.EMPTY_TYPE_REPOSITORY)))
                .isFalse();
    }

    @Test
    void declaredTypePlansNamedParameterValueFreeWithRecordStoreState() throws Exception {
        final var plan = PlanGenerator.create(
                        Optional.empty(),
                        booksTemplate(),
                        new RecordStoreState(null, null),
                        NoOpMetricCollector.INSTANCE,
                        Options.NONE,
                        PreparedParams.empty().withDeclaredTypeParams(Map.of("param_a", "BIGINT")))
                .getPlan("select title from books where id = ?param_a");

        assertThat(plan.getConstraint().isConstrained()).isTrue();
        assertThat(plan.getConstraint()
                .compileTimeEval(EvaluationContext.forTypeRepository(ParseHelpers.EMPTY_TYPE_REPOSITORY)))
                .isFalse();
    }

    @Test
    void declaredTypeRejectsANullBinding() throws Exception {
        final var constraint = valueFreePlanConstraint("BIGINT");

        assertThat(constraint.compileTimeEval(bindingConstantsOf(constraint, 42L))).isTrue();
        assertThat(constraint.compileTimeEval(bindingConstantsOf(constraint, null))).isFalse();
    }

    /**
     * A declaration is resolved to a non-nullable type whatever it says about nullability, since it is only ever
     * resolved for a parameter left without a value.
     */
    @Test
    void nullableDeclarationStillRejectsANullBinding() throws Exception {
        final var constraint = valueFreePlanConstraint("BIGINT NULL");

        assertThat(constraint.compileTimeEval(bindingConstantsOf(constraint, 42L))).isTrue();
        assertThat(constraint.compileTimeEval(bindingConstantsOf(constraint, null))).isFalse();
        assertThat(constraint).isEqualTo(valueFreePlanConstraint("BIGINT NOT NULL"));
    }

    /**
     * The constraint is the plan cache key, so a warmed plan and a plan built later from a value must not become two
     * competing entries.
     */
    @Test
    void declaredTypeConstrainsAsABoundValueDoes() throws Exception {
        final var warmed = valueFreePlanConstraint("BIGINT");
        final var fromValue = planConstraint(PreparedParams.ofNamed(Map.of("param_a", 42L)));

        assertThat(warmed).isEqualTo(fromValue);
    }

    /**
     * Reported rather than guessed: a wrong type would warm a plan no binding could match.
     */
    @Test
    void schemaTemplateTypeCannotBeResolvedFromADeclaration() {
        assertThatThrownBy(() -> valueFreePlanConstraint("TYPE some_struct"))
                .hasMessageContaining("cannot resolve declared type");
    }

    /**
     * Plans {@code where id = ?param_a} with {@code param_a} declared and unbound, and returns the plan's constraint.
     */
    @Nonnull
    private QueryPlanConstraint valueFreePlanConstraint(@Nonnull final String declaredType) throws Exception {
        return planConstraint(PreparedParams.empty().withDeclaredTypeParams(Map.of("param_a", declaredType)));
    }

    @Nonnull
    private QueryPlanConstraint planConstraint(@Nonnull final PreparedParams preparedParams) throws Exception {
        return PlanGenerator.create(
                        booksTemplate(),
                        NoOpMetadataOperationsFactory.INSTANCE,
                        NoOpMetricCollector.INSTANCE,
                        Options.NONE,
                        preparedParams)
                .getPlan("select title from books where id = ?param_a")
                .getConstraint();
    }

    /**
     * Binds every constant the constraint references to {@code value}. The ids are read out of the constraint rather
     * than assumed, since they follow token positions.
     */
    @Nonnull
    private static EvaluationContext bindingConstantsOf(@Nonnull final QueryPlanConstraint constraint,
                                                        @Nullable final Object value) {
        final var bindings = new HashMap<String, Object>();
        constraint.getPredicate().preOrderStream()
                .flatMap(predicate -> predicate.narrowMaybe(ValuePredicate.class).stream())
                .forEach(valuePredicate -> valuePredicate.getValue().preOrderStream()
                        .filter(ConstantObjectValue.class::isInstance)
                        .map(ConstantObjectValue.class::cast)
                        .forEach(cov -> bindings.put(cov.getConstantId(), value)));
        return EvaluationContext.newBuilder()
                .setConstant(Quantifier.constant(), bindings)
                .build(ParseHelpers.EMPTY_TYPE_REPOSITORY);
    }

    /**
     * Without a declaration the parameter is a plain unbound one, which is an error — this is what keeps value-free
     * planning reachable only from warm-up.
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
