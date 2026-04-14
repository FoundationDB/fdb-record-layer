/*
 * ExplodePlanTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.plans;

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.planprotos.PRecordQueryExplodePlan;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.explain.ExplainPlanVisitor;
import com.apple.foundationdb.record.query.plan.cascades.expressions.ExplodeExpression;
import com.apple.foundationdb.record.query.plan.cascades.properties.DerivationsProperty;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.apple.foundationdb.record.query.plan.serialization.DefaultPlanSerializationRegistry;
import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.junit.jupiter.params.support.ParameterDeclarations;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

public class ExplodePlanTest {

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    private static final class ExplodeCursorBuilder {

        @Nonnull
        private final RecordQueryPlan explodePlan;

        @Nonnull
        private Optional<Integer> skip;

        @Nonnull
        private Optional<Integer> limit;

        private ExplodeCursorBuilder() {
            explodePlan = generateExplodePlan();
            skip = Optional.empty();
            limit = Optional.empty();
        }

        @Nonnull
        ExplodeCursorBuilder withSkip(int skip) {
            this.skip = Optional.of(skip);
            return this;
        }

        @Nonnull
        ExplodeCursorBuilder withLimit(int limit) {
            this.limit = Optional.of(limit);
            return this;
        }

        @Override
        public String toString() {
            return "explode [1, 2, 3, 4, 5, 6, 7, 8, 9, 10] "
                    + limit.map(l -> "limit " + l + " ").orElse("")
                    + skip.map(s -> "skip " + s).orElse("");
        }

        @Nonnull
        @SuppressWarnings("DataFlowIssue") // explode transposes the underlying constant array Value, it does not strictly require a record store instance.
        RecordCursor<QueryResult> build() {
            final var executionPropertiesBuilder = ExecuteProperties.newBuilder();
            skip.ifPresent(executionPropertiesBuilder::setSkip);
            limit.ifPresent(executionPropertiesBuilder::setReturnedRowLimit);
            final var executionProperties = executionPropertiesBuilder.build();
            return explodePlan.executePlan(null, EvaluationContext.EMPTY, null, executionProperties);
        }

        @Nonnull
        private static RecordQueryPlan generateExplodePlan() {
            final Value collectionValue = LiteralValue.ofList(List.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
            return new RecordQueryExplodePlan(collectionValue);
        }

        @Nonnull
        public static ExplodeCursorBuilder instance() {
            return new ExplodeCursorBuilder();
        }
    }

    private static void verifyCursor(@Nonnull final RecordCursor<QueryResult> actualCursor,
                                     @Nonnull final List<Integer> expectedResults,
                                     boolean verifyLimitExceeded) {
        for (final var expectedValue : expectedResults) {
            final var result = actualCursor.getNext();
            Assertions.assertTrue(result.hasNext());
            Assertions.assertNotNull(result.get());
            Assertions.assertEquals(expectedValue, Objects.requireNonNull(result.get()).getDatum());
        }
        if (verifyLimitExceeded) {
            final var result = actualCursor.getNext();
            Assertions.assertFalse(result.hasNext());
            Assertions.assertEquals(RecordCursor.NoNextReason.RETURN_LIMIT_REACHED, result.getNoNextReason());
        }
    }

    private static class ArgumentProvider implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(final ParameterDeclarations parameterDeclarations,
                                                            final ExtensionContext context) {
            return Stream.of(
                    Arguments.of(ExplodeCursorBuilder.instance().withLimit(1), ImmutableList.of(1), true),
                    Arguments.of(ExplodeCursorBuilder.instance().withLimit(4), ImmutableList.of(1, 2, 3, 4), true),
                    Arguments.of(ExplodeCursorBuilder.instance().withLimit(1).withSkip(3), ImmutableList.of(4), true),
                    Arguments.of(ExplodeCursorBuilder.instance().withLimit(2).withSkip(5), ImmutableList.of(6, 7), true),
                    Arguments.of(ExplodeCursorBuilder.instance(), ImmutableList.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), false));
        }
    }

    @ParameterizedTest(name = "{0} should return {1}")
    @ArgumentsSource(ArgumentProvider.class)
    void explodeWithSkipAndLimitWorks(@Nonnull final ExplodeCursorBuilder actualCursorBuilder,
                                      @Nonnull final List<Integer> expectedResult,
                                      boolean shouldReachLimit) {
        verifyCursor(actualCursorBuilder.build(), expectedResult, shouldReachLimit);
    }

    @Test
    void translateCorrelationsPreservesWithOrdinality() {
        final var sourceAlias = CorrelationIdentifier.of("source");
        final var targetAlias = CorrelationIdentifier.of("target");

        // Build a correlated collection value: `sourceAlias.arr` (an integer array field).
        final var arrayType = new Type.Array(false, Type.primitiveType(Type.TypeCode.INT, false));
        final var recordType = Type.Record.fromFields(List.of(
                Type.Record.Field.of(arrayType, Optional.of("arr"))));
        final var qov = QuantifiedObjectValue.of(sourceAlias, recordType);
        final var collectionValue = FieldValue.ofFieldName(qov, "arr");

        final var plan = new RecordQueryExplodePlan(collectionValue, true);
        Assertions.assertTrue(plan.isWithOrdinality());
        Assertions.assertTrue(plan.getCollectionValue().getCorrelatedTo().contains(sourceAlias));

        // Translate: Remap sourceAlias → targetAlias.
        final var translated = plan.translateCorrelations(TranslationMap.ofAliases(sourceAlias, targetAlias), true, List.of());

        // A new plan must have been created (the correlation changed).
        Assertions.assertNotSame(plan, translated);
        // `withOrdinality` must be preserved.
        Assertions.assertTrue(translated.isWithOrdinality());
        // The collection value must now reference the target alias, not the source.
        Assertions.assertTrue(translated.getCollectionValue().getCorrelatedTo().contains(targetAlias));
        Assertions.assertFalse(translated.getCollectionValue().getCorrelatedTo().contains(sourceAlias));
    }

    @Test
    void translateCorrelationsNoOpReturnsSameInstance() {
        final var collectionValue = LiteralValue.ofList(List.of(1, 2, 3));
        final var plan = new RecordQueryExplodePlan(collectionValue, true);
        Assertions.assertTrue(plan.isWithOrdinality());

        // Translate with a mapping for an alias not present in the value.
        final var translated = plan.translateCorrelations(
                TranslationMap.ofAliases(CorrelationIdentifier.of("absent"), CorrelationIdentifier.of("target")),
                true, List.of());

        // No translation occurred, so the same instance is returned.
        Assertions.assertSame(plan, translated);
        Assertions.assertTrue(translated.isWithOrdinality());
    }

    @Test
    void explainOutputIncludesWithOrdinality() {
        final var collectionValue = LiteralValue.ofList(List.of(1, 2, 3));

        final String plan1 = ExplainPlanVisitor.toStringForDebugging(
                new RecordQueryExplodePlan(collectionValue, true));
        Assertions.assertTrue(plan1.contains("WITH ORDINALITY"));

        final String plan2 = ExplainPlanVisitor.toStringForDebugging(
                new RecordQueryExplodePlan(collectionValue, false));
        Assertions.assertFalse(plan2.contains("WITH ORDINALITY"));
        Assertions.assertFalse(plan2.contains("ORDINALITY"));
    }

    @Nonnull
    private static PlanSerializationContext newSerializationContext() {
        return new PlanSerializationContext(new DefaultPlanSerializationRegistry(),
                PlanHashable.CURRENT_FOR_CONTINUATION);
    }

    @Test
    void protoRoundTripPreservesWithOrdinality() {
        // Build an array-typed correlated field value that serializes cleanly.
        final var sourceAlias = CorrelationIdentifier.of("source");
        final var arrayType = new Type.Array(false, Type.primitiveType(Type.TypeCode.INT, false));
        final var recordType = Type.Record.fromFields(List.of(
                Type.Record.Field.of(arrayType, Optional.of("arr"))));
        final var qov = QuantifiedObjectValue.of(sourceAlias, recordType);
        final Value collectionValue = FieldValue.ofFieldName(qov, "arr");

        // With ordinality.
        final RecordQueryExplodePlan original = new RecordQueryExplodePlan(collectionValue, true);
        final PRecordQueryExplodePlan proto = original.toProto(newSerializationContext());
        final RecordQueryExplodePlan deserialized = RecordQueryExplodePlan.fromProto(newSerializationContext(), proto);
        Assertions.assertTrue(deserialized.isWithOrdinality());
        Assertions.assertEquals(original, deserialized);

        // Without ordinality.
        final RecordQueryExplodePlan originalNoOrd = new RecordQueryExplodePlan(collectionValue, false);
        final PRecordQueryExplodePlan protoNoOrd = originalNoOrd.toProto(newSerializationContext());
        final RecordQueryExplodePlan deserializedNoOrd = RecordQueryExplodePlan.fromProto(newSerializationContext(), protoNoOrd);
        Assertions.assertFalse(deserializedNoOrd.isWithOrdinality());
        Assertions.assertEquals(originalNoOrd, deserializedNoOrd);
    }

    // Pinned hash values for the `planHashIsStable()` test.
    private static final int WITHOUT_ORDINALITY_LEGACY_HASH = -1251896027;
    private static final int WITHOUT_ORDINALITY_FOR_CONTINUATION_HASH = -1251896027;
    private static final int WITH_ORDINALITY_LEGACY_HASH = -154069942;
    private static final int WITH_ORDINALITY_FOR_CONTINUATION_HASH = -154069942;

    @Test
    void planHashIsStable() {
        final var collectionValue = LiteralValue.ofList(List.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));

        final var withoutOrdinality = new RecordQueryExplodePlan(collectionValue, false);
        Assertions.assertEquals(WITHOUT_ORDINALITY_LEGACY_HASH,
                withoutOrdinality.planHash(PlanHashable.CURRENT_LEGACY));
        Assertions.assertEquals(WITHOUT_ORDINALITY_FOR_CONTINUATION_HASH,
                withoutOrdinality.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));

        final var withOrdinality = new RecordQueryExplodePlan(collectionValue, true);
        Assertions.assertEquals(WITH_ORDINALITY_LEGACY_HASH,
                withOrdinality.planHash(PlanHashable.CURRENT_LEGACY));
        Assertions.assertEquals(WITH_ORDINALITY_FOR_CONTINUATION_HASH,
                withOrdinality.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));

        // Sanity check: The two variants must hash differently.
        Assertions.assertNotEquals(
                withoutOrdinality.planHash(PlanHashable.CURRENT_FOR_CONTINUATION),
                withOrdinality.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
    }

    @Test
    void derivationsPreserveCollectionCorrelation() {
        final var sourceAlias = CorrelationIdentifier.of("source");
        final var arrayType = new Type.Array(false, Type.primitiveType(Type.TypeCode.INT, false));
        final var recordType = Type.Record.fromFields(List.of(
                Type.Record.Field.of(arrayType, Optional.of("arr"))));
        final var qov = QuantifiedObjectValue.of(sourceAlias, recordType);
        final Value collectionValue = FieldValue.ofFieldName(qov, "arr");
        final var visitor = new DerivationsProperty.DerivationsVisitor();

        // Check that the correlation to `source` is correctly derived, both in the regular case and WITH ORDINALITY.
        final var withoutOrdinality = visitor.visitExplodePlan(new RecordQueryExplodePlan(collectionValue, false));
        Assertions.assertTrue(withoutOrdinality.getResultValues().get(0).getCorrelatedTo().contains(sourceAlias));
        final var withOrdinality = visitor.visitExplodePlan(new RecordQueryExplodePlan(collectionValue, true));
        Assertions.assertTrue(withOrdinality.getResultValues().get(0).getCorrelatedTo().contains(sourceAlias));
    }

    /**
     * Verify that {@link ExplodeExpression#getDynamicTypes()} registers the synthesized {@code {_element, _ordinal}}
     * struct as a dynamic type in the WITH ORDINALITY variant.
     */
    @Test
    void dynamicTypesIncludeOrdinalityStruct() {
        final var sourceAlias = CorrelationIdentifier.of("source");
        final var arrayType = new Type.Array(false, Type.primitiveType(Type.TypeCode.INT, false));
        final var recordType = Type.Record.fromFields(List.of(
                Type.Record.Field.of(arrayType, Optional.of("arr"))));
        final var qov = QuantifiedObjectValue.of(sourceAlias, recordType);
        final Value collectionValue = FieldValue.ofFieldName(qov, "arr");

        final var withOrdinality = new ExplodeExpression(collectionValue, true);
        final Type structType = withOrdinality.getExplodeResultType();
        final Set<Type> dynamicTypes = withOrdinality.getDynamicTypes();
        Assertions.assertTrue(dynamicTypes.contains(structType));

        final var withoutOrdinality = new ExplodeExpression(collectionValue, false);
        Assertions.assertFalse(withoutOrdinality.getDynamicTypes().contains(structType));
    }
}
