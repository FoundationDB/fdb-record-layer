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
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.explain.ExplainPlanVisitor;
import com.apple.foundationdb.record.query.plan.cascades.expressions.ExplodeExpression;
import com.apple.foundationdb.record.query.plan.cascades.properties.DerivationsProperty;
import com.apple.foundationdb.record.query.plan.cascades.properties.DistinctRecordsProperty;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.TypeRepository;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.apple.foundationdb.record.query.plan.serialization.DefaultPlanSerializationRegistry;
import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Message;
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
    private static final int WITH_ZERO_BASED_ORDINALITY_LEGACY_HASH = -481199675;
    private static final int WITH_ZERO_BASED_ORDINALITY_FOR_CONTINUATION_HASH = -481199675;

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

        final var withZeroBasedOrdinality = new RecordQueryExplodePlan(collectionValue, true, true);
        Assertions.assertEquals(WITH_ZERO_BASED_ORDINALITY_LEGACY_HASH,
                withZeroBasedOrdinality.planHash(PlanHashable.CURRENT_LEGACY));
        Assertions.assertEquals(WITH_ZERO_BASED_ORDINALITY_FOR_CONTINUATION_HASH,
                withZeroBasedOrdinality.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));

        // Sanity check: The three variants must hash differently.
        Assertions.assertNotEquals(
                withoutOrdinality.planHash(PlanHashable.CURRENT_FOR_CONTINUATION),
                withOrdinality.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        Assertions.assertNotEquals(
                withOrdinality.planHash(PlanHashable.CURRENT_FOR_CONTINUATION),
                withZeroBasedOrdinality.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
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

    @Test
    void distinctRecordsWithoutOrdinalityIsFalse() {
        final var collectionValue = LiteralValue.ofList(List.of(1, 2, 3));
        final var plan = new RecordQueryExplodePlan(collectionValue, false);
        Assertions.assertFalse(DistinctRecordsProperty.distinctRecords().evaluate(plan));
    }

    @Test
    void distinctRecordsWithOrdinalityIsTrue() {
        final var collectionValue = LiteralValue.ofList(List.of(1, 2, 3));
        final var plan = new RecordQueryExplodePlan(collectionValue, true);
        Assertions.assertTrue(DistinctRecordsProperty.distinctRecords().evaluate(plan));
    }

    /**
     * Verify that {@link ExplodeExpression#getDynamicTypes()} registers the synthesized (element, ordinal) struct as
     * a dynamic type in the WITH ORDINALITY variant.
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

    /**
     * Runs an {@code EXPLODE ... WITH ORDINALITY} over the given elements and returns the ordinals it flows.
     */
    @Nonnull
    @SuppressWarnings("DataFlowIssue") // explode transposes a constant array Value, it does not need a record store
    private static List<Object> ordinalsOf(@Nonnull final List<Integer> elements, final boolean zeroBasedOrdinality) {
        final var plan = new RecordQueryExplodePlan(LiteralValue.ofList(elements), true, zeroBasedOrdinality);
        final var resultType = plan.getExplodeResultType();
        final var typeRepository = TypeRepository.newBuilder().addTypeIfNeeded(resultType).build();
        final var descriptor = Objects.requireNonNull(typeRepository.getMessageDescriptor(resultType));
        final var elementField = descriptor.getFields().get(0);
        final var ordinalField = descriptor.getFields().get(1);
        final var cursor = plan.executePlan(null, EvaluationContext.forTypeRepository(typeRepository), null,
                ExecuteProperties.newBuilder().build());
        final var ordinals = ImmutableList.builder();
        for (final var element : elements) {
            final var result = cursor.getNext();
            Assertions.assertTrue(result.hasNext());
            final var message = (Message)Objects.requireNonNull(result.get()).getDatum();
            Assertions.assertEquals(element, message.getField(elementField));
            ordinals.add(message.getField(ordinalField));
        }
        Assertions.assertFalse(cursor.getNext().hasNext());
        return ordinals.build();
    }

    @Test
    void explodeWithOrdinalityFlowsOneBasedOrdinalsByDefault() {
        // 1-based is what the SQL standard requires of WITH ORDINALITY, and what every plan serialized before 0-based
        // ordinals existed flows.
        Assertions.assertEquals(List.of(1, 2, 3), ordinalsOf(List.of(100, 200, 300), false));
    }

    @Test
    void explodeWithOrdinalityFlowsZeroBasedOrdinalsWhenAsked() {
        Assertions.assertEquals(List.of(0, 1, 2), ordinalsOf(List.of(100, 200, 300), true));
    }

    @Test
    void zeroBasedOrdinalityRequiresOrdinality() {
        final var collectionValue = LiteralValue.ofList(List.of(1, 2, 3));
        Assertions.assertThrows(VerifyException.class,
                () -> new RecordQueryExplodePlan(collectionValue, false, true));
        Assertions.assertThrows(VerifyException.class,
                () -> new ExplodeExpression(collectionValue, false, true));
    }

    @Test
    void translateCorrelationsPreservesZeroBasedOrdinality() {
        final var sourceAlias = CorrelationIdentifier.of("source");
        final var targetAlias = CorrelationIdentifier.of("target");
        final var arrayType = new Type.Array(false, Type.primitiveType(Type.TypeCode.INT, false));
        final var recordType = Type.Record.fromFields(List.of(
                Type.Record.Field.of(arrayType, Optional.of("arr"))));
        final var qov = QuantifiedObjectValue.of(sourceAlias, recordType);
        final Value collectionValue = FieldValue.ofFieldName(qov, "arr");
        final var translationMap = TranslationMap.ofAliases(sourceAlias, targetAlias);

        final var plan = new RecordQueryExplodePlan(collectionValue, true, true);
        final var translatedPlan = plan.translateCorrelations(translationMap, true, List.of());
        Assertions.assertNotSame(plan, translatedPlan);
        Assertions.assertTrue(translatedPlan.isWithOrdinality());
        Assertions.assertTrue(translatedPlan.isZeroBasedOrdinality());

        final var expression = new ExplodeExpression(collectionValue, true, true);
        final var translatedExpression = expression.translateCorrelations(translationMap, true, List.of());
        Assertions.assertNotSame(expression, translatedExpression);
        Assertions.assertTrue(translatedExpression.isWithOrdinality());
        Assertions.assertTrue(translatedExpression.isZeroBasedOrdinality());
    }

    @Test
    void ordinalBaseIsPartOfIdentity() {
        final var collectionValue = LiteralValue.ofList(List.of(1, 2, 3));

        // The two variants flow different ordinals for the same array, so neither may stand in for the other, whether
        // as a plan or as an expression to be matched.
        Assertions.assertNotEquals(new RecordQueryExplodePlan(collectionValue, true, false),
                new RecordQueryExplodePlan(collectionValue, true, true));
        Assertions.assertFalse(new ExplodeExpression(collectionValue, true, false)
                .semanticEquals(new ExplodeExpression(collectionValue, true, true), AliasMap.emptyMap()));
    }

    @Test
    void explainOutputIncludesZeroBasedOrdinality() {
        final var collectionValue = LiteralValue.ofList(List.of(1, 2, 3));

        final String zeroBased = ExplainPlanVisitor.toStringForDebugging(
                new RecordQueryExplodePlan(collectionValue, true, true));
        Assertions.assertTrue(zeroBased.contains("WITH ZERO BASED ORDINALITY"));
        Assertions.assertEquals("[1, 2, 3] WITH ZERO BASED ORDINALITY",
                new ExplodeExpression(collectionValue, true, true).toString());

        final String oneBased = ExplainPlanVisitor.toStringForDebugging(
                new RecordQueryExplodePlan(collectionValue, true, false));
        Assertions.assertTrue(oneBased.contains("WITH ORDINALITY"));
        Assertions.assertFalse(oneBased.contains("ZERO"));
    }

    @Test
    void protoRoundTripPreservesZeroBasedOrdinality() {
        final var sourceAlias = CorrelationIdentifier.of("source");
        final var arrayType = new Type.Array(false, Type.primitiveType(Type.TypeCode.INT, false));
        final var recordType = Type.Record.fromFields(List.of(
                Type.Record.Field.of(arrayType, Optional.of("arr"))));
        final var qov = QuantifiedObjectValue.of(sourceAlias, recordType);
        final Value collectionValue = FieldValue.ofFieldName(qov, "arr");

        final RecordQueryExplodePlan zeroBased = new RecordQueryExplodePlan(collectionValue, true, true);
        final PRecordQueryExplodePlan zeroBasedProto = zeroBased.toProto(newSerializationContext());
        Assertions.assertTrue(zeroBasedProto.getZeroBasedOrdinality());
        final RecordQueryExplodePlan deserializedZeroBased =
                RecordQueryExplodePlan.fromProto(newSerializationContext(), zeroBasedProto);
        Assertions.assertTrue(deserializedZeroBased.isZeroBasedOrdinality());
        Assertions.assertEquals(zeroBased, deserializedZeroBased);

        // A plan flowing 1-based ordinals must not set the field at all, so that it serializes to exactly the bytes it
        // serialized to before the field existed.
        final RecordQueryExplodePlan oneBased = new RecordQueryExplodePlan(collectionValue, true, false);
        final PRecordQueryExplodePlan oneBasedProto = oneBased.toProto(newSerializationContext());
        Assertions.assertFalse(oneBasedProto.hasZeroBasedOrdinality());
        Assertions.assertFalse(RecordQueryExplodePlan.fromProto(newSerializationContext(), oneBasedProto)
                .isZeroBasedOrdinality());

        // ... and a plan serialized by such a version, which cannot have the field, deserializes to 1-based ordinals.
        final PRecordQueryExplodePlan legacyProto = PRecordQueryExplodePlan.newBuilder()
                .setCollectionValue(collectionValue.toValueProto(newSerializationContext()))
                .setWithOrdinality(true)
                .build();
        final RecordQueryExplodePlan legacy = RecordQueryExplodePlan.fromProto(newSerializationContext(), legacyProto);
        Assertions.assertTrue(legacy.isWithOrdinality());
        Assertions.assertFalse(legacy.isZeroBasedOrdinality());
        Assertions.assertEquals(oneBased, legacy);
    }
}
