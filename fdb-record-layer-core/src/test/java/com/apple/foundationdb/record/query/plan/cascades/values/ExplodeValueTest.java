/*
 * ExplodeValueTest.java
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

package com.apple.foundationdb.record.query.plan.cascades.values;

import com.apple.foundationdb.record.Bindings;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.TestRecords4Proto;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.properties.DerivationsProperty;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.TypeRepository;
import com.apple.foundationdb.record.query.plan.explain.DefaultExplainFormatter;
import com.apple.foundationdb.record.query.plan.explain.DefaultExplainSymbolMap;
import com.apple.foundationdb.record.query.plan.plans.QueryResult;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryTableFunctionPlan;
import com.apple.foundationdb.record.query.plan.serialization.DefaultPlanSerializationRegistry;
import com.apple.test.BooleanSource;
import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.mockito.Mockito;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tests for {@link ExplodeValue}.
 */
class ExplodeValueTest {

    private static final CorrelationIdentifier RESTAURANT_ALIAS = CorrelationIdentifier.of("restaurant");

    @Nonnull
    private static final FDBRecordStoreBase<Message> STORE = mockStore();

    @Nonnull
    @SuppressWarnings("unchecked")
    private static FDBRecordStoreBase<Message> mockStore() {
        final FDBRecordStoreBase<Message> store = Mockito.mock(FDBRecordStoreBase.class);
        Mockito.when(store.getExecutor()).thenReturn(ForkJoinPool.commonPool());
        return store;
    }

    @Nonnull
    private static PlanSerializationContext newSerializationContext() {
        return new PlanSerializationContext(new DefaultPlanSerializationRegistry(),
                PlanHashable.CURRENT_FOR_CONTINUATION);
    }

    @Nonnull
    private static TestRecords4Proto.RestaurantReview review(final long reviewer, final int rating) {
        return TestRecords4Proto.RestaurantReview.newBuilder().setReviewer(reviewer).setRating(rating).build();
    }

    @Nonnull
    private static Value reviewsOfRestaurant() {
        return FieldValue.ofFieldName(QuantifiedObjectValue.of(RESTAURANT_ALIAS,
                Type.Record.fromDescriptor(TestRecords4Proto.RestaurantRecord.getDescriptor())), "reviews");
    }

    @Nonnull
    private static EvaluationContext contextFor(@Nonnull final ExplodeValue value) {
        return EvaluationContext.forTypeRepository(TypeRepository.newBuilder().addAllTypes(value.getDynamicTypes()).build());
    }

    @Nonnull
    private static EvaluationContext contextFor(@Nonnull final ExplodeValue value,
                                                @Nonnull final TestRecords4Proto.RestaurantRecord restaurant) {
        return contextFor(value).withBinding(Bindings.Internal.CORRELATION, RESTAURANT_ALIAS,
                QueryResult.ofComputed(restaurant));
    }

    /**
     * Drains the stream of an explode, returning each struct it produces as the list of its fields, that is
     * {@code [element]}, or {@code [element, ordinal]} for an explode with ordinality.
     */
    @Nonnull
    private static List<List<Object>> explode(@Nonnull final ExplodeValue value, @Nonnull final EvaluationContext context,
                                              @Nullable final byte[] continuation,
                                              @Nonnull final ExecuteProperties executeProperties) {
        final var structs = value.evalAsStream(STORE, context, continuation, executeProperties).asList().join();
        return structs.stream()
                .map(result -> fieldsOf(Objects.requireNonNull(result.getDatum())))
                .collect(ImmutableList.toImmutableList());
    }

    /**
     * Evaluates an explode eagerly, returning each struct it produces as the list of its fields.
     */
    @Nonnull
    private static List<List<Object>> explodeAsList(@Nonnull final ExplodeValue value, @Nonnull final EvaluationContext context) {
        return value.evalAsList(null, context).stream()
                .map(ExplodeValueTest::fieldsOf)
                .collect(ImmutableList.toImmutableList());
    }

    @Nonnull
    private static List<Object> fieldsOf(@Nonnull final Object struct) {
        final var message = (Message)struct;
        return message.getDescriptorForType().getFields().stream()
                .map(message::getField)
                .collect(ImmutableList.toImmutableList());
    }

    @Nonnull
    private static List<List<Object>> explode(@Nonnull final ExplodeValue value, @Nonnull final EvaluationContext context) {
        return explode(value, context, null, ExecuteProperties.SERIAL_EXECUTE);
    }

    @Test
    void resultTypeIsTheElement() {
        final var value = new ExplodeValue(LiteralValue.ofList(List.of("a", "b")));
        Assertions.assertFalse(value.hasOrdinality());

        final var fields = value.getResultType().getFields();
        Assertions.assertEquals(1, fields.size());
        Assertions.assertEquals(((Type.Array)value.getCollectionValue().getResultType()).getElementType(), fields.get(0).getFieldType());
        Assertions.assertEquals("_0", fields.get(0).getFieldName());
        Assertions.assertTrue(value.getDynamicTypes().contains(value.getResultType()));
    }

    @Test
    void resultTypeWithOrdinalityIsTheElementAndItsOrdinal() {
        final var value = new ExplodeValue(LiteralValue.ofList(List.of("a", "b")), true);
        Assertions.assertTrue(value.hasOrdinality());

        final var fields = value.getResultType().getFields();
        Assertions.assertEquals(2, fields.size());
        Assertions.assertEquals(((Type.Array)value.getCollectionValue().getResultType()).getElementType(), fields.get(0).getFieldType());
        Assertions.assertEquals(Type.primitiveType(Type.TypeCode.INT, false), fields.get(1).getFieldType());
        // Both fields are declared anonymous, the way an explode WITH ORDINALITY declares its struct, and are so named
        // by their position.
        Assertions.assertEquals("_0", fields.get(0).getFieldName());
        Assertions.assertEquals("_1", fields.get(1).getFieldName());

        // The struct type is created by the value, so it is one of its dynamic types.
        Assertions.assertTrue(value.getDynamicTypes().contains(value.getResultType()));
    }

    @Test
    void producesEveryElement() {
        final var value = new ExplodeValue(LiteralValue.ofList(List.of("a", "b", "c")));
        Assertions.assertEquals(List.of(List.of("a"), List.of("b"), List.of("c")),
                explode(value, contextFor(value)));
    }

    @Test
    void producesEveryElementWithItsZeroBasedOrdinal() {
        final var value = new ExplodeValue(LiteralValue.ofList(List.of("a", "b", "c")), true);
        Assertions.assertEquals(List.of(List.of("a", 0), List.of("b", 1), List.of("c", 2)),
                explode(value, contextFor(value)));
    }

    @Test
    void producesOrdinalsEvenWhenTheElementsAreOfTheOrdinalType() {
        final var value = new ExplodeValue(LiteralValue.ofList(List.of(7, 7, 9)), true);
        Assertions.assertEquals(List.of(List.of(7, 0), List.of(7, 1), List.of(9, 2)),
                explode(value, contextFor(value)));
    }

    @Test
    void explodesAnArrayOfMessagesOfACorrelatedRecord() {
        final var value = new ExplodeValue(reviewsOfRestaurant(), true);
        final var restaurant = TestRecords4Proto.RestaurantRecord.newBuilder()
                .setRestNo(1L)
                .addReviews(review(10L, 5))
                .addReviews(review(20L, 3))
                .build();

        final var exploded = explode(value, contextFor(value, restaurant));
        Assertions.assertEquals(2, exploded.size());
        final var firstReview = (Message)exploded.get(0).get(0);
        final var secondReview = (Message)exploded.get(1).get(0);
        Assertions.assertEquals(10L, firstReview.getField(firstReview.getDescriptorForType().findFieldByName("reviewer")));
        Assertions.assertEquals(20L, secondReview.getField(secondReview.getDescriptorForType().findFieldByName("reviewer")));
        Assertions.assertEquals(List.of(0, 1), List.of(exploded.get(0).get(1), exploded.get(1).get(1)));
    }

    @ParameterizedTest
    @BooleanSource
    void evaluatingAsAListProducesWhatTheStreamProduces(final boolean withOrdinality) {
        final var literal = new ExplodeValue(LiteralValue.ofList(List.of("a", "b", "c")), withOrdinality);
        final var literalContext = contextFor(literal);
        Assertions.assertEquals(explode(literal, literalContext), explodeAsList(literal, literalContext));

        final var reviews = new ExplodeValue(reviewsOfRestaurant(), withOrdinality);
        final var restaurant = TestRecords4Proto.RestaurantRecord.newBuilder()
                .setRestNo(1L)
                .addReviews(review(10L, 5))
                .addReviews(review(20L, 3))
                .build();
        // the same context for both, so that the messages of the reviews share their descriptor
        final var reviewsContext = contextFor(reviews, restaurant);
        Assertions.assertEquals(explode(reviews, reviewsContext), explodeAsList(reviews, reviewsContext));
    }

    @Test
    void evaluatingAsAListOfAnEmptyOrNullArrayProducesNothing() {
        final var empty = new ExplodeValue(new LiteralValue<>(new Type.Array(Type.primitiveType(Type.TypeCode.INT, false)), List.of()), true);
        Assertions.assertEquals(List.of(), explodeAsList(empty, contextFor(empty)));

        final var nullArray = new ExplodeValue(new NullValue(new Type.Array(Type.primitiveType(Type.TypeCode.INT, false))), true);
        Assertions.assertEquals(List.of(), explodeAsList(nullArray, contextFor(nullArray)));
    }

    @ParameterizedTest
    @BooleanSource
    void theOrdinalsOfASortedCollectionFollowTheSortedOrder(final boolean reverse) {
        final var value = new ExplodeValue(new ArraySortValue(LiteralValue.ofList(List.of(30, 10, 20)), reverse), true);
        Assertions.assertEquals(reverse
                                ? List.of(List.of(30, 0), List.of(20, 1), List.of(10, 2))
                                : List.of(List.of(10, 0), List.of(20, 1), List.of(30, 2)),
                explodeAsList(value, contextFor(value)));
    }

    @Test
    void anEmptyOrNullArrayProducesNothing() {
        final var value = new ExplodeValue(reviewsOfRestaurant());
        final var restaurantWithoutReviews = TestRecords4Proto.RestaurantRecord.newBuilder().setRestNo(1L).build();
        Assertions.assertEquals(List.of(), explode(value, contextFor(value, restaurantWithoutReviews)));

        final var nullArray = new ExplodeValue(new NullValue(new Type.Array(Type.primitiveType(Type.TypeCode.INT, false))));
        Assertions.assertEquals(List.of(), explode(nullArray, contextFor(nullArray)));
    }

    @Test
    void honorsSkipAndLimit() {
        final var value = new ExplodeValue(LiteralValue.ofList(List.of("a", "b", "c", "d")), true);
        final var executeProperties = ExecuteProperties.newBuilder().setSkip(1).setReturnedRowLimit(2).build();
        Assertions.assertEquals(List.of(List.of("b", 1), List.of("c", 2)),
                explode(value, contextFor(value), null, executeProperties));
    }

    @Test
    void resumesFromAContinuation() {
        final var value = new ExplodeValue(LiteralValue.ofList(List.of("a", "b", "c")), true);
        final var context = contextFor(value);

        final RecordCursorResult<QueryResult> first =
                value.evalAsStream(STORE, context, null, ExecuteProperties.SERIAL_EXECUTE).getNext();
        Assertions.assertTrue(first.hasNext());

        // The rest of the stream picks up after the first element, ordinals included.
        Assertions.assertEquals(List.of(List.of("b", 1), List.of("c", 2)),
                explode(value, context, first.getContinuation().toBytes(), ExecuteProperties.SERIAL_EXECUTE));
    }

    @Test
    void theContinuationOfTheLastElementResumesToNothing() {
        final var value = new ExplodeValue(LiteralValue.ofList(List.of("a", "b")));
        final var context = contextFor(value);
        final var cursor = value.evalAsStream(STORE, context, null, ExecuteProperties.SERIAL_EXECUTE);
        cursor.getNext();
        final RecordCursorResult<QueryResult> last = cursor.getNext();
        Assertions.assertTrue(last.hasNext());

        // A continuation handed out with a value is never an end continuation, even after the last element.
        Assertions.assertFalse(last.getContinuation().isEnd());
        Assertions.assertEquals(List.of(),
                explode(value, context, last.getContinuation().toBytes(), ExecuteProperties.SERIAL_EXECUTE));
        Assertions.assertTrue(cursor.getNext().getContinuation().isEnd());
    }

    @Test
    void rejectsAContinuationPastTheEndOfTheArray() {
        final var longer = new ExplodeValue(LiteralValue.ofList(List.of("a", "b", "c")));
        final var cursor = longer.evalAsStream(STORE, contextFor(longer), null, ExecuteProperties.SERIAL_EXECUTE);
        cursor.getNext();
        cursor.getNext();
        final byte[] continuation = cursor.getNext().getContinuation().toBytes();

        // Resuming the same position over an array that has since shrunk is an error.
        final var shorter = new ExplodeValue(LiteralValue.ofList(List.of("a")));
        Assertions.assertThrows(RecordCoreException.class,
                () -> shorter.evalAsStream(STORE, contextFor(shorter), continuation, ExecuteProperties.SERIAL_EXECUTE));
    }

    @Test
    void rejectsAContinuationThatCannotBeParsed() {
        final var value = new ExplodeValue(LiteralValue.ofList(List.of("a")));
        Assertions.assertThrows(RecordCoreException.class,
                () -> value.evalAsStream(STORE, contextFor(value), new byte[] {(byte)0xff, (byte)0xff},
                        ExecuteProperties.SERIAL_EXECUTE));
    }

    @Test
    void createsAStructOnlyOnceTheCursorReachesItsElement() {
        final var structsCreated = new AtomicInteger();
        final var cursor = new ExplodeValue.Cursor(ForkJoinPool.commonPool(), List.of("a", "b", "c"),
                (array, ordinal) -> {
                    structsCreated.incrementAndGet();
                    return List.of(array.get(ordinal), ordinal);
                }, null);
        Assertions.assertEquals(0, structsCreated.get());

        Assertions.assertEquals(List.of("a", 0), Objects.requireNonNull(cursor.getNext().get()).getDatum());
        Assertions.assertEquals(1, structsCreated.get());
    }

    @Test
    void rejectsACollectionThatIsNotAnArray() {
        Assertions.assertThrows(VerifyException.class, () -> new ExplodeValue(LiteralValue.ofScalar(1)));
    }

    @Test
    void cannotBeEvaluatedAsAScalar() {
        final var value = new ExplodeValue(LiteralValue.ofList(List.of(1, 2)));
        Assertions.assertThrows(IllegalStateException.class, () -> value.eval(null, contextFor(value)));
    }

    @Test
    void equalityAndHashingFollowTheCollection() {
        final var collection = LiteralValue.ofList(List.of(1, 2));
        final var value = new ExplodeValue(collection);

        Assertions.assertEquals(value, new ExplodeValue(LiteralValue.ofList(List.of(1, 2))));
        Assertions.assertEquals(value.hashCode(), new ExplodeValue(LiteralValue.ofList(List.of(1, 2))).hashCode());
        Assertions.assertNotEquals(value, new ExplodeValue(LiteralValue.ofList(List.of(1, 3))));
        Assertions.assertNotEquals(value.planHash(PlanHashable.CURRENT_FOR_CONTINUATION),
                new ExplodeValue(LiteralValue.ofList(List.of(1, 3))).planHash(PlanHashable.CURRENT_FOR_CONTINUATION));

        Assertions.assertSame(value, value.withChildren(List.of(collection)));
        final var otherCollection = LiteralValue.ofList(List.of(3, 4));
        Assertions.assertEquals(new ExplodeValue(otherCollection), value.withChildren(List.of(otherCollection)));
    }

    @Test
    void equalityAndHashingFollowTheOrdinality() {
        final var collection = LiteralValue.ofList(List.of(1, 2));
        final var value = new ExplodeValue(collection);
        final var withOrdinality = new ExplodeValue(collection, true);

        Assertions.assertNotEquals(value, withOrdinality);
        Assertions.assertNotEquals(value.planHash(PlanHashable.CURRENT_FOR_CONTINUATION),
                withOrdinality.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));

        // rebasing the collection keeps the ordinality
        final var otherCollection = LiteralValue.ofList(List.of(3, 4));
        Assertions.assertEquals(new ExplodeValue(otherCollection, true), withOrdinality.withChildren(List.of(otherCollection)));
        Assertions.assertEquals(new ExplodeValue(otherCollection, true), withOrdinality.withCollectionValue(otherCollection));
    }

    @Test
    void explainsAsAnExplodeOfTheCollection() {
        final var value = new ExplodeValue(LiteralValue.ofList(List.of(1, 2)));
        Assertions.assertEquals("explode([1, 2])",
                value.explain().getExplainTokens().render(DefaultExplainFormatter.create(DefaultExplainSymbolMap::new)).toString());
    }

    @ParameterizedTest
    @BooleanSource
    void derivationsOfATableFunctionKeepTheCollectionCorrelationAndTheStruct(final boolean withOrdinality) {
        final var sourceAlias = CorrelationIdentifier.of("source");
        final var arrayType = new Type.Array(false, Type.primitiveType(Type.TypeCode.INT, false));
        final var sourceType = Type.Record.fromFields(List.of(Type.Record.Field.of(arrayType, Optional.of("arr"))));
        final Value collectionValue = FieldValue.ofFieldName(QuantifiedObjectValue.of(sourceAlias, sourceType), "arr");
        final var explodeValue = new ExplodeValue(collectionValue, withOrdinality);

        final var derivations = new DerivationsProperty.DerivationsVisitor()
                .visitTableFunctionPlan(new RecordQueryTableFunctionPlan(explodeValue));

        Assertions.assertEquals(1, derivations.getResultValues().size());
        final Value derived = derivations.getResultValues().get(0);
        // the representative of some struct of the explode is still correlated to what the collection is correlated to
        Assertions.assertTrue(derived.getCorrelatedTo().contains(sourceAlias));
        // and it has the shape of the struct the explode flows: the first element of the collection, and the ordinal
        Assertions.assertEquals(explodeValue.getResultType(), derived.getResultType());
        Assertions.assertTrue(derived instanceof RecordConstructorValue);
        final List<? extends Value> columns = ImmutableList.copyOf(derived.getChildren());
        Assertions.assertEquals(withOrdinality ? 2 : 1, columns.size());
        Assertions.assertTrue(columns.get(0) instanceof FirstOrDefaultValue);
    }

    @ParameterizedTest
    @BooleanSource
    void protoRoundTrip(final boolean withOrdinality) {
        final var value = new ExplodeValue(reviewsOfRestaurant(), withOrdinality);
        final var valueProto = value.toValueProto(newSerializationContext());
        Assertions.assertTrue(valueProto.hasExplodeValue());

        final var deserialized = Value.fromValueProto(newSerializationContext(), valueProto);
        Assertions.assertInstanceOf(ExplodeValue.class, deserialized);
        Assertions.assertEquals(value, deserialized);
        Assertions.assertEquals(value.getResultType(), deserialized.getResultType());
        Assertions.assertEquals(withOrdinality, ((ExplodeValue)deserialized).hasOrdinality());
    }
}
