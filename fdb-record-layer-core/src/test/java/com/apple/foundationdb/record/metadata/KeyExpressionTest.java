/*
 * KeyExpressionTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.metadata;

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.UnstoredRecord;
import com.apple.foundationdb.record.metadata.ExpressionTestsProto.Customer;
import com.apple.foundationdb.record.metadata.ExpressionTestsProto.NestedField;
import com.apple.foundationdb.record.metadata.ExpressionTestsProto.SubString;
import com.apple.foundationdb.record.metadata.ExpressionTestsProto.SubStrings;
import com.apple.foundationdb.record.metadata.ExpressionTestsProto.TestScalarFieldAccess;
import com.apple.foundationdb.record.metadata.expressions.EmptyKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.FieldKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.FunctionKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression.FanType;
import com.apple.foundationdb.record.metadata.expressions.NestingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.SplitKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.ThenKeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static com.apple.foundationdb.record.metadata.Key.Evaluated.NullStandin.NULL;
import static com.apple.foundationdb.record.metadata.Key.Evaluated.concatenate;
import static com.apple.foundationdb.record.metadata.Key.Evaluated.scalar;
import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.concatenateFields;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.metadata.Key.Expressions.function;
import static com.apple.foundationdb.record.metadata.Key.Expressions.keyWithValue;
import static com.apple.foundationdb.record.metadata.Key.Expressions.value;
import static com.apple.foundationdb.record.metadata.expressions.EmptyKeyExpression.EMPTY;
import static com.apple.foundationdb.record.metadata.expressions.VersionKeyExpression.VERSION;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Tests for {@link KeyExpression}.
 */
public class KeyExpressionTest {

    public static List<Key.Evaluated> evaluate(@Nonnull KeyExpression expression, @Nullable Message record) {
        return expression.evaluate(new UnstoredRecord<>(record));
    }

    private static final TestScalarFieldAccess plantsBoxesAndBowls = TestScalarFieldAccess.newBuilder()
            .setField("Plants")
            .addRepeatMe("Boxes").addRepeatMe("Bowls")
            .build();

    private static final TestScalarFieldAccess emptyScalar = TestScalarFieldAccess.newBuilder().build();

    private static final TestScalarFieldAccess numbers = TestScalarFieldAccess.newBuilder()
            .setField("numbers")
            .addRepeatMe("one")
            .addRepeatMe("two")
            .addRepeatMe("three")
            .addRepeatMe("four")
            .addRepeatMe("five")
            .addRepeatMe("six")
            .addRepeatMe("seven")
            .addRepeatMe("eight")
            .addRepeatMe("nine")
            .build();

    private static final NestedField matryoshkaDolls = NestedField.newBuilder()
            .setRegularOldField("Grandmother")
            .setNesty(
                    NestedField.newBuilder()
                            .setRegularOldField("Mother")
                            .setRegularIntField(1066)
                            .addRepeatedField("lily")
                            .addRepeatedField("rose"))
            .addRepeatedNesty(
                    NestedField.newBuilder()
                            .setRegularOldField("Daughter")
                            .addRepeatedField("daffodil"))
            .addRepeatedNesty(
                    NestedField.newBuilder()
                            .setRegularOldField("Sister")
                            .addRepeatedField("lady slipper")
                            .addRepeatedField("orchid")
                            .addRepeatedField("morning glory"))
            .build();

    private static final NestedField emptyNested = NestedField.newBuilder().build();

    private static final NestedField lonelyDoll = NestedField.newBuilder()
            .setRegularOldField("Lonely")
            .setNesty(
                    NestedField.newBuilder()
            )
            .addRepeatedNesty(
                    NestedField.newBuilder()
            )
            .addRepeatedNesty(
                    NestedField.newBuilder()
            )
            .build();

    private static final Customer customer = Customer.newBuilder()
            .setId("customer1")
            .setFirstName("1 first name")
            .setLastName("1 last name")
            .addOrder(Customer.Order.newBuilder()
                    .setId("order1")
                    .addItem(
                            Customer.Order.Item.newBuilder()
                                    .setId("i1")
                                    .setName("a1"))
                    .addItem(
                            Customer.Order.Item.newBuilder()
                                    .setId("j1")
                                    .setName("a2"))
            )
            .addOrder(Customer.Order.newBuilder()
                    .setId("order2")
                    .addItem(
                            Customer.Order.Item.newBuilder()
                                    .setId("i2")
                                    .setName("b1"))
                    .addItem(
                            Customer.Order.Item.newBuilder()
                                    .setId("j2")
                                    .setName("b2"))
            )
            .addOrder(Customer.Order.newBuilder()
                    .setId("order3")
                    .addItem(
                            Customer.Order.Item.newBuilder()
                                    .setId("i3")
                                    .setName("c1"))
                    .addItem(
                            Customer.Order.Item.newBuilder()
                                    .setId("j3")
                                    .setName("c2"))
                    .addItem(
                            Customer.Order.Item.newBuilder()
                                    .setId("k3")
                                    .setName("c3"))
                    .addItem(
                            Customer.Order.Item.newBuilder()
                                    .setId("l3")
                                    .setName("c4"))
            ).build();

    public static final Customer emptyCustomer = Customer.newBuilder()
            .setId("the void")
            .build();

    public static final Customer aleph = Customer.newBuilder()
            .setId("aleph_numbers")
            .setFirstName("Infinity")
            .setLastName("Cardinalities")
            .addOrder(Customer.Order.newBuilder()
                    .setId("aleph null")
            )
            .addOrder(Customer.Order.newBuilder()
                    .setId("aleph one?")
            )
            .build();

    public static final SubStrings subString = SubStrings.newBuilder()
            .addSubstrings(SubString.newBuilder().setContent("scott").setStart(1).setEnd(3))
            .addSubstrings(SubString.newBuilder().setContent("mike").setStart(2).setEnd(4))
            .addSubstrings(SubString.newBuilder().setContent("jay").setStart(0).setEnd(1))
            .addSubstrings(SubString.newBuilder().setContent("christos").setStart(5).setEnd(8))
            .build();

    /**
     * One of the static analysis tools used flagged the EmptyKeyExpression class as not including
     * the serialVersionUID field even though it implemented {@link Serializable}. However,
     * that class doesn't implement it, as verified by this test. So the check was disabled for that
     * class. If this test ever fails, then a serial version UID should probably be added and that
     * check un-suppressed.
     */
    @Test
    public void testEmptyNotSerializable() {
        assertThat(EmptyKeyExpression.EMPTY, not(instanceOf(Serializable.class)));
    }

    @Test
    public void testScalarFieldAccess() throws Exception {
        final KeyExpression expression = field("field");
        expression.validate(TestScalarFieldAccess.getDescriptor());
        assertFalse(expression.createsDuplicates());
        assertEquals(Collections.singletonList(scalar("Plants")),
                evaluate(expression, plantsBoxesAndBowls));
        assertEquals(Collections.singletonList(Key.Evaluated.NULL),
                evaluate(expression, emptyScalar));
        assertEquals(Collections.singletonList(Key.Evaluated.NULL),
                evaluate(expression, null));
    }

    @Test
    public void testFunctions() throws Exception {
        final KeyExpression expression = function("substr", concat(field("field"), value(0), value(2)));
        expression.validate(TestScalarFieldAccess.getDescriptor());
        assertEquals(Collections.singletonList(scalar("Pl")),
                evaluate(expression, plantsBoxesAndBowls));
        assertEquals(Collections.singletonList(Key.Evaluated.NULL),
                evaluate(expression, emptyScalar));
        assertEquals(Collections.singletonList(Key.Evaluated.NULL),
                evaluate(expression, null));
    }

    @Test
    public void testFunctionTooFewArguments() throws Exception {
        assertThrows(KeyExpression.InvalidExpressionException.class, () -> {
            function("two_min_three_max", field("field"));
        });
    }

    @Test
    public void testFunctionTooManyArguments() throws Exception {
        assertThrows(KeyExpression.InvalidExpressionException.class, () -> {
            function("two_min_three_max", concat(field("field"), value(1), value(2), value(3)));
        });
    }

    @Test
    public void testFunctionWrongColumnCount() throws Exception {
        // two_min_three_max declares that it will return only one column, but it really returns the result of
        // the expression that is its argument, in this case, will return three columns.
        assertThrows(KeyExpression.InvalidResultException.class, () -> {
            final KeyExpression expression = function("two_min_three_max", concat(field("field"), value(1), value(2)));
            expression.validate(TestScalarFieldAccess.getDescriptor());
            evaluate(expression, plantsBoxesAndBowls);
        });
    }

    @Test
    public void testFunctionNotExists() throws Exception {
        assertThrows(KeyExpression.InvalidExpressionException.class,
                () -> function("fooberries", field("field")));
    }

    @Test
    public void testFunctionEvalToWrongType() throws Exception {
        assertThrows(KeyExpression.InvalidResultException.class,
                () -> {
                    final KeyExpression expression = function("substr", concat(field("field"), field("field"), value(2)));
                    expression.validate(TestScalarFieldAccess.getDescriptor());
                    evaluate(expression, plantsBoxesAndBowls);
                });
    }

    @Test
    public void testSubstrFunctionStaticFanout() throws Exception {
        final KeyExpression expression = function("substr", concat(field("repeat_me", FanType.FanOut),value(0), value(3)));
        expression.validate(TestScalarFieldAccess.getDescriptor());
        List<Key.Evaluated> results = evaluate(expression, plantsBoxesAndBowls);
        assertEquals(2, results.size(), "Wrong number of results");
        assertEquals(ImmutableList.of(Key.Evaluated.scalar("Box"), Key.Evaluated.scalar("Bow")), results);
    }

    @Test
    public void testSubstrFunctionDynamicFanout() throws Exception {
        final KeyExpression expression = function("substr",
                field("substrings", FanType.FanOut).nest(
                        concatenateFields("content", "start", "end")));
        expression.validate(SubStrings.getDescriptor());
        List<Key.Evaluated> results = evaluate(expression, subString);
        assertEquals(4, results.size(), "Wrong number of results");
        assertEquals(ImmutableList.of(
                    Key.Evaluated.scalar("co"),
                    Key.Evaluated.scalar("ke"),
                    Key.Evaluated.scalar("j"),
                    Key.Evaluated.scalar("tos")),
                results);
    }

    @Test
    public void testConcatenateSingleRepeatedField() throws Exception {
        final KeyExpression expression = field("repeat_me", FanType.Concatenate);
        expression.validate(TestScalarFieldAccess.getDescriptor());
        assertFalse(expression.createsDuplicates());
        assertEquals(Collections.singletonList(scalar(Arrays.asList("Boxes", "Bowls"))),
                evaluate(expression, plantsBoxesAndBowls));
        assertEquals(Collections.singletonList(scalar(Collections.emptyList())),
                evaluate(expression, emptyScalar));
        assertEquals(Collections.singletonList(scalar(Collections.emptyList())),
                evaluate(expression, null));
    }

    @Test
    public void testFieldThenConcatenateRepeated() throws Exception {
        final KeyExpression expression = Key.Expressions.concat(field("field"),
                field("repeat_me", FanType.Concatenate));
        expression.validate(TestScalarFieldAccess.getDescriptor());
        assertFalse(expression.createsDuplicates());
        assertEquals(Collections.singletonList(Key.Evaluated.concatenate("Plants", Arrays.asList("Boxes", "Bowls"))),
                evaluate(expression, plantsBoxesAndBowls));
        assertEquals(Collections.singletonList(Key.Evaluated.concatenate(NULL, Collections.emptyList())),
                evaluate(expression, emptyScalar));
        assertEquals(Collections.singletonList(Key.Evaluated.concatenate(NULL, Collections.emptyList())),
                evaluate(expression, null));
    }

    @Test
    public void testFanSingleRepeatedField() throws Exception {
        final KeyExpression expression = field("repeat_me", FanType.FanOut);
        expression.validate(TestScalarFieldAccess.getDescriptor());
        assertTrue(expression.createsDuplicates());
        assertEquals(Arrays.asList(scalar("Boxes"), scalar("Bowls")),
                evaluate(expression, plantsBoxesAndBowls));
        assertEquals(Collections.emptyList(),
                evaluate(expression, emptyScalar));
        assertEquals(Collections.emptyList(),
                evaluate(expression, null));
    }

    @Test
    public void testValidateFanRequiresRepeated() throws Exception {
        assertThrows(KeyExpression.InvalidExpressionException.class, () -> {
            field("field", FanType.FanOut).validate(TestScalarFieldAccess.getDescriptor());
        });
    }

    @Test
    public void testValidateConcatenateRequiresRepeated() throws Exception {
        assertThrows(KeyExpression.InvalidExpressionException.class, () -> {
            field("field", FanType.Concatenate).validate(TestScalarFieldAccess.getDescriptor());
        });
    }

    @Test
    public void testValidateRepeatedRequiresFanType() throws Exception {
        assertThrows(KeyExpression.InvalidExpressionException.class, () -> {
            field("repeat_me").validate(TestScalarFieldAccess.getDescriptor());
        });
    }

    @Test
    public void testValidateMissingField() throws Exception {
        assertThrows(KeyExpression.InvalidExpressionException.class, () -> {
            field("no_field_here").validate(TestScalarFieldAccess.getDescriptor());
        });
    }

    @Test
    public void testScalarThenFanned() throws Exception {
        final KeyExpression expression = concat(
                field("field"),
                field("repeat_me", FanType.FanOut));
        expression.validate(TestScalarFieldAccess.getDescriptor());
        assertTrue(expression.createsDuplicates());
        assertEquals(Arrays.asList(
                        concatenate("Plants", "Boxes"),
                        concatenate("Plants", "Bowls")),
                evaluate(expression, plantsBoxesAndBowls));
        assertEquals(Collections.emptyList(),
                evaluate(expression, emptyScalar));
        assertEquals(Collections.emptyList(),
                evaluate(expression, null));
    }

    @Test
    public void testFannedThenScalar() throws Exception {
        final KeyExpression expression = concat(
                field("repeat_me", FanType.FanOut),
                field("field"));
        expression.validate(TestScalarFieldAccess.getDescriptor());
        assertTrue(expression.createsDuplicates());
        assertEquals(Arrays.asList(
                        concatenate("Boxes", "Plants"),
                        concatenate("Bowls", "Plants")),
                evaluate(expression, plantsBoxesAndBowls));
        assertEquals(Collections.emptyList(),
                evaluate(expression, emptyScalar));
        assertEquals(Collections.emptyList(),
                evaluate(expression, null));
    }

    @Test
    public void testValidateThenFailsOnFirst() throws Exception {
        assertThrows(KeyExpression.InvalidExpressionException.class, () -> {
            concat(field("repeat_me"), field("field")).validate(TestScalarFieldAccess.getDescriptor());
        });
    }

    @Test
    public void testValidateThenFailsOnSecond() throws Exception {
        assertThrows(KeyExpression.InvalidExpressionException.class, () -> {
            concat(field("repeat_me", FanType.FanOut), field("field", FanType.FanOut))
                    .validate(TestScalarFieldAccess.getDescriptor());
        });
    }

    @Test
    public void testNestedScalars() throws Exception {
        final KeyExpression expression = field("nesty").nest("regular_old_field");
        expression.validate(NestedField.getDescriptor());
        assertFalse(expression.createsDuplicates());
        assertEquals(Collections.singletonList(
                        scalar("Mother")),
                evaluate(expression, matryoshkaDolls));
        assertEquals(Collections.singletonList(Key.Evaluated.NULL),
                evaluate(expression, emptyNested));
        assertEquals(Collections.singletonList(Key.Evaluated.NULL),
                evaluate(expression, lonelyDoll));
        assertEquals(Collections.singletonList(Key.Evaluated.NULL),
                evaluate(expression, null));
    }

    @Test
    public void testNestedRepeats() throws Exception {
        final KeyExpression expression =
                field("repeated_nesty", FanType.FanOut).nest("regular_old_field");
        expression.validate(NestedField.getDescriptor());
        assertTrue(expression.createsDuplicates());
        assertEquals(Arrays.asList(
                        scalar("Daughter"),
                        scalar("Sister")),
                evaluate(expression, matryoshkaDolls));
        assertEquals(Collections.emptyList(),
                evaluate(expression, emptyNested));
        assertEquals(Arrays.asList(Key.Evaluated.NULL, Key.Evaluated.NULL),
                evaluate(expression, lonelyDoll));
        assertEquals(Collections.emptyList(),
                evaluate(expression, null));
    }

    @Test
    public void testNestedThenRepeats() throws Exception {
        final KeyExpression expression =
                field("nesty").nest("repeated_field", FanType.FanOut);
        expression.validate(NestedField.getDescriptor());
        assertTrue(expression.createsDuplicates());
        assertEquals(Arrays.asList(
                        scalar("lily"),
                        scalar("rose")),
                evaluate(expression, matryoshkaDolls));
        assertEquals(Collections.emptyList(),
                evaluate(expression, emptyNested));
        assertEquals(Collections.emptyList(),
                evaluate(expression, lonelyDoll));
        assertEquals(Collections.emptyList(),
                evaluate(expression, null));
    }

    @Test
    public void testNestedThenRepeatsConcatentated() throws Exception {
        final KeyExpression expression =
                field("nesty").nest("repeated_field", FanType.Concatenate);
        expression.validate(NestedField.getDescriptor());
        assertFalse(expression.createsDuplicates());
        assertEquals(Collections.singletonList(scalar(Arrays.asList("lily", "rose"))),
                evaluate(expression, matryoshkaDolls));
        assertEquals(Collections.singletonList(scalar(Collections.emptyList())),
                evaluate(expression, emptyNested));
        assertEquals(Collections.singletonList(scalar(Collections.emptyList())),
                evaluate(expression, lonelyDoll));
        assertEquals(Collections.singletonList(scalar(Collections.emptyList())),
                evaluate(expression, null));
    }

    @Test
    public void testNestedThenConatenatedFields() throws Exception {
        final KeyExpression expression = field("nesty").nest(concatenateFields("regular_old_field", "regular_int_field"));
        expression.validate(NestedField.getDescriptor());
        assertFalse(expression.createsDuplicates());
        assertEquals(Collections.singletonList(Key.Evaluated.concatenate("Mother", 1066)),
                evaluate(expression, matryoshkaDolls));
        assertEquals(Collections.singletonList(Key.Evaluated.concatenate(NULL, NULL)),
                evaluate(expression, emptyNested));
        assertEquals(Collections.singletonList(Key.Evaluated.concatenate(NULL, NULL)),
                evaluate(expression, lonelyDoll));
        assertEquals(Collections.singletonList(Key.Evaluated.concatenate(NULL, NULL)),
                evaluate(expression, null));
    }

    @Test
    public void testInvalidFanOnNested() throws Exception {
        assertThrows(KeyExpression.InvalidExpressionException.class, () -> {
            field("nesty").nest("regular_old_field", FanType.FanOut).validate(NestedField.getDescriptor());
        });
    }

    @Test
    public void testInvalidFanOnParentNested() throws Exception {
        assertThrows(KeyExpression.InvalidExpressionException.class, () -> {
            field("repeated_nesty", FanType.Concatenate).nest("regular_old_field").validate(NestedField.getDescriptor());
        });
    }

    @Test
    public void testInvalidDoubleNested() throws Exception {
        assertThrows(KeyExpression.InvalidExpressionException.class, () -> {
            field("nesty").nest(field("nesty").nest("regular_old_field", FanType.FanOut)).validate(NestedField.getDescriptor());
        });
    }

    @Test
    public void testValidDoubleNested() throws Exception {
        field("nesty").nest(field("nesty").nest("repeated_field", FanType.FanOut)).validate(NestedField.getDescriptor());
    }

    @Test
    public void testValidDoubleNested2() throws Exception {
        field("nesty2").nest(field("nesty3").nest("last_field")).validate(NestedField.getDescriptor());
    }

    @Test
    public void testNestWithParentField() throws Exception {
        final KeyExpression expression = concat(
                field("regular_old_field"),
                field("repeated_nesty", FanType.FanOut).nest("regular_old_field"));
        expression.validate(NestedField.getDescriptor());
        assertTrue(expression.createsDuplicates());
        assertEquals(Arrays.asList(
                        concatenate("Grandmother", "Daughter"),
                        concatenate("Grandmother", "Sister")),
                evaluate(expression, matryoshkaDolls));
        assertEquals(Collections.emptyList(),
                evaluate(expression, emptyNested));
        assertEquals(Arrays.asList(
                Key.Evaluated.concatenate("Lonely", NULL),
                Key.Evaluated.concatenate("Lonely", NULL)),
                evaluate(expression, lonelyDoll));
        assertEquals(Collections.emptyList(),
                evaluate(expression, null));
    }

    @Test
    public void testNestWithParentField2() throws Exception {
        final KeyExpression expression =
                field("repeated_nesty", FanType.FanOut).nest(
                        field("regular_old_field"),
                        field("repeated_field", FanType.FanOut));
        expression.validate(NestedField.getDescriptor());
        assertTrue(expression.createsDuplicates());
        assertEquals(Arrays.asList(
                        concatenate("Daughter", "daffodil"),
                        concatenate("Sister", "lady slipper"),
                        concatenate("Sister", "orchid"),
                        concatenate("Sister", "morning glory")),
                evaluate(expression, matryoshkaDolls));
        assertEquals(Collections.emptyList(),
                evaluate(expression, emptyNested));
        assertEquals(Collections.emptyList(),
                evaluate(expression, lonelyDoll));
        assertEquals(Collections.emptyList(),
                evaluate(expression, null));
    }

    @Test
    public void testDoubleNested() throws Exception {
        final KeyExpression expression = concat(
                field("id"),
                field("order", FanType.FanOut).nest(
                        field("id"),
                        field("item", FanType.FanOut).nest(
                                field("id"),
                                field("name")
                        )),
                field("first_name"),
                field("last_name"));
        expression.validate(Customer.getDescriptor());
        assertTrue(expression.createsDuplicates());
        // note Same as testDoubleNestedWithExtraConcats
        assertEquals(Arrays.asList(
                        concatenate("customer1", "order1", "i1", "a1", "1 first name", "1 last name"),
                        concatenate("customer1", "order1", "j1", "a2", "1 first name", "1 last name"),
                        concatenate("customer1", "order2", "i2", "b1", "1 first name", "1 last name"),
                        concatenate("customer1", "order2", "j2", "b2", "1 first name", "1 last name"),
                        concatenate("customer1", "order3", "i3", "c1", "1 first name", "1 last name"),
                        concatenate("customer1", "order3", "j3", "c2", "1 first name", "1 last name"),
                        concatenate("customer1", "order3", "k3", "c3", "1 first name", "1 last name"),
                        concatenate("customer1", "order3", "l3", "c4", "1 first name", "1 last name")),
                evaluate(expression, customer)
        );
        assertEquals(Collections.emptyList(),
                evaluate(expression, emptyCustomer));
        assertEquals(Collections.emptyList(),
                evaluate(expression, aleph));
        assertEquals(Collections.emptyList(),
                evaluate(expression, null));
    }

    @Test
    public void testDoubleNestedWithExtraConcats() throws Exception {
        final KeyExpression expressionWithConcats = concat(
                field("id"),
                field("order", FanType.FanOut).nest(
                        concat(field("id"),
                                field("item", FanType.FanOut).nest(concat(
                                                field("id"),
                                                field("name"))
                                ))),
                field("first_name"),
                field("last_name"));
        final KeyExpression expressionWithoutConcats = concat(
                field("id"),
                field("order", FanType.FanOut).nest(
                        field("id"),
                        field("item", FanType.FanOut).nest(
                                field("id"),
                                field("name")
                        )),
                field("first_name"),
                field("last_name"));
        expressionWithConcats.validate(Customer.getDescriptor());
        assertTrue(expressionWithConcats.createsDuplicates());
        expressionWithoutConcats.validate(Customer.getDescriptor());
        assertTrue(expressionWithoutConcats.createsDuplicates());
        assertEquals(evaluate(expressionWithoutConcats, customer),
                evaluate(expressionWithConcats, customer));
        assertEquals(evaluate(expressionWithoutConcats, emptyCustomer),
                evaluate(expressionWithConcats, emptyCustomer));
        assertEquals(evaluate(expressionWithoutConcats, aleph),
                evaluate(expressionWithConcats, aleph));
        assertEquals(Collections.emptyList(),
                evaluate(expressionWithoutConcats, null));
        assertEquals(evaluate(expressionWithoutConcats, null),
                evaluate(expressionWithConcats, null));
    }

    @Test
    public void testThenFlattens() throws Exception {
        final KeyExpression concat = concat(field("f1"),
                concat(field("f2"), field("f3")),
                field("f4"));
        ThenKeyExpression then = (ThenKeyExpression) concat;
        assertFalse(then.createsDuplicates());
        assertEquals(4, then.getChildren().size());
        for (KeyExpression child : then.getChildren()) {
            if (child instanceof ThenKeyExpression) {
                fail("Expected no instances of Then, got one " + child.getClass());
            }
        }
    }

    @Test
    public void testSerializeField() throws Exception {
        final FieldKeyExpression f1 = field("f1", FanType.FanOut);
        final FieldKeyExpression f1Deserialized = new FieldKeyExpression(f1.toProto());
        assertEquals("f1", f1Deserialized.getFieldName());
        assertEquals(FanType.FanOut, f1Deserialized.getFanType());
    }

    @Test
    public void testSerializeThen() throws Exception {
        final ThenKeyExpression concat = concat(field("f1"), field("f2"));
        final ThenKeyExpression then = new ThenKeyExpression(concat.toProto());
        assertEquals(2, then.getChildren().size());
        assertEquals("f2", ((FieldKeyExpression)then.getChildren().get(1)).getFieldName());
    }

    @Test
    public void testSerializeNesting() throws Exception {
        final NestingKeyExpression nest = field("f1").nest(field("f2", FanType.FanOut).nest("f3"));
        final NestingKeyExpression reserialized = new NestingKeyExpression(nest.toProto());
        assertEquals("f1", reserialized.getParent().getFieldName());
        final NestingKeyExpression child = (NestingKeyExpression) reserialized.getChild();
        assertEquals("f2", child.getParent().getFieldName());
        assertEquals(FanType.FanOut, child.getParent().getFanType());
    }

    @Test
    public void testSplit() throws Exception {
        final SplitKeyExpression split = field("repeat_me", FanType.FanOut).split(3);
        split.validate(TestScalarFieldAccess.getDescriptor());
        assertEquals(Arrays.asList(
                concatenate("one", "two", "three"),
                concatenate("four", "five", "six"),
                concatenate("seven", "eight", "nine")),
                evaluate(split, numbers));
        assertEquals(Collections.emptyList(), evaluate(split, null));
    }

    @Test
    public void testSplitBad() throws Exception {
        assertThrows(RecordCoreException.class, () -> {
            final SplitKeyExpression split = field("repeat_me", FanType.FanOut).split(4);
            split.validate(TestScalarFieldAccess.getDescriptor());
            evaluate(split, numbers);
        });
    }

    @Test
    public void testSplitConcat() throws Exception {
        final ThenKeyExpression splitConcat = concat(field("field"),
                field("repeat_me", FanType.FanOut).split(3));
        splitConcat.validate(TestScalarFieldAccess.getDescriptor());
        assertEquals(Arrays.asList(
                concatenate("numbers", "one", "two", "three"),
                concatenate("numbers", "four", "five", "six"),
                concatenate("numbers", "seven", "eight", "nine")),
                evaluate(splitConcat, numbers));
    }

    public static Stream<Arguments> getPrefixKeyComparisons() {
        final KeyExpression nestedKeyWithValue = keyWithValue(field("a", FanType.FanOut).nest(
                        concat(field("b"), field("c"), field("d"))), 2);

        return Stream.of(
                Arguments.of(field("a"),
                        concat(field("a"), field("b"), field("c")),
                        true),
                Arguments.of(field("x"),
                        concat(field("a"), field("b"), field("c")),
                        false),
                Arguments.of(concat(concat(field("a"), EMPTY), EMPTY),
                        field("a"),
                        true),
                Arguments.of(field("a"),
                        concat(concat(field("a"), EMPTY), EMPTY),
                        true),
                Arguments.of(concat(field("a"), field("b")),
                        concat(field("a"), concat(field("b"), field("c"))),
                        true),
                Arguments.of(concat(field("a").nest("b"), field("c")),
                        concat(field("a").nest("b"), concat(field("c"), field("d"))),
                        true),
                Arguments.of(field("a").nest("b"),
                        concat(field("a").nest("b"), field("a").nest("c")),
                        true),
                Arguments.of(field("a").nest("b"),
                        field("a").nest(concat(field("b"), field("c"))),
                        true),
                Arguments.of(concat(field("a"), field("b")),
                        concat(field("a"), new GroupingKeyExpression(concat(field("b"), field("c")), 1)),
                        true),
                Arguments.of(concat(field("a"), field("b"), field("c")),
                        new GroupingKeyExpression(concat(field("a"), field("b"), field("c")), 1),
                        false),
                Arguments.of(new GroupingKeyExpression(concat(field("a"), field("b")), 1),
                        new GroupingKeyExpression(concat(field("a"), field("b")), 1),
                        true), // a grouping should not be a prefix, unless both prefix and key are identical
                Arguments.of(new GroupingKeyExpression(concat(field("a"), field("b")), 1),
                        new GroupingKeyExpression(concat(field("a"), field("b"), field("c")), 1),
                        false), // a grouping should not be a prefix, unless both prefix and key are identical
                Arguments.of(field("a"),
                        field("a"),
                        true),
                Arguments.of(field("a", FanType.FanOut),
                        field("a", FanType.FanOut),
                        true),
                Arguments.of(field("a", FanType.Concatenate),
                        field("a", FanType.Concatenate),
                        true),
                Arguments.of(field("a", FanType.FanOut),
                        field("a", FanType.Concatenate),
                        false),
                Arguments.of(field("a", FanType.FanOut),
                        field("a", FanType.None),
                        false),
                Arguments.of(field("a", FanType.Concatenate),
                        field("a", FanType.FanOut),
                        false),
                Arguments.of(field("a", FanType.Concatenate),
                        field("a", FanType.None),
                        false),
                Arguments.of(field("a", FanType.None),
                        field("a", FanType.Concatenate),
                        false),
                Arguments.of(field("a", FanType.None),
                        field("a", FanType.FanOut),
                        false),
                Arguments.of(field("a", FanType.FanOut).nest("b"),
                        field("a", FanType.FanOut).nest(concat(field("b"), field("c"))),
                        true),
                Arguments.of(field("a", FanType.FanOut).nest("b"),
                        concat(field("a", FanType.FanOut).nest("b"), field("a", FanType.FanOut).nest("c")),
                        true),
                Arguments.of(field("a", FanType.FanOut).nest(concat(field("b"), field("c"))),
                        concat(field("a", FanType.FanOut).nest("b"), field("a", FanType.FanOut).nest("c")),
                        false),
                Arguments.of(concat(field("a"), VERSION),
                        concat(field("a"), field("b")),
                        false),
                Arguments.of(concat(field("a"), VERSION),
                        concat(field("a"), VERSION),
                        true),
                Arguments.of(field("a").split(3),
                        concat(field("a").split(3), field("b"), field("c")),
                        true),
                Arguments.of(field("a").split(2),
                        field("a").split(3),
                        false),
                Arguments.of(field("a").split(3),
                        field("a").split(2),
                        false),
                Arguments.of(keyWithValue(concat(field("a"), field("b")), 1),
                        concat(field("a"), field("c")),
                        true),
                Arguments.of(keyWithValue(concat(field("a"), field("b")), 1),
                        field("a"),
                        true),
                Arguments.of(keyWithValue(concat(field("a"), field("b"), field("c")), 2),
                        concat(field("a"), field("c")), false),
                Arguments.of(concat(field("a"), field("b")),
                        keyWithValue(concat(field("a"), field("b"), field("c")), 2),
                        true),
                Arguments.of(concat(field("a"), field("b")),
                        keyWithValue(concat(field("a"), field("b")), 1),
                        false),
                Arguments.of(field("a", FanType.FanOut).nest(field("b")),
                        nestedKeyWithValue,
                        true),
                Arguments.of(field("a", FanType.FanOut).nest(concat(field("b"), field("c"))),
                        nestedKeyWithValue,
                        true),
                Arguments.of(field("a", FanType.FanOut).nest(
                        concat(field("b"), field("c"), field("d"))),
                        nestedKeyWithValue,
                        false),
                Arguments.of(concat(field("a", FanType.FanOut).nest(
                        field("b")), field("a", FanType.FanOut).nest("b")),
                        nestedKeyWithValue,
                        false),
                Arguments.of(concat(field("a", FanType.FanOut).nest(
                        field("b")), field("a", FanType.FanOut).nest("c")),
                        nestedKeyWithValue,
                        false));
    }

    @ParameterizedTest
    @MethodSource("getPrefixKeyComparisons")
    public void testIsPrefixKey(@Nonnull KeyExpression prefix, @Nonnull KeyExpression key, boolean shouldBePrefix) {
        assertEquals(shouldBePrefix, prefix.isPrefixKey(key));
    }

    /**
     * Function registry for {@link TwoMinThreeMaxFunction}.
     */
    @AutoService(FunctionKeyExpression.Factory.class)
    public static class TestFunctionRegistry implements FunctionKeyExpression.Factory {
        @Nonnull
        @Override
        public List<FunctionKeyExpression.Builder> getBuilders() {
            return Lists.newArrayList(
                    new FunctionKeyExpression.BiFunctionBuilder("substr", SubstrFunction::new),
                    new FunctionKeyExpression.BiFunctionBuilder("two_min_three_max", TwoMinThreeMaxFunction::new));
        }
    }

    /**
     * Function that limits the number of arguments.
     */
    public static class TwoMinThreeMaxFunction extends FunctionKeyExpression {
        public TwoMinThreeMaxFunction(@Nonnull String name, @Nonnull KeyExpression arguments) {
            super(name, arguments);
        }

        @Override
        public int getMinArguments() {
            return 2;
        }

        @Override
        public int getMaxArguments() {
            return 3;
        }

        @Nonnull
        @Override
        public <M extends Message> List<Key.Evaluated> evaluateFunction(@Nullable FDBRecord<M> record,
                                                                        @Nullable Message message,
                                                                        @Nonnull Key.Evaluated arguments) {
            return Collections.singletonList(arguments);
        }

        @Override
        public boolean createsDuplicates() {
            return false;
        }

        @Override
        public int getColumnSize() {
            return 1;
        }
    }

    /**
     * Function that computes substring.
     */
    public static class SubstrFunction extends FunctionKeyExpression {
        public SubstrFunction(@Nonnull String name, @Nonnull KeyExpression arguments) {
            super(name, arguments);
        }

        @Override
        public int getMinArguments() {
            return 2;
        }

        @Override
        public int getMaxArguments() {
            return 3;
        }

        @Nonnull
        @Override
        public <M extends Message> List<Key.Evaluated> evaluateFunction(@Nullable FDBRecord<M> record,
                                                                        @Nullable Message message,
                                                                        @Nonnull Key.Evaluated arguments) {
            final String value = arguments.getString(0);
            final Number startIdx = arguments.getObject(1, Number.class);
            final Number endIdx = (arguments.size() > 2) ? arguments.getObject(2, Number.class) : null;

            if (value == null || startIdx == null || endIdx == null) {
                return Collections.singletonList(Key.Evaluated.NULL);
            }

            return Collections.singletonList(Key.Evaluated.scalar(
                    value.substring(startIdx.intValue(), endIdx == null ? value.length() : endIdx.intValue())));
        }

        @Override
        public boolean createsDuplicates() {
            return false;
        }

        @Override
        public int getColumnSize() {
            return 1;
        }
    }
}
