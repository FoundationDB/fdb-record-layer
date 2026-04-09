/*
 * RenameFieldsVisitorTest.java
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

package com.apple.foundationdb.record.metadata.expressions.visitors;

import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.TestRecordsDoubleNestedProto;
import com.apple.foundationdb.record.TestRecordsWithHeaderProto;
import com.apple.foundationdb.record.evolution.TestMergedNestedTypesProto;
import com.apple.foundationdb.record.evolution.TestSplitNestedTypesProto;
import com.apple.foundationdb.record.evolution.TestUnmergedNestedTypesProto;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.MetaDataException;
import com.apple.foundationdb.record.metadata.expressions.DimensionsKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.FieldKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.NestingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.SplitKeyExpression;
import com.apple.foundationdb.record.util.pair.NonnullPair;
import com.apple.test.RandomizedTestUtils;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.concatenateFields;
import static com.apple.foundationdb.record.metadata.Key.Expressions.empty;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.metadata.Key.Expressions.function;
import static com.apple.foundationdb.record.metadata.Key.Expressions.keyWithValue;
import static com.apple.foundationdb.record.metadata.Key.Expressions.list;
import static com.apple.foundationdb.record.metadata.Key.Expressions.recordType;
import static com.apple.foundationdb.record.metadata.Key.Expressions.value;
import static com.apple.foundationdb.record.metadata.Key.Expressions.version;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.fail;

/**
 * Tests of the {@link RenameFieldsVisitor}. This generates key expressions subject to certain field renamings, and then
 * validates that the visitor will generate the expected expression.
 */
class RenameFieldsVisitorTest {
    private static final int MAX_RECURSION_DEPTH = 4;

    // Tests for collecting FieldRenames

    @Test
    void renamesForSelfReturnsIdentity() {
        assertThat(FieldRenames.constructFor(TestRecords1Proto.MySimpleRecord.getDescriptor(), TestRecords1Proto.MySimpleRecord.getDescriptor()))
                .matches(FieldRenames::isIdentity)
                .isEqualTo(FieldRenames.identity())
                .hasSameHashCodeAs(FieldRenames.identity())
                .hasToString("FieldRenaming[]");
    }

    @Test
    void basicRenameCollection() {
        final Descriptors.Descriptor remappedSimpleRecord = remapMySimpleRecord(Map.of("str_value_indexed", "str_value_indexed__1", "repeater", "repeater__2")).getPayload();
        final FieldRenames renames = FieldRenames.constructFor(TestRecords1Proto.MySimpleRecord.getDescriptor(), remappedSimpleRecord);
        final FieldRenames expectedRenames = FieldRenames.newBuilder()
                .putRenamedField(TestRecords1Proto.MySimpleRecord.getDescriptor(), remappedSimpleRecord, "str_value_indexed", "str_value_indexed__1")
                .putRenamedField(TestRecords1Proto.MySimpleRecord.getDescriptor(), remappedSimpleRecord, "repeater", "repeater__2")
                .build();
        assertThat(renames)
                .matches(Predicate.not(FieldRenames::isIdentity))
                .isEqualTo(expectedRenames)
                .hasSameHashCodeAs(expectedRenames);
        assertThat(renames.getRenamingForTypes(TestRecords1Proto.MySimpleRecord.getDescriptor(), remappedSimpleRecord))
                .containsExactlyInAnyOrderEntriesOf(Map.of("str_value_indexed", "str_value_indexed__1", "repeater", "repeater__2"));
    }

    @Test
    void renameIgnoresExtraFieldsButComplainsIfMissing() {
        final Descriptors.Descriptor descriptorWithExtraField = mutateSingleType(TestRecords1Proto.getDescriptor(), "MySimpleRecord", descriptor -> {
            final DescriptorProtos.DescriptorProto.Builder descriptorProto = descriptor.toProto().toBuilder();
            descriptorProto.addFieldBuilder()
                    .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
                    .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_SINT32)
                    .setName("blah")
                    .setNumber(1000);
            return descriptorProto.build();
        }).getPayload();
        FieldRenames renames = FieldRenames.constructFor(TestRecords1Proto.MySimpleRecord.getDescriptor(), descriptorWithExtraField);
        assertThat(renames)
                .matches(FieldRenames::isIdentity)
                .isEqualTo(FieldRenames.identity())
                .hasSameHashCodeAs(FieldRenames.identity());
        assertThat(renames.getRenamingForTypes(TestRecords1Proto.MySimpleRecord.getDescriptor(), descriptorWithExtraField))
                .isEmpty();

        assertThatCode(() -> FieldRenames.constructFor(descriptorWithExtraField, TestRecords1Proto.MySimpleRecord.getDescriptor()))
                .isInstanceOf(MetaDataException.class)
                .hasMessageContaining("target descriptor missing field");
    }

    @Test
    void traverseNestedFields() {
        final Descriptors.FileDescriptor renamedFile = mutateFileDescriptor(TestRecordsWithHeaderProto.getDescriptor(), mutatorByMap(Map.of(TestRecordsWithHeaderProto.HeaderRecord.getDescriptor(), Map.of("path", "path__2"))));
        final Descriptors.Descriptor myRecordWithRenamedChild = renamedFile.findMessageTypeByName("MyRecord");
        final Descriptors.Descriptor renamedHeaderRecord = renamedFile.findMessageTypeByName("HeaderRecord");
        final FieldRenames fieldRenames = FieldRenames.constructFor(TestRecordsWithHeaderProto.MyRecord.getDescriptor(), myRecordWithRenamedChild);
        final FieldRenames expectedFieldRenames = FieldRenames.newBuilder()
                .putRenamedField(TestRecordsWithHeaderProto.HeaderRecord.getDescriptor(), renamedHeaderRecord, "path", "path__2")
                .build();
        assertThat(fieldRenames)
                .matches(Predicate.not(FieldRenames::isIdentity))
                .isEqualTo(expectedFieldRenames)
                .hasSameHashCodeAs(expectedFieldRenames);
        assertThat(fieldRenames.getRenamingForTypes(TestRecordsWithHeaderProto.MyRecord.getDescriptor(), myRecordWithRenamedChild))
                .isEmpty();
        assertThat(fieldRenames.getRenamingForTypes(TestRecordsWithHeaderProto.HeaderRecord.getDescriptor(), renamedHeaderRecord))
                .hasSize(1)
                .containsEntry("path", "path__2");
    }

    @Test
    void errorsWhenNestedFieldIsUnnested() {
        final Descriptors.FileDescriptor mutatedFile = mutateFileDescriptor(TestRecordsWithHeaderProto.getDescriptor(), descriptor -> {
            if (!descriptor.equals(TestRecordsWithHeaderProto.MyRecord.getDescriptor())) {
                return descriptor.toProto();
            }
            final DescriptorProtos.DescriptorProto.Builder myRecordBuilder = descriptor.toProto().toBuilder();
            myRecordBuilder.getFieldBuilderList().stream()
                    .filter(field -> field.getType().equals(DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE))
                    .forEach(field -> field.clearTypeName().setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32));
            return myRecordBuilder.build();
        });

        final Descriptors.Descriptor mutatedMyRecord = mutatedFile.findMessageTypeByName("MyRecord");
        assertThatCode(() -> FieldRenames.constructFor(TestRecordsWithHeaderProto.MyRecord.getDescriptor(), mutatedMyRecord))
                .isInstanceOf(MetaDataException.class)
                .hasMessageContaining("target descriptor field is not a message type");
    }

    // Tests for running the RenameFieldsVisitor

    @Nonnull
    static Stream<Arguments> renameMySimpleRecord() {
        final Named<Descriptors.Descriptor> identity = Named.of("IDENTITY", TestRecords1Proto.MySimpleRecord.getDescriptor());
        final Named<Descriptors.Descriptor> renameNumValue2 = remapMySimpleRecord(Map.of("num_value_2", "num_value_2a"));
        final Named<Descriptors.Descriptor> renameStrValue = remapMySimpleRecord(Map.of("str_value_indexed", "str_value_indexed_b"));
        final Named<Descriptors.Descriptor> renameRepeater = remapMySimpleRecord(Map.of("repeater", "repeater_bis"));
        final Named<Descriptors.Descriptor> renameAll = remapMySimpleRecord(Map.of("num_value_2", "num_value_2c", "str_value_indexed", "str_value_indexed_c", "repeater", "repeater_bis"));

        // These key expressions are invariant to all transformations. Run them through with various kinds of mapping functions
        final Stream<Arguments> constantArgs = Stream.of(
                        empty(),
                        recordType(),
                        version(),
                        value(42L),
                        value("str_value_indexed"),
                        value(false)
                )
                .flatMap(key -> Stream.of(identity, renameNumValue2, renameStrValue, renameRepeater, renameAll)
                        .map(renamedDescriptor -> Arguments.of(key, renamedDescriptor, key)));

        // These key expressions can change. Take more care to assert on the transformations seen
        final Stream<Arguments> nonConstantArgs = Stream.of(
                // FieldKeyExpressions
                Arguments.of(field("num_value_2"), identity, field("num_value_2")),
                Arguments.of(field("num_value_2"), renameNumValue2, field("num_value_2a")),
                Arguments.of(field("num_value_2"), renameStrValue, field("num_value_2")),
                Arguments.of(field("num_value_2", KeyExpression.FanType.None, Key.Evaluated.NullStandin.NOT_NULL), identity, field("num_value_2")),
                Arguments.of(field("num_value_2", KeyExpression.FanType.None, Key.Evaluated.NullStandin.NULL), renameNumValue2, field("num_value_2a")),
                Arguments.of(field("repeater", KeyExpression.FanType.FanOut), identity, field("repeater", KeyExpression.FanType.FanOut)),
                Arguments.of(field("repeater", KeyExpression.FanType.FanOut), renameRepeater, field("repeater_bis", KeyExpression.FanType.FanOut)),
                Arguments.of(field("repeater", KeyExpression.FanType.FanOut), renameStrValue, field("repeater", KeyExpression.FanType.FanOut)),
                Arguments.of(field("repeater", KeyExpression.FanType.Concatenate), identity, field("repeater", KeyExpression.FanType.Concatenate)),
                Arguments.of(field("repeater", KeyExpression.FanType.Concatenate), renameRepeater, field("repeater_bis", KeyExpression.FanType.Concatenate)),
                Arguments.of(field("repeater", KeyExpression.FanType.Concatenate), renameNumValue2, field("repeater", KeyExpression.FanType.Concatenate)),

                // ThenKeyExpressions
                Arguments.of(concatenateFields("num_value_2", "str_value_indexed"), identity, concatenateFields("num_value_2", "str_value_indexed")),
                Arguments.of(concatenateFields("num_value_2", "str_value_indexed"), renameNumValue2, concatenateFields("num_value_2a", "str_value_indexed")),
                Arguments.of(concatenateFields("num_value_2", "str_value_indexed"), renameStrValue, concatenateFields("num_value_2", "str_value_indexed_b")),
                Arguments.of(concatenateFields("num_value_2", "str_value_indexed"), renameAll, concatenateFields("num_value_2c", "str_value_indexed_c")),

                // ListKeyExpressions
                Arguments.of(list(field("num_value_2"), field("str_value_indexed")), identity, list(field("num_value_2"), field("str_value_indexed"))),
                Arguments.of(list(field("num_value_2"), field("str_value_indexed")), renameNumValue2, list(field("num_value_2a"), field("str_value_indexed"))),
                Arguments.of(list(field("num_value_2"), field("str_value_indexed")), renameStrValue, list(field("num_value_2"), field("str_value_indexed_b"))),
                Arguments.of(list(field("num_value_2"), field("str_value_indexed")), renameAll, list(field("num_value_2c"), field("str_value_indexed_c"))),

                // KeyWithValueExpressions
                Arguments.of(keyWithValue(concatenateFields("num_value_2", "str_value_indexed"), 1), identity, keyWithValue(concatenateFields("num_value_2", "str_value_indexed"), 1)),
                Arguments.of(keyWithValue(concatenateFields("num_value_2", "str_value_indexed"), 1), renameNumValue2, keyWithValue(concatenateFields("num_value_2a", "str_value_indexed"), 1)),
                Arguments.of(keyWithValue(concatenateFields("num_value_2", "str_value_indexed"), 1), renameStrValue, keyWithValue(concatenateFields("num_value_2", "str_value_indexed_b"), 1)),
                Arguments.of(keyWithValue(concatenateFields("num_value_2", "str_value_indexed"), 1), renameAll, keyWithValue(concatenateFields("num_value_2c", "str_value_indexed_c"), 1)),
                Arguments.of(keyWithValue(concatenateFields("num_value_2", "str_value_indexed"), 0), renameAll, keyWithValue(concatenateFields("num_value_2c", "str_value_indexed_c"), 0)),
                Arguments.of(keyWithValue(concat(field("num_value_2"), field("repeater", KeyExpression.FanType.FanOut), field("str_value_indexed")), 2), identity, keyWithValue(concat(field("num_value_2"), field("repeater", KeyExpression.FanType.FanOut), field("str_value_indexed")), 2)),
                Arguments.of(keyWithValue(concat(field("num_value_2"), field("repeater", KeyExpression.FanType.FanOut), field("str_value_indexed")), 2), renameNumValue2, keyWithValue(concat(field("num_value_2a"), field("repeater", KeyExpression.FanType.FanOut), field("str_value_indexed")), 2)),
                Arguments.of(keyWithValue(concat(field("num_value_2"), field("repeater", KeyExpression.FanType.FanOut), field("str_value_indexed")), 2), renameStrValue, keyWithValue(concat(field("num_value_2"), field("repeater", KeyExpression.FanType.FanOut), field("str_value_indexed_b")), 2)),
                Arguments.of(keyWithValue(concat(field("num_value_2"), field("repeater", KeyExpression.FanType.FanOut), field("str_value_indexed")), 2), renameRepeater, keyWithValue(concat(field("num_value_2"), field("repeater_bis", KeyExpression.FanType.FanOut), field("str_value_indexed")), 2)),
                Arguments.of(keyWithValue(concat(field("num_value_2"), field("repeater", KeyExpression.FanType.FanOut), field("str_value_indexed")), 2), renameAll, keyWithValue(concat(field("num_value_2c"), field("repeater_bis", KeyExpression.FanType.FanOut), field("str_value_indexed_c")), 2)),

                // FunctionKeyExpressions
                Arguments.of(function("nada", field("num_value_2")), identity, function("nada", field("num_value_2"))),
                Arguments.of(function("nada", field("num_value_2")), renameNumValue2, function("nada", field("num_value_2a"))),

                // SplitKeyExpressions
                Arguments.of(new SplitKeyExpression(field("repeater", KeyExpression.FanType.FanOut), 2), identity, new SplitKeyExpression(field("repeater", KeyExpression.FanType.FanOut), 2)),
                Arguments.of(new SplitKeyExpression(field("repeater", KeyExpression.FanType.FanOut), 2), renameRepeater, new SplitKeyExpression(field("repeater_bis", KeyExpression.FanType.FanOut), 2)),
                Arguments.of(new SplitKeyExpression(field("repeater", KeyExpression.FanType.FanOut), 2), renameAll, new SplitKeyExpression(field("repeater_bis", KeyExpression.FanType.FanOut), 2)),

                // GroupingKeyExpressions
                Arguments.of(field("num_value_2").groupBy(field("str_value_indexed")), identity, field("num_value_2").groupBy(field("str_value_indexed"))),
                Arguments.of(field("num_value_2").groupBy(field("str_value_indexed")), renameNumValue2, field("num_value_2a").groupBy(field("str_value_indexed"))),
                Arguments.of(field("num_value_2").groupBy(field("str_value_indexed")), renameStrValue, field("num_value_2").groupBy(field("str_value_indexed_b"))),
                Arguments.of(field("num_value_2").groupBy(field("str_value_indexed")), renameAll, field("num_value_2c").groupBy(field("str_value_indexed_c")))
        );

        // Create some random ones as well
        final Stream<Arguments> randomArgs = RandomizedTestUtils.randomSeeds(0x5ca1ab1e, 0xfdb01234L).flatMap(seed -> {
            final Random random = new Random(seed);
            return Stream.generate(() -> mutateMySimpleRecord(randomMutator(random))).limit(5).flatMap(renamedDescriptor ->
                Stream.generate(() -> {
                    final NonnullPair<KeyExpression, KeyExpression> randomCase = randomExpressionWithRename(random, TestRecords1Proto.MySimpleRecord.getDescriptor(), renamedDescriptor.getPayload(), 0);
                    return Arguments.of(randomCase.getLeft(), renamedDescriptor, randomCase.getRight());
                }).limit(20)
            );
        });

        return Stream.concat(Stream.concat(constantArgs, nonConstantArgs), randomArgs);
    }

    @ParameterizedTest(name = "renameMySimpleRecord[{0}, {1}]")
    @MethodSource
    void renameMySimpleRecord(@Nonnull KeyExpression original, @Nonnull Descriptors.Descriptor renamedDescriptor, @Nonnull KeyExpression expected) {
        assertRenaming(original, TestRecords1Proto.MySimpleRecord.getDescriptor(), renamedDescriptor, expected);
    }

    @Nonnull
    static Stream<Arguments> renameOuterRecord() {
        // Create some static test cases. These focus on nesting cases, as the non-nested cases are covered by the randomized tests or by the
        // static tests in renameMySimpleRecord
        final Named<Descriptors.Descriptor> identity = Named.of("IDENTITY", TestRecordsDoubleNestedProto.OuterRecord.getDescriptor());
        final Named<Descriptors.Descriptor> swapInnerFooBar = remapOuterRecord(Map.of(TestRecordsDoubleNestedProto.OuterRecord.MiddleRecord.InnerRecord.getDescriptor(), Map.of("foo", "bar", "bar", "foo")));
        final Named<Descriptors.Descriptor> renameMiddleInner = remapOuterRecord(Map.of(TestRecordsDoubleNestedProto.OuterRecord.MiddleRecord.getDescriptor(), Map.of("inner", "inner_2b")));
        final Named<Descriptors.Descriptor> renameOuterMiddle = remapOuterRecord(Map.of(TestRecordsDoubleNestedProto.OuterRecord.getDescriptor(), Map.of("middle", "middle_c_261")));
        final Named<Descriptors.Descriptor> renameOuterManyMiddle = remapOuterRecord(Map.of(TestRecordsDoubleNestedProto.OuterRecord.getDescriptor(), Map.of("many_middle", "multi_middle")));

        final Stream<Arguments> staticArgs = Stream.of(
                // Expression 1: middle.inner.foo
                // Test renaming all three levels (and at no levels)
                Arguments.of(field("middle").nest(field("inner").nest("foo")), identity, field("middle").nest(field("inner").nest("foo"))),
                Arguments.of(field("middle").nest(field("inner").nest("foo")), swapInnerFooBar, field("middle").nest(field("inner").nest("bar"))),
                Arguments.of(field("middle").nest(field("inner").nest("foo")), renameMiddleInner, field("middle").nest(field("inner_2b").nest("foo"))),
                Arguments.of(field("middle").nest(field("inner").nest("foo")), renameOuterMiddle, field("middle_c_261").nest(field("inner").nest("foo"))),

                // Expression 2: middle.inner.(foo, bar)
                // Test renaming all three levels (and at no levels)
                Arguments.of(field("middle").nest(field("inner").nest(concatenateFields("foo", "bar"))), identity, field("middle").nest(field("inner").nest(concatenateFields("foo", "bar")))),
                Arguments.of(field("middle").nest(field("inner").nest(concatenateFields("foo", "bar"))), swapInnerFooBar, field("middle").nest(field("inner").nest(concatenateFields("bar", "foo")))),
                Arguments.of(field("middle").nest(field("inner").nest(concatenateFields("foo", "bar"))), renameMiddleInner, field("middle").nest(field("inner_2b").nest(concatenateFields("foo", "bar")))),
                Arguments.of(field("middle").nest(field("inner").nest(concatenateFields("foo", "bar"))), renameOuterMiddle, field("middle_c_261").nest(field("inner").nest(concatenateFields("foo", "bar")))),

                // Expression 3: other.outer.middle.(foo, bar)
                // Goes through the other record in order to get back to the original outer record
                Arguments.of(field("other").nest(field("outer").nest(field("middle").nest(field("inner").nest(concatenateFields("foo", "bar"))))), identity, field("other").nest(field("outer").nest(field("middle").nest(field("inner").nest(concatenateFields("foo", "bar")))))),
                Arguments.of(field("other").nest(field("outer").nest(field("middle").nest(field("inner").nest(concatenateFields("foo", "bar"))))), swapInnerFooBar, field("other").nest(field("outer").nest(field("middle").nest(field("inner").nest(concatenateFields("bar", "foo")))))),
                Arguments.of(field("other").nest(field("outer").nest(field("middle").nest(field("inner").nest(concatenateFields("foo", "bar"))))), renameMiddleInner, field("other").nest(field("outer").nest(field("middle").nest(field("inner_2b").nest(concatenateFields("foo", "bar")))))),
                Arguments.of(field("other").nest(field("outer").nest(field("middle").nest(field("inner").nest(concatenateFields("foo", "bar"))))), renameOuterMiddle, field("other").nest(field("outer").nest(field("middle_c_261").nest(field("inner").nest(concatenateFields("foo", "bar")))))),

                // Expression 4: many_middle.inner.foo
                // This tests that we do the right thing on a repeated parent field
                Arguments.of(field("many_middle", KeyExpression.FanType.FanOut).nest(field("inner").nest("foo")), identity, field("many_middle", KeyExpression.FanType.FanOut).nest(field("inner").nest("foo"))),
                Arguments.of(field("many_middle", KeyExpression.FanType.FanOut).nest(field("inner").nest("foo")), swapInnerFooBar, field("many_middle", KeyExpression.FanType.FanOut).nest(field("inner").nest("bar"))),
                Arguments.of(field("many_middle", KeyExpression.FanType.FanOut).nest(field("inner").nest("foo")), renameMiddleInner, field("many_middle", KeyExpression.FanType.FanOut).nest(field("inner_2b").nest("foo"))),
                Arguments.of(field("many_middle", KeyExpression.FanType.FanOut).nest(field("inner").nest("foo")), renameOuterManyMiddle, field("multi_middle", KeyExpression.FanType.FanOut).nest(field("inner").nest("foo")))
        );

        // Create some random arguments as well
        final Stream<Arguments> randomArgs = RandomizedTestUtils.randomSeeds(0x5ca1ab1e, 0xfdb01234L).flatMap(seed -> {
            final Random random = new Random(seed);
            return Stream.generate(() -> mutateOuterRecord(randomMutator(random))).limit(5).flatMap(renamedDescriptor ->
                    Stream.generate(() -> {
                        final NonnullPair<KeyExpression, KeyExpression> randomCase = randomExpressionWithRename(random, TestRecordsDoubleNestedProto.OuterRecord.getDescriptor(), renamedDescriptor.getPayload(), 0);
                        return Arguments.of(randomCase.getLeft(), renamedDescriptor, randomCase.getRight());
                    }).limit(20)
            );
        });

        return Stream.concat(staticArgs, randomArgs);
    }

    @ParameterizedTest(name = "renameOuterRecord[{0}, {1}]")
    @MethodSource
    void renameOuterRecord(@Nonnull KeyExpression original, @Nonnull Descriptors.Descriptor targetDescriptor, @Nonnull KeyExpression expected) {
        assertRenaming(original, TestRecordsDoubleNestedProto.OuterRecord.getDescriptor(), targetDescriptor, expected);
    }

    @Nonnull
    static Stream<Arguments> renameMiddleRecord() {
        // Create some static test cases. These are mainly designed to allow us to make sure the renaming visitor properly follows types
        final Named<Descriptors.Descriptor> identity = Named.of("IDENTITY", TestRecordsDoubleNestedProto.MiddleRecord.getDescriptor());
        final Named<Descriptors.Descriptor> renameMiddleOtherInt = remapMiddleRecord(Map.of(TestRecordsDoubleNestedProto.MiddleRecord.getDescriptor(), Map.of("other_int", "other_int_a")));
        final Named<Descriptors.Descriptor> renameOuterMiddleOtherInt = remapMiddleRecord(Map.of(TestRecordsDoubleNestedProto.OuterRecord.MiddleRecord.getDescriptor(), Map.of("other_int", "other_int_b")));
        final Named<Descriptors.Descriptor> renameBothOtherInts = remapMiddleRecord(Map.of(TestRecordsDoubleNestedProto.MiddleRecord.getDescriptor(), Map.of("other_int", "other_int_a"), TestRecordsDoubleNestedProto.OuterRecord.MiddleRecord.getDescriptor(), Map.of("other_int", "other_int_b")));
        final Named<Descriptors.Descriptor> renameBothOtherIntsAndSwapMiddles = remapMiddleRecord(
                Map.of(TestRecordsDoubleNestedProto.MiddleRecord.getDescriptor(), Map.of("other_int", "other_int_a", "middle", "other_middle", "other_middle", "middle"),
                        TestRecordsDoubleNestedProto.OuterRecord.MiddleRecord.getDescriptor(), Map.of("other_int", "other_int_b")));
        final Named<Descriptors.Descriptor> swapFooBar = remapMiddleRecord(Map.of(TestRecordsDoubleNestedProto.OuterRecord.MiddleRecord.InnerRecord.getDescriptor(), Map.of("foo", "bar", "bar", "foo")));
        final Named<Descriptors.Descriptor> swapMiddlesAndFooBar = remapMiddleRecord(Map.of(TestRecordsDoubleNestedProto.MiddleRecord.getDescriptor(), Map.of("middle", "other_middle", "other_middle", "middle"), TestRecordsDoubleNestedProto.OuterRecord.MiddleRecord.InnerRecord.getDescriptor(), Map.of("foo", "bar", "bar", "foo")));

        final Stream<Arguments> staticArgs = Stream.of(
                // Expression 1: middle.other_int, other_middle.other_int
                // Note that the first other_int is defined in MiddleRecord; the second is defined in OuterRecord.MiddleRecord
                Arguments.of(concat(field("middle").nest("other_int"), field("other_middle").nest("other_int")), identity, concat(field("middle").nest("other_int"), field("other_middle").nest("other_int"))),
                Arguments.of(concat(field("middle").nest("other_int"), field("other_middle").nest("other_int")), renameMiddleOtherInt, concat(field("middle").nest("other_int_a"), field("other_middle").nest("other_int"))),
                Arguments.of(concat(field("middle").nest("other_int"), field("other_middle").nest("other_int")), renameOuterMiddleOtherInt, concat(field("middle").nest("other_int"), field("other_middle").nest("other_int_b"))),
                Arguments.of(concat(field("middle").nest("other_int"), field("other_middle").nest("other_int")), renameBothOtherInts, concat(field("middle").nest("other_int_a"), field("other_middle").nest("other_int_b"))),
                Arguments.of(concat(field("middle").nest("other_int"), field("other_middle").nest("other_int")), renameBothOtherIntsAndSwapMiddles, concat(field("other_middle").nest("other_int_a"), field("middle").nest("other_int_b"))),

                // Expression 2: middle.other_middle.inner.foo, other_middle.inner.bar
                Arguments.of(concat(field("middle").nest(field("other_middle").nest(field("inner").nest("foo"))), field("other_middle").nest(field("inner").nest("bar"))), identity, concat(field("middle").nest(field("other_middle").nest(field("inner").nest("foo"))), field("other_middle").nest(field("inner").nest("bar")))),
                Arguments.of(concat(field("middle").nest(field("other_middle").nest(field("inner").nest("foo"))), field("other_middle").nest(field("inner").nest("bar"))), swapFooBar, concat(field("middle").nest(field("other_middle").nest(field("inner").nest("bar"))), field("other_middle").nest(field("inner").nest("foo")))),
                Arguments.of(concat(field("middle").nest(field("other_middle").nest(field("inner").nest("foo"))), field("other_middle").nest(field("inner").nest("bar"))), renameBothOtherIntsAndSwapMiddles, concat(field("other_middle").nest(field("middle").nest(field("inner").nest("foo"))), field("middle").nest(field("inner").nest("bar")))),
                Arguments.of(concat(field("middle").nest(field("other_middle").nest(field("inner").nest("foo"))), field("other_middle").nest(field("inner").nest("bar"))), swapMiddlesAndFooBar, concat(field("other_middle").nest(field("middle").nest(field("inner").nest("bar"))), field("middle").nest(field("inner").nest("foo"))))
        );

        // Create some random arguments as well
        final Stream<Arguments> randomArgs = RandomizedTestUtils.randomSeeds(0x5ca1ab1e, 0xfdb01234L).flatMap(seed -> {
            final Random random = new Random(seed);
            return Stream.generate(() -> mutateMiddleRecord(randomMutator(random))).limit(5).flatMap(renamedDescriptor ->
                    Stream.generate(() -> {
                        final NonnullPair<KeyExpression, KeyExpression> randomCase = randomExpressionWithRename(random, TestRecordsDoubleNestedProto.MiddleRecord.getDescriptor(), renamedDescriptor.getPayload(), 0);
                        return Arguments.of(randomCase.getLeft(), renamedDescriptor, randomCase.getRight());
                    }).limit(20)
            );
        });

        return Stream.concat(staticArgs, randomArgs);
    }

    @ParameterizedTest(name = "renameMiddleRecord[{0}, {1}]")
    @MethodSource
    void renameMiddleRecord(@Nonnull KeyExpression original, @Nonnull Descriptors.Descriptor targetDescriptor, @Nonnull KeyExpression expected) {
        assertRenaming(original, TestRecordsDoubleNestedProto.MiddleRecord.getDescriptor(), targetDescriptor, expected);
    }

    @Nonnull
    static Stream<Arguments> renameMergedRecord() {
        // Create some static test cases. See the test method for an explanation as to how these are supposed to be structured
        final Named<Descriptors.Descriptor> identity = Named.of("IDENTITY", TestMergedNestedTypesProto.MyRecord.getDescriptor());
        final Named<Descriptors.Descriptor> renameTopA = remapMergedRecord(Map.of(TestMergedNestedTypesProto.MyRecord.getDescriptor(), Map.of("a", "a_1")));
        final Named<Descriptors.Descriptor> renameTopB = remapMergedRecord(Map.of(TestMergedNestedTypesProto.MyRecord.getDescriptor(), Map.of("b", "b_1")));
        final Named<Descriptors.Descriptor> renameNestedA = remapMergedRecord(Map.of(TestMergedNestedTypesProto.OneTrueNested.getDescriptor(), Map.of("a", "a_2")));
        final Named<Descriptors.Descriptor> renameNestedB = remapMergedRecord(Map.of(TestMergedNestedTypesProto.OneTrueNested.getDescriptor(), Map.of("b", "b_2")));
        final Named<Descriptors.Descriptor> renameAll = remapMergedRecord(Map.of(
                TestMergedNestedTypesProto.MyRecord.getDescriptor(), Map.of("a", "a_1", "b", "b_1"),
                TestMergedNestedTypesProto.OneTrueNested.getDescriptor(), Map.of("a", "a_2", "b", "b_2")
        ));

        final Stream<Arguments> staticArgs = Stream.of(
                Arguments.of(field("a").nest("a"), identity, field("a").nest("a")),
                Arguments.of(field("a").nest("a"), renameTopA, field("a_1").nest("a")),
                Arguments.of(field("a").nest("a"), renameTopB, field("a").nest("a")),
                Arguments.of(field("a").nest("a"), renameNestedA, field("a").nest("a_2")),
                Arguments.of(field("a").nest("a"), renameNestedB, field("a").nest("a")),
                Arguments.of(field("a").nest("a"), renameAll, field("a_1").nest("a_2")),

                Arguments.of(field("b").nest("b"), identity, field("b").nest("b")),
                Arguments.of(field("b").nest("b"), renameTopA, field("b").nest("b")),
                Arguments.of(field("b").nest("b"), renameTopB, field("b_1").nest("b")),
                Arguments.of(field("b").nest("b"), renameNestedA, field("b").nest("b")),
                Arguments.of(field("b").nest("b"), renameNestedB, field("b").nest("b_2")),
                Arguments.of(field("b").nest("b"), renameAll, field("b_1").nest("b_2")),

                Arguments.of(field("a").nest("a").groupBy(field("b").nest("b")), identity, field("a").nest("a").groupBy(field("b").nest("b"))),
                Arguments.of(field("a").nest("a").groupBy(field("b").nest("b")), renameTopA, field("a_1").nest("a").groupBy(field("b").nest("b"))),
                Arguments.of(field("a").nest("a").groupBy(field("b").nest("b")), renameTopB, field("a").nest("a").groupBy(field("b_1").nest("b"))),
                Arguments.of(field("a").nest("a").groupBy(field("b").nest("b")), renameNestedA, field("a").nest("a_2").groupBy(field("b").nest("b"))),
                Arguments.of(field("a").nest("a").groupBy(field("b").nest("b")), renameNestedB, field("a").nest("a").groupBy(field("b").nest("b_2"))),
                Arguments.of(field("a").nest("a").groupBy(field("b").nest("b")), renameAll, field("a_1").nest("a_2").groupBy(field("b_1").nest("b_2")))
        );

        // Create some random arguments as well
        final Stream<Arguments> randomArgs = RandomizedTestUtils.randomSeeds(0x5ca1ab1e, 0xfdb01234L).flatMap(seed -> {
            final Random random = new Random(seed);
            return Stream.generate(() -> mutateMergedRecord(randomMutator(random))).limit(5).flatMap(renamedDescriptor ->
                    Stream.generate(() -> {
                        final NonnullPair<KeyExpression, KeyExpression> randomCase = randomExpressionWithRename(random, TestUnmergedNestedTypesProto.MyRecord.getDescriptor(), renamedDescriptor.getPayload(), 0);
                        return Arguments.of(randomCase.getLeft(), renamedDescriptor, randomCase.getRight());
                    }).limit(20)
            );
        });

        return Stream.concat(staticArgs, randomArgs);
    }

    /**
     * Validate that we can rename fields and merge two nested types at the same time. In this case, we start with an
     * expression on the {@link TestUnmergedNestedTypesProto.MyRecord} type, and then we compare it to an expression
     * possibly renamed {@link TestMergedNestedTypesProto.MyRecord} type. This involves a {@link FieldRenames} map
     * where the renaming map from {@link TestUnmergedNestedTypesProto.NestedA} to {@link TestMergedNestedTypesProto.OneTrueNested}
     * may differ from the map from {@link TestUnmergedNestedTypesProto.NestedB} to {@link TestMergedNestedTypesProto.OneTrueNested}.
     *
     * @param original the original expression
     * @param targetDescriptor the target descriptor (a renamed {@link TestMergedNestedTypesProto.MyRecord} descriptor)
     * @param expected the expected target expression
     */
    @ParameterizedTest(name = "renameMergedRecord[{0}, {1}]")
    @MethodSource
    void renameMergedRecord(@Nonnull KeyExpression original, @Nonnull Descriptors.Descriptor targetDescriptor, @Nonnull KeyExpression expected) {
        assertRenaming(original, TestUnmergedNestedTypesProto.MyRecord.getDescriptor(), targetDescriptor, expected);
    }

    @Nonnull
    static Stream<Arguments> renameSplitRecord() {
        // Create some static test cases. See the test method for an explanation as to how these are supposed to be structured
        final Named<Descriptors.Descriptor> identity = Named.of("IDENTITY", TestSplitNestedTypesProto.MyRecord.getDescriptor());
        final Named<Descriptors.Descriptor> renameNestedA = remapSplitRecord(Map.of(TestSplitNestedTypesProto.NestedA.getDescriptor(), Map.of("a", "a_1", "b", "b_1")));
        final Named<Descriptors.Descriptor> renameNestedB = remapSplitRecord(Map.of(TestSplitNestedTypesProto.NestedB.getDescriptor(), Map.of("a", "a_2", "b", "b_2")));
        final Named<Descriptors.Descriptor> swapTopAAndBAndRenameNested = remapSplitRecord(Map.of(
                TestSplitNestedTypesProto.MyRecord.getDescriptor(), Map.of("a", "b", "b", "a"),
                TestSplitNestedTypesProto.NestedA.getDescriptor(), Map.of("a", "a_1", "b", "b_1"),
                TestSplitNestedTypesProto.NestedB.getDescriptor(), Map.of("a", "a_2", "b", "b_2")
        ));

        final Stream<Arguments> staticArgs = Stream.of(
                Arguments.of(concat(field("a").nest("a"), field("b").nest("a")), identity, concat(field("a").nest("a"), field("b").nest("a"))),
                Arguments.of(concat(field("a").nest("a"), field("b").nest("a")), renameNestedA, concat(field("a").nest("a_1"), field("b").nest("a"))),
                Arguments.of(concat(field("a").nest("a"), field("b").nest("a")), renameNestedB, concat(field("a").nest("a"), field("b").nest("a_2"))),
                Arguments.of(concat(field("a").nest("a"), field("b").nest("a")), swapTopAAndBAndRenameNested, concat(field("b").nest("a_1"), field("a").nest("a_2"))),

                Arguments.of(concat(field("a").nest("b"), field("b").nest("b")), identity, concat(field("a").nest("b"), field("b").nest("b"))),
                Arguments.of(concat(field("a").nest("b"), field("b").nest("b")), renameNestedA, concat(field("a").nest("b_1"), field("b").nest("b"))),
                Arguments.of(concat(field("a").nest("b"), field("b").nest("b")), renameNestedB, concat(field("a").nest("b"), field("b").nest("b_2"))),
                Arguments.of(concat(field("a").nest("b"), field("b").nest("b")), swapTopAAndBAndRenameNested, concat(field("b").nest("b_1"), field("a").nest("b_2")))
        );

        // Create some random arguments as well
        final Stream<Arguments> randomArgs = RandomizedTestUtils.randomSeeds(0x5ca1ab1e, 0xfdb01234L).flatMap(seed -> {
            final Random random = new Random(seed);
            return Stream.generate(() -> mutateSplitRecord(randomMutator(random))).limit(5).flatMap(renamedDescriptor ->
                    Stream.generate(() -> {
                        final NonnullPair<KeyExpression, KeyExpression> randomCase = randomExpressionWithRename(random, TestMergedNestedTypesProto.MyRecord.getDescriptor(), renamedDescriptor.getPayload(), 0);
                        return Arguments.of(randomCase.getLeft(), renamedDescriptor, randomCase.getRight());
                    }).limit(20)
            );
        });

        return Stream.concat(staticArgs, randomArgs);
    }

    /**
     * Validate that we can rename fields and split two nested types at the same time. In this case, we start with an
     * expression on the {@link TestMergedNestedTypesProto.MyRecord} type, and then we compare it to an expression on a
     * possibly renamed {@link TestSplitNestedTypesProto.MyRecord} type. This involves a {@link FieldRenames} map
     * where the renaming map from {@link TestMergedNestedTypesProto.OneTrueNested} to {@link TestSplitNestedTypesProto.NestedA}
     * may differ from the map from {@link TestMergedNestedTypesProto.OneTrueNested} to {@link TestSplitNestedTypesProto.NestedB}.
     * The renaming logic must correctly look up the mapping based on the type of the field in the split nested type proto.
     *
     * @param original the original expression
     * @param targetDescriptor the target descriptor (a renamed {@link TestSplitNestedTypesProto.MyRecord} descriptor)
     * @param expected the expected target expression
     */
    @ParameterizedTest(name = "renameSplitRecord[{0}, {1}]")
    @MethodSource
    void renameSplitRecord(@Nonnull KeyExpression original, @Nonnull Descriptors.Descriptor targetDescriptor, @Nonnull KeyExpression expected) {
        assertRenaming(original, TestMergedNestedTypesProto.MyRecord.getDescriptor(), targetDescriptor, expected);
    }

    private static void assertRenaming(@Nonnull KeyExpression original, @Nonnull Descriptors.Descriptor descriptor, @Nonnull Descriptors.Descriptor targetDescriptor, @Nonnull KeyExpression expected) {
        final FieldRenames fieldRenames = FieldRenames.constructFor(descriptor, targetDescriptor);
        final KeyExpression renamed = RenameFieldsVisitor.renameFields(original, fieldRenames, descriptor, targetDescriptor);
        assertThat(renamed)
                .isEqualTo(expected);
        if (fieldRenames.isIdentity() || original.equals(renamed)) {
            assertThat(renamed)
                    .isSameAs(original);
        }
        if (original instanceof FieldKeyExpression) {
            final FieldKeyExpression originalFieldExpression = (FieldKeyExpression) original;
            assertThat(renamed)
                    .isInstanceOf(FieldKeyExpression.class);
            final FieldKeyExpression renamedFieldExpression = (FieldKeyExpression) renamed;
            assertThat(renamedFieldExpression.getNullStandin())
                    .isEqualTo(originalFieldExpression.getNullStandin());
            assertThat(renamedFieldExpression.getFanType())
                    .isEqualTo(originalFieldExpression.getFanType());
        } else if (original instanceof NestingKeyExpression) {
            final NestingKeyExpression originalNestingExpression = (NestingKeyExpression) original;
            assertThat(renamed)
                    .isInstanceOf(NestingKeyExpression.class);
            final NestingKeyExpression renamedNestingExpression = (NestingKeyExpression) renamed;
            assertThat(originalNestingExpression.getParent().getNullStandin())
                    .isEqualTo(renamedNestingExpression.getParent().getNullStandin());
            assertThat(originalNestingExpression.getParent().getFanType())
                    .isEqualTo(renamedNestingExpression.getParent().getFanType());
        }
    }

    /**
     * Generate a random expression that should be valid on the given descriptor. At the same time,
     * generates an expression that walks an equivalent path through the second descriptor. This uses
     * the field numbers in the two Protobuf descriptors in order to establish field equivalency, and
     * it is in this way supposed to reflect rewrites to the fields.
     *
     * <p>
     * This takes a {@code depth} parameter to avoid infinite recursion. It has three purposes:
     * </p>
     *
     * <ul>
     *     <li>
     *         Certain expression types (like the {@link GroupingKeyExpression}) only have semantic meaning if
     *         they are at the top level. Those expressions are only generated if the {@code depth} is zero.
     *     </li>
     *     <li>
     *         Other expression types have one or more child expressions that they are built on top of. To limit
     *         recursion depth, we only generate one of those if we have not already hit a certain depth limit.
     *     </li>
     *     <li>
     *         There are some expression types that are not interesting if they are the only expression as they
     *         are always invariant to all field transformations. For example, the {@link com.apple.foundationdb.record.metadata.expressions.EmptyKeyExpression},
     *         which always returns {@link com.apple.foundationdb.record.metadata.expressions.EmptyKeyExpression}. We
     *         do not generate these expressions at depth 0 to avoid generating trivial test cases.
     *     </li>
     * </ul>
     *
     * @param random a source of random values
     * @param sourceDescriptor a message descriptor on which to generate key expressions
     * @param targetDescriptor a separate message descriptor with renamed fields
     * @param depth the current stack depth of these calls
     * @return a pair consisting of a random expression evaluatable on the {@code sourceDescriptor} on the left and then that
     *      same expression with its referenced fields renamed subject so that it applies to the {@code targetDescriptor} on the right
     */
    @Nonnull
    private static NonnullPair<KeyExpression, KeyExpression> randomExpressionWithRename(@Nonnull Random random, @Nonnull Descriptors.Descriptor sourceDescriptor, @Nonnull Descriptors.Descriptor targetDescriptor, int depth) {
        double randomChoice = random.nextDouble();
        if (depth > 0 && randomChoice < 0.05) {
            // EmptyKeyExpression
            return NonnullPair.of(empty(), empty());
        } else if (depth > 0 && randomChoice < 0.1) {
            // Record Type
            return NonnullPair.of(recordType(), recordType());
        } else if (depth > 0 && randomChoice < 0.15) {
            // Version
            return NonnullPair.of(version(), version());
        } else if (depth > 0 && randomChoice < 0.2) {
            // Literal value
            final long value = random.nextLong();
            return NonnullPair.of(value(value), value(value));
        } else if (depth < MAX_RECURSION_DEPTH && randomChoice < 0.4) {
            // Then or list key expression
            int count = random.nextInt(4) + 2;
            List<NonnullPair<KeyExpression, KeyExpression>> children = Stream.generate(() -> randomExpressionWithRename(random, sourceDescriptor, targetDescriptor, depth + 1))
                    .limit(count)
                    .collect(Collectors.toList());
            List<KeyExpression> originalChildren = children.stream().map(NonnullPair::getLeft).collect(Collectors.toList());
            List<KeyExpression> renamedChildren = children.stream().map(NonnullPair::getRight).collect(Collectors.toList());
            if (randomChoice < 0.3) {
                // ThenKeyExpression
                return NonnullPair.of(concat(originalChildren), concat(renamedChildren));
            } else {
                // ListKeyExpression
                return NonnullPair.of(list(originalChildren), list(renamedChildren));
            }
        } else if (depth < MAX_RECURSION_DEPTH && randomChoice < 0.5) {
            // Function key expression
            final NonnullPair<KeyExpression, KeyExpression> arguments = randomExpressionWithRename(random, sourceDescriptor, targetDescriptor, depth + 1);
            return NonnullPair.of(function("nada", arguments.getLeft()), function("nada", arguments.getRight()));
        } else if (depth < MAX_RECURSION_DEPTH && randomChoice < 0.6) {
            // SplitKeyExpression
            final NonnullPair<KeyExpression, KeyExpression> joined = randomExpressionWithRename(random, sourceDescriptor, targetDescriptor, depth + 1);
            int splitSize = random.nextInt(3) + 1;
            return NonnullPair.of(new SplitKeyExpression(joined.getLeft(), splitSize), new SplitKeyExpression(joined.getRight(), splitSize));
        } else if (depth == 0 && randomChoice < 0.7) {
            // Grouping key expression
            final NonnullPair<KeyExpression, KeyExpression> wholeKey = randomExpressionWithRename(random, sourceDescriptor, targetDescriptor, depth + 1);
            int groupingCount = random.nextInt(wholeKey.getLeft().getColumnSize() + 1);
            return NonnullPair.of(new GroupingKeyExpression(wholeKey.getLeft(), groupingCount), new GroupingKeyExpression(wholeKey.getRight(), groupingCount));
        } else if (depth == 0 && randomChoice < 0.8) {
            // KeyWithValueExpression
            final NonnullPair<KeyExpression, KeyExpression> wholeKey = randomExpressionWithRename(random, sourceDescriptor, targetDescriptor, depth + 1);
            int splitPoint = random.nextInt(wholeKey.getLeft().getColumnSize() + 1);
            return NonnullPair.of(keyWithValue(wholeKey.getLeft(), splitPoint), keyWithValue(wholeKey.getRight(), splitPoint));
        } else if (depth == 0 && randomChoice < 0.9) {
            // DimensionsKeyExpression
            NonnullPair<KeyExpression, KeyExpression> wholeKey = randomExpressionWithRename(random, sourceDescriptor, targetDescriptor, depth + 1);
            while (wholeKey.getLeft().getColumnSize() < 2) {
                wholeKey = randomExpressionWithRename(random, sourceDescriptor, targetDescriptor, depth + 1);
            }
            int columnSize = wholeKey.getLeft().getColumnSize();
            int dimensionsCount = columnSize == 2 ? 2 : random.nextInt(columnSize - 2) + 2;
            int prefixCount = random.nextInt(columnSize - dimensionsCount + 1);
            return NonnullPair.of(DimensionsKeyExpression.of(wholeKey.getLeft(), prefixCount, dimensionsCount), DimensionsKeyExpression.of(wholeKey.getRight(), prefixCount, dimensionsCount));
        } else {
            // Random field
            final List<Descriptors.FieldDescriptor> fields = sourceDescriptor.getFields();
            int fieldChoice = random.nextInt(fields.size());
            final Descriptors.FieldDescriptor sourceField = fields.get(fieldChoice);
            final String origName = sourceField.getName();
            final Descriptors.FieldDescriptor targetField = Objects.requireNonNull(targetDescriptor.findFieldByNumber(sourceField.getNumber()));
            final String newName = targetField.getName();
            final FieldKeyExpression origFieldExpression;
            final FieldKeyExpression renamedFieldExpression;
            final boolean isMessage = sourceField.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE;
            if (sourceField.isRepeated()) {
                origFieldExpression = field(origName, (isMessage || random.nextBoolean()) ? KeyExpression.FanType.FanOut : KeyExpression.FanType.Concatenate);
                renamedFieldExpression = field(newName, origFieldExpression.getFanType());
            } else {
                origFieldExpression = field(origName, KeyExpression.FanType.None);
                renamedFieldExpression = field(newName, KeyExpression.FanType.None);
            }

            if (isMessage) {
                // This is a message type. Generated a nested child
                final Descriptors.Descriptor childSourceDescriptor = sourceField.getMessageType();
                final Descriptors.Descriptor childTargetDescriptor = targetField.getMessageType();
                final NonnullPair<KeyExpression, KeyExpression> child = randomExpressionWithRename(random, childSourceDescriptor, childTargetDescriptor, depth + 1);
                return NonnullPair.of(origFieldExpression.nest(child.getLeft()), renamedFieldExpression.nest(child.getRight()));
            } else {
                // This is not a message type. Return immediately
                return NonnullPair.of(origFieldExpression, renamedFieldExpression);
            }
        }
    }


    @Nonnull
    private static Function<Descriptors.Descriptor, DescriptorProtos.DescriptorProto> mutatorByMap(@Nonnull Map<Descriptors.Descriptor, Map<String, String>> renamingMap) {
        return descriptor -> {
            final DescriptorProtos.DescriptorProto descriptorProto = descriptor.toProto();
            if (renamingMap.containsKey(descriptor)) {
                final Map<String, String> renamingForType = renamingMap.get(descriptor);
                final DescriptorProtos.DescriptorProto.Builder builder = descriptorProto.toBuilder();
                builder.getFieldBuilderList().forEach(fieldBuilder -> fieldBuilder.setName(renamingForType.getOrDefault(fieldBuilder.getName(), fieldBuilder.getName())));
                return builder.build();
            } else {
                return descriptor.toProto();
            }
        };
    }

    @Nonnull
    private static Function<Descriptors.Descriptor, DescriptorProtos.DescriptorProto> randomMutator(@Nonnull Random random) {
        return descriptor -> {
            final DescriptorProtos.DescriptorProto.Builder protoBuilder = descriptor.toProto().toBuilder();
            protoBuilder.getFieldBuilderList().forEach(fieldBuilder -> {
                if (random.nextBoolean()) {
                    fieldBuilder.setName(fieldBuilder.getName() + "_" + random.nextInt(100));
                }
            });
            return protoBuilder.build();
        };
    }

    @Nonnull
    private static Named<Descriptors.Descriptor> remapMySimpleRecord(@Nonnull Map<String, String> renaming) {
        return mutateMySimpleRecord(mutatorByMap(Map.of(TestRecords1Proto.MySimpleRecord.getDescriptor(), renaming)));
    }

    @Nonnull
    private static Named<Descriptors.Descriptor> mutateMySimpleRecord(@Nonnull Function<Descriptors.Descriptor, DescriptorProtos.DescriptorProto> mutator) {
        return mutateSingleType(TestRecords1Proto.getDescriptor(), "MySimpleRecord", mutator);
    }

    @Nonnull
    private static Named<Descriptors.Descriptor> remapOuterRecord(@Nonnull Map<Descriptors.Descriptor, Map<String, String>> renaming) {
        return mutateOuterRecord(mutatorByMap(renaming));
    }

    @Nonnull
    private static Named<Descriptors.Descriptor> mutateOuterRecord(@Nonnull Function<Descriptors.Descriptor, DescriptorProtos.DescriptorProto> mutator) {
        return mutateSingleType(TestRecordsDoubleNestedProto.getDescriptor(), "OuterRecord", mutator);
    }

    @Nonnull
    private static Named<Descriptors.Descriptor> remapMiddleRecord(@Nonnull Map<Descriptors.Descriptor, Map<String, String>> renaming) {
        return mutateMiddleRecord(mutatorByMap(renaming));
    }

    @Nonnull
    private static Named<Descriptors.Descriptor> mutateMiddleRecord(@Nonnull Function<Descriptors.Descriptor, DescriptorProtos.DescriptorProto> mutator) {
        return mutateSingleType(TestRecordsDoubleNestedProto.getDescriptor(), "MiddleRecord", mutator);
    }

    @Nonnull
    private static Named<Descriptors.Descriptor> remapMergedRecord(@Nonnull Map<Descriptors.Descriptor, Map<String, String>> renaming) {
        return mutateMergedRecord(mutatorByMap(renaming));
    }

    @Nonnull
    private static Named<Descriptors.Descriptor> mutateMergedRecord(@Nonnull Function<Descriptors.Descriptor, DescriptorProtos.DescriptorProto> mutator) {
        return mutateSingleType(TestMergedNestedTypesProto.getDescriptor(), "MyRecord", mutator);
    }

    @Nonnull
    private static Named<Descriptors.Descriptor> remapSplitRecord(@Nonnull Map<Descriptors.Descriptor, Map<String, String>> renaming) {
        return mutateSplitRecord(mutatorByMap(renaming));
    }

    @Nonnull
    private static Named<Descriptors.Descriptor> mutateSplitRecord(@Nonnull Function<Descriptors.Descriptor, DescriptorProtos.DescriptorProto> mutator) {
        return mutateSingleType(TestSplitNestedTypesProto.getDescriptor(), "MyRecord", mutator);
    }

    @Nonnull
    private static Named<Descriptors.Descriptor> mutateSingleType(@Nonnull Descriptors.FileDescriptor originalFile, @Nonnull String typeName, @Nonnull Function<Descriptors.Descriptor, DescriptorProtos.DescriptorProto> mutator) {
        final Descriptors.FileDescriptor renamedFile = mutateFileDescriptor(originalFile, mutator);
        final Descriptors.Descriptor renamedDescriptor = renamedFile.findMessageTypeByName(typeName);
        return Named.of(FieldRenames.constructFor(originalFile.findMessageTypeByName(typeName), renamedDescriptor).toString(), renamedDescriptor);
    }

    @Nonnull
    private static Descriptors.FileDescriptor mutateFileDescriptor(@Nonnull Descriptors.FileDescriptor originalFile, @Nonnull Function<Descriptors.Descriptor, DescriptorProtos.DescriptorProto> mutator) {
        final DescriptorProtos.FileDescriptorProto.Builder fileBuilder = originalFile.toProto().toBuilder();
        fileBuilder.clearMessageType();
        for (Descriptors.Descriptor descriptor : originalFile.getMessageTypes()) {
            fileBuilder.addMessageType(mutateDescriptor(descriptor, mutator));
        }
        try {
            return Descriptors.FileDescriptor.buildFrom(fileBuilder.build(), originalFile.getDependencies().toArray(Descriptors.FileDescriptor[]::new));
        } catch (Descriptors.DescriptorValidationException e) {
            return fail("Unable to build mutated file descriptor", e);
        }
    }

    @Nonnull
    private static DescriptorProtos.DescriptorProto mutateDescriptor(@Nonnull Descriptors.Descriptor descriptor, @Nonnull Function<Descriptors.Descriptor, DescriptorProtos.DescriptorProto> mutator) {
        final DescriptorProtos.DescriptorProto mutated = mutator.apply(descriptor);
        if (descriptor.getNestedTypes().isEmpty()) {
            return mutated;
        }
        final DescriptorProtos.DescriptorProto.Builder mutatedWithMutatedChildren = mutated.toBuilder();
        mutatedWithMutatedChildren.clearNestedType();
        for (Descriptors.Descriptor nestedDescriptor : descriptor.getNestedTypes()) {
            mutatedWithMutatedChildren.addNestedType(mutateDescriptor(nestedDescriptor, mutator));
        }
        return mutatedWithMutatedChildren.build();
    }
}
