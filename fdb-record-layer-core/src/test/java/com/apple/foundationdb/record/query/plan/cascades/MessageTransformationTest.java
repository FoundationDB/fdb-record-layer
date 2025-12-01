/*
 * MessageTransformationTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2022 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.cascades;

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.TestRecords4Proto;
import com.apple.foundationdb.record.TestRecords4WrapperProto;
import com.apple.foundationdb.record.TestRecordsTransformProto;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.TypeRepository;
import com.apple.foundationdb.record.query.plan.cascades.values.AbstractArrayConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.cascades.values.MessageHelpers;
import com.apple.foundationdb.record.query.plan.cascades.values.NullValue;
import com.apple.foundationdb.record.query.plan.cascades.values.PromoteValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUpdatePlan;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Tests different aspects of functionality transforming a message through a multitude of replacements.
 */
@SuppressWarnings("ConstantConditions")
class MessageTransformationTest {
    @Test
    void testTransformTrie() {
        final var recordType = someRecordType();
        final var inValue = QuantifiedObjectValue.of(Quantifier.current(), recordType);
        final var a_aa_aaa = FieldValue.ofFieldNames(inValue, ImmutableList.of("a", "aa", "aaa"));
        final var a_aa_aab = FieldValue.ofFieldNames(inValue, ImmutableList.of("a", "aa", "aab"));
        final var a_ab = FieldValue.ofFieldNames(inValue, ImmutableList.of("a", "ab"));
        
        final var transformMap =
                ImmutableMap.<FieldValue.FieldPath, Value>of(a_aa_aaa.getFieldPath(), new LiteralValue<>("1"),
                        a_aa_aab.getFieldPath(), new LiteralValue<>(2),
                        a_ab.getFieldPath(), new LiteralValue<>(3));

        final var transformTrie =
                RecordQueryUpdatePlan.computeTrieForFieldPaths(RecordQueryUpdatePlan.checkAndPrepareOrderedFieldPaths(transformMap),
                        transformMap);
        Assertions.assertNull(transformTrie.getValue());
        var childrenMap = transformTrie.getChildrenMap();
        Assertions.assertNotNull(childrenMap);
        Assertions.assertTrue(childrenMap.containsKey(0)); //aField
        Assertions.assertEquals(1, childrenMap.size());
        var aTrie = childrenMap.get(0); // aField
        Assertions.assertNull(aTrie.getValue());
        childrenMap = aTrie.getChildrenMap();
        Assertions.assertNotNull(childrenMap);
        Assertions.assertTrue(childrenMap.containsKey(0)); // aaField
        Assertions.assertTrue(childrenMap.containsKey(1)); // abField
        Assertions.assertEquals(2, childrenMap.size());
        var aaTrie = childrenMap.get(0); // aaField
        Assertions.assertNull(aaTrie.getValue());
        childrenMap = aaTrie.getChildrenMap();
        Assertions.assertNotNull(childrenMap);
        Assertions.assertTrue(childrenMap.containsKey(0)); // aaaField
        Assertions.assertTrue(childrenMap.containsKey(1)); // aabField
        Assertions.assertEquals(2, childrenMap.size());
        var aaaTrie = childrenMap.get(0); // aaaField
        Assertions.assertNull(aaaTrie.getChildrenMap());
        Assertions.assertNotNull(aaaTrie.getValue());
        Assertions.assertEquals(aaaTrie.getValue(), transformMap.get(a_aa_aaa.getFieldPath()));
        var aabTrie = childrenMap.get(1); // aabField
        Assertions.assertEquals(aabTrie.getValue(), transformMap.get(a_aa_aab.getFieldPath()));
        childrenMap = aTrie.getChildrenMap();
        var abTrie = childrenMap.get(1); // abField
        Assertions.assertNull(abTrie.getChildrenMap());
        Assertions.assertNotNull(abTrie.getValue());
        Assertions.assertEquals(abTrie.getValue(), transformMap.get(a_ab.getFieldPath()));
    }

    @Test
    void testTransformWrongReplacementType() {
        final var inValue = QuantifiedObjectValue.of(Quantifier.current(), someRecordType());
        final var a_aa_aaa = FieldValue.ofFieldNames(inValue, ImmutableList.of("a", "aa", "aaa"));
        final var a_aa_aab = FieldValue.ofFieldNames(inValue, ImmutableList.of("a", "aa", "aab"));
        final var a_ab = FieldValue.ofFieldNames(inValue, ImmutableList.of("a", "ab"));

        final var transformMap =
                ImmutableMap.<FieldValue.FieldPath, Value>of(a_aa_aaa.getFieldPath(), new LiteralValue<>(1),
                        a_aa_aab.getFieldPath(), new LiteralValue<>(2),
                        a_ab.getFieldPath(), new LiteralValue<>(3));

        final var transformationsTrie =
                RecordQueryUpdatePlan.computeTrieForFieldPaths(RecordQueryUpdatePlan.checkAndPrepareOrderedFieldPaths(transformMap),
                transformMap);
        Assertions.assertThrows(SemanticException.class,
                () -> PromoteValue.computePromotionsTrie(inValue.getResultType(), inValue.getResultType(), transformationsTrie));
    }

    @Test
    void testTransformAmbiguousReplacement() {
        final var inValue = QuantifiedObjectValue.of(Quantifier.current(), someRecordType());
        final var a_aa = FieldValue.ofFieldNames(inValue, ImmutableList.of("a", "aa"));
        final var a_aa_aab = FieldValue.ofFieldNames(inValue, ImmutableList.of("a", "aa", "aab"));

        final var transformMap =
                ImmutableMap.<FieldValue.FieldPath, Value>of(a_aa.getFieldPath(), new NullValue(a_aa.getResultType()),
                        a_aa_aab.getFieldPath(), new LiteralValue<>(2));

        Assertions.assertThrows(SemanticException.class,
                () -> RecordQueryUpdatePlan.computeTrieForFieldPaths(RecordQueryUpdatePlan.checkAndPrepareOrderedFieldPaths(transformMap),
                        transformMap));
    }

    @Test
    void testTransformLeafs() {
        final var inValue = makeRecordConstructor();
        final var a_aa_aaa = FieldValue.ofFieldNames(inValue, ImmutableList.of("a", "aa", "aaa"));
        final var a_aa_aab = FieldValue.ofFieldNames(inValue, ImmutableList.of("a", "aa", "aab"));
        final var a_ab = FieldValue.ofFieldNames(inValue, ImmutableList.of("a", "ab"));

        final var transformMap =
                ImmutableMap.<FieldValue.FieldPath, Value>of(a_aa_aaa.getFieldPath(), new LiteralValue<>("1"),
                        a_aa_aab.getFieldPath(), new LiteralValue<>(2),
                        a_ab.getFieldPath(), new LiteralValue<>(3));

        final var trie =
                RecordQueryUpdatePlan.computeTrieForFieldPaths(RecordQueryUpdatePlan.checkAndPrepareOrderedFieldPaths(transformMap),
                        transformMap);

        final var evaluationContext = EvaluationContext.forTypeRepository(TypeRepository.newBuilder().addTypeIfNeeded(inValue.getResultType()).build());
        final var inRecord = inValue.eval(null, evaluationContext);
        final var result = MessageHelpers.transformMessage(null,
                evaluationContext,
                trie,
                null,
                inValue.getResultType(),
                evaluationContext.getTypeRepository().getMessageDescriptor(inValue.getResultType()),
                inValue.getResultType(),
                evaluationContext.getTypeRepository().getMessageDescriptor(inValue.getResultType()),
                inRecord);

        final var aaValue =
                RecordConstructorValue.ofColumns(
                        ImmutableList.of(
                                Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("aaa")), new LiteralValue<>("1")),
                                Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.INT), Optional.of("aab")), new LiteralValue<>(2)),
                                Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("aac")), new LiteralValue<>("aac"))
                        ));
        final var aValue =
                RecordConstructorValue.ofColumns(
                        ImmutableList.of(
                                Column.of(Optional.of("aa"), aaValue),
                                Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.INT), Optional.of("ab")), new LiteralValue<>(3)),
                                Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("ac")), new LiteralValue<>("ac"))
                        ));
        final var xValue =
                RecordConstructorValue.ofColumns(
                        ImmutableList.of(
                                Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("xa")), new LiteralValue<>("xa")),
                                Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.INT), Optional.of("xb")), new LiteralValue<>(3)),
                                Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("xc")), new LiteralValue<>("xc"))
                        ));

        final var expectedValue = RecordConstructorValue.ofColumns(
                ImmutableList.of(
                        Column.of(Optional.of("a"), aValue),
                        Column.of(Optional.of("x"), xValue),
                        Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("z")), new LiteralValue<>("z"))
                ));
        final var expected = expectedValue.eval(null, evaluationContext);
        Assertions.assertEquals(expected, result);
    }

    @Test
    void testTransformIntermediate() {
        final var inValue = makeRecordConstructor();
        final var a_aa = FieldValue.ofFieldNames(inValue, ImmutableList.of("a", "aa"));
        final var a_ab = FieldValue.ofFieldNames(inValue, ImmutableList.of("a", "ab"));

        final var aaValue =
                RecordConstructorValue.ofColumns(
                        ImmutableList.of(
                                Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("aaa")), new LiteralValue<>("10")),
                                Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.INT), Optional.of("aab")), new LiteralValue<>(20)),
                                Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("aac")), new LiteralValue<>("30"))
                        ));

        final var transformMap =
                ImmutableMap.of(a_aa.getFieldPath(), aaValue,
                        a_ab.getFieldPath(), new LiteralValue<>(3));

        final var trie =
                RecordQueryUpdatePlan.computeTrieForFieldPaths(RecordQueryUpdatePlan.checkAndPrepareOrderedFieldPaths(transformMap),
                        transformMap);

        final var evaluationContext = EvaluationContext.forTypeRepository(TypeRepository.newBuilder().addTypeIfNeeded(inValue.getResultType()).build());
        final var inRecord = inValue.eval(null, evaluationContext);
        final var result = MessageHelpers.transformMessage(null,
                evaluationContext,
                trie,
                null,
                inValue.getResultType(),
                evaluationContext.getTypeRepository().getMessageDescriptor(inValue.getResultType()),
                inValue.getResultType(),
                evaluationContext.getTypeRepository().getMessageDescriptor(inValue.getResultType()),
                inRecord);
        
        final var aValue =
                RecordConstructorValue.ofColumns(
                        ImmutableList.of(
                                Column.of(Optional.of("aa"), aaValue),
                                Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.INT), Optional.of("ab")), new LiteralValue<>(3)),
                                Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("ac")), new LiteralValue<>("ac"))
                        ));
        final var xValue =
                RecordConstructorValue.ofColumns(
                        ImmutableList.of(
                                Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("xa")), new LiteralValue<>("xa")),
                                Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.INT), Optional.of("xb")), new LiteralValue<>(3)),
                                Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("xc")), new LiteralValue<>("xc"))
                        ));

        final var expectedValue = RecordConstructorValue.ofColumns(
                ImmutableList.of(
                        Column.of(Optional.of("a"), aValue),
                        Column.of(Optional.of("x"), xValue),
                        Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("z")), new LiteralValue<>("z"))
                ));
        final var expected = expectedValue.eval(null, evaluationContext);
        Assertions.assertEquals(expected, result);
    }

    @Test
    void testTransformSparse() {
        final var inValue = makeSparseRecordConstructor();
        final var a_aa_aaa = FieldValue.ofFieldNames(inValue, ImmutableList.of("a", "aa", "aaa"));
        final var a_aa_aab = FieldValue.ofFieldNames(inValue, ImmutableList.of("a", "aa", "aab"));
        final var a_ab = FieldValue.ofFieldNames(inValue, ImmutableList.of("a", "ab"));
        
        final var transformMap =
                ImmutableMap.<FieldValue.FieldPath, Value>of(a_aa_aaa.getFieldPath(), new LiteralValue<>("1"),
                        a_aa_aab.getFieldPath(), new LiteralValue<>(2),
                        a_ab.getFieldPath(), new LiteralValue<>(3));
        final var trie =
                RecordQueryUpdatePlan.computeTrieForFieldPaths(RecordQueryUpdatePlan.checkAndPrepareOrderedFieldPaths(transformMap),
                        transformMap);

        final var aaValue =
                RecordConstructorValue.ofColumns(
                        ImmutableList.of(
                                Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("aaa")), new LiteralValue<>("1")),
                                Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.INT), Optional.of("aab")), new LiteralValue<>(2)),
                                Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("aac")), new NullValue(Type.primitiveType(Type.TypeCode.STRING)))
                        ));
        final var aValue =
                RecordConstructorValue.ofColumns(
                        ImmutableList.of(
                                Column.of(Type.Record.Field.of(aaValue.getResultType(), Optional.of("aa")), aaValue),
                                Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.INT), Optional.of("ab")), new LiteralValue<>(3)),
                                Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("ac")), new LiteralValue<>("ac"))
                        ));
        final var xValue =
                RecordConstructorValue.ofColumns(
                        ImmutableList.of(
                                Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("xa")), new LiteralValue<>("xa")),
                                Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.INT), Optional.of("xb")), new LiteralValue<>(3)),
                                Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("xc")), new LiteralValue<>("xc"))
                        ));

        final var expectedValue = RecordConstructorValue.ofColumns(
                ImmutableList.of(
                        Column.of(Type.Record.Field.of(aValue.getResultType(), Optional.of("a")), aValue),
                        Column.of(Type.Record.Field.of(xValue.getResultType(), Optional.of("x")), xValue),
                        Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("z")), new LiteralValue<>("z"))
                ));

        final var evaluationContext = EvaluationContext.forTypeRepository(TypeRepository.newBuilder().addTypeIfNeeded(inValue.getResultType()).addTypeIfNeeded(expectedValue.getResultType()).build());
        final var inRecord = (Message)inValue.eval(null, evaluationContext);
        final var result = MessageHelpers.transformMessage(null,
                evaluationContext,
                trie,
                null,
                expectedValue.getResultType(),
                evaluationContext.getTypeRepository().getMessageDescriptor(expectedValue.getResultType()),
                inValue.getResultType(),
                inRecord.getDescriptorForType(),
                inRecord);

        final var expected = expectedValue.eval(null, evaluationContext);
        Assertions.assertEquals(expected, result);
    }

    @Test
    void testTransformLeafsWithCoercion() throws Exception {
        final var inValue = makeRecordConstructor();
        final var a_aa_aaa = FieldValue.ofFieldNames(inValue, ImmutableList.of("a", "aa", "aaa"));
        final var a_aa_aab = FieldValue.ofFieldNames(inValue, ImmutableList.of("a", "aa", "aab"));
        final var a_ab = FieldValue.ofFieldNames(inValue, ImmutableList.of("a", "ab"));

        final var transformMap =
                ImmutableMap.<FieldValue.FieldPath, Value>of(a_aa_aaa.getFieldPath(), new LiteralValue<>("1"),
                        a_aa_aab.getFieldPath(), new LiteralValue<>(2),
                        a_ab.getFieldPath(), new LiteralValue<>(3));

        final var trie =
                RecordQueryUpdatePlan.computeTrieForFieldPaths(RecordQueryUpdatePlan.checkAndPrepareOrderedFieldPaths(transformMap),
                        transformMap);

        final var evaluationContext = EvaluationContext.forTypeRepository(TypeRepository.newBuilder().addTypeIfNeeded(inValue.getResultType()).build());
        final var inRecord = (Message)inValue.eval(null, evaluationContext);
        final var coercedType = Type.Record.fromDescriptor(TestRecordsTransformProto.DefaultTransformMessage.getDescriptor());
        final var result = (Message)Verify.verifyNotNull(MessageHelpers.transformMessage(null,
                evaluationContext,
                trie,
                null,
                coercedType,
                TestRecordsTransformProto.DefaultTransformMessage.getDescriptor(),
                inValue.getResultType(),
                inRecord.getDescriptorForType(),
                inRecord));
        Assertions.assertEquals(TestRecordsTransformProto.DefaultTransformMessage.getDescriptor().getFullName(), result.getDescriptorForType().getFullName());
        final var resultSerialized = result.toByteString();
        final var typedResult = TestRecordsTransformProto.DefaultTransformMessage.parseFrom(resultSerialized);
        Assertions.assertEquals("1", typedResult.getA().getAa().getAaa());
        Assertions.assertEquals(2, typedResult.getA().getAa().getAab());
        Assertions.assertEquals("aac", typedResult.getA().getAa().getAac());
        Assertions.assertEquals(3, typedResult.getA().getAb());
        Assertions.assertEquals("ac", typedResult.getA().getAc());
        Assertions.assertEquals("z", typedResult.getZ());
    }

    @Test
    void testTransformIntermediateWithCoercion() throws Exception {
        final var inValue = makeRecordConstructor();
        final var a_aa = FieldValue.ofFieldNames(inValue, ImmutableList.of("a", "aa"));
        final var a_ab = FieldValue.ofFieldNames(inValue, ImmutableList.of("a", "ab"));

        final var aaValue =
                RecordConstructorValue.ofColumns(
                        ImmutableList.of(
                                Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("aaa")), new LiteralValue<>("10")),
                                Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.INT), Optional.of("aab")), new LiteralValue<>(20)),
                                Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("aac")), new LiteralValue<>("30"))
                        ));

        final var transformMap =
                ImmutableMap.of(a_aa.getFieldPath(), aaValue,
                        a_ab.getFieldPath(), new LiteralValue<>(3));

        final var trie =
                RecordQueryUpdatePlan.computeTrieForFieldPaths(RecordQueryUpdatePlan.checkAndPrepareOrderedFieldPaths(transformMap),
                        transformMap);

        final var evaluationContext = EvaluationContext.forTypeRepository(TypeRepository.newBuilder().addTypeIfNeeded(inValue.getResultType()).build());
        final var inRecord = (Message)inValue.eval(null, evaluationContext);
        final var coercedType = Type.Record.fromDescriptor(TestRecordsTransformProto.DefaultTransformMessage.getDescriptor());
        final var result = (Message)MessageHelpers.transformMessage(null,
                evaluationContext,
                trie,
                null,
                coercedType,
                TestRecordsTransformProto.DefaultTransformMessage.getDescriptor(),
                inValue.getResultType(),
                inRecord.getDescriptorForType(),
                inRecord);
        final var resultSerialized = result.toByteString();
        final var typedResult = TestRecordsTransformProto.DefaultTransformMessage.parseFrom(resultSerialized);
        Assertions.assertEquals("10", typedResult.getA().getAa().getAaa());
        Assertions.assertEquals(20, typedResult.getA().getAa().getAab());
        Assertions.assertEquals("30", typedResult.getA().getAa().getAac());
        Assertions.assertEquals(3, typedResult.getA().getAb());
        Assertions.assertEquals("ac", typedResult.getA().getAc());
        Assertions.assertEquals("z", typedResult.getZ());
    }

    @Test
    void testTransformLeafsWithPromotion() throws Exception {
        final var inValue = makeRecordConstructor();
        final var a_aa_aaa = FieldValue.ofFieldNames(inValue, ImmutableList.of("a", "aa", "aaa"));
        final var a_aa_aab = FieldValue.ofFieldNames(inValue, ImmutableList.of("a", "aa", "aab"));
        final var a_ab = FieldValue.ofFieldNames(inValue, ImmutableList.of("a", "ab"));

        final var transformMap =
                ImmutableMap.<FieldValue.FieldPath, Value>of(a_aa_aaa.getFieldPath(), new LiteralValue<>("1"),
                        a_aa_aab.getFieldPath(), new LiteralValue<>(2),
                        a_ab.getFieldPath(), new LiteralValue<>(3));

        final var coercedType = Type.Record.fromDescriptor(TestRecordsTransformProto.TransformMessageMaxTypes.getDescriptor());
        final var transformationsTrie =
                RecordQueryUpdatePlan.computeTrieForFieldPaths(RecordQueryUpdatePlan.checkAndPrepareOrderedFieldPaths(transformMap),
                        transformMap);
        final var promotionsTrie = PromoteValue.computePromotionsTrie(coercedType, inValue.getResultType(), transformationsTrie);

        final var evaluationContext = EvaluationContext.forTypeRepository(TypeRepository.newBuilder().addTypeIfNeeded(inValue.getResultType()).build());
        final var inRecord = (Message)inValue.eval(null, evaluationContext);
        final var result = (Message)Verify.verifyNotNull(MessageHelpers.transformMessage(null,
                evaluationContext,
                transformationsTrie,
                promotionsTrie,
                coercedType,
                TestRecordsTransformProto.TransformMessageMaxTypes.getDescriptor(),
                inValue.getResultType(),
                inRecord.getDescriptorForType(),
                inRecord));
        Assertions.assertEquals(TestRecordsTransformProto.TransformMessageMaxTypes.getDescriptor().getFullName(), result.getDescriptorForType().getFullName());
        final var resultSerialized = result.toByteString();
        final var typedResult = TestRecordsTransformProto.TransformMessageMaxTypes.parseFrom(resultSerialized);
        Assertions.assertEquals("1", typedResult.getA().getAa().getAaa());
        Assertions.assertEquals(2.0, typedResult.getA().getAa().getAab());
        Assertions.assertEquals("aac", typedResult.getA().getAa().getAac());
        Assertions.assertEquals(3.0, typedResult.getA().getAb());
        Assertions.assertEquals("ac", typedResult.getA().getAc());
        Assertions.assertEquals("z", typedResult.getZ());
    }

    /**
     * Make sure sub messages do get copied when the target descriptor is different.
     * <pre>
     * {@code
     * a:                                               a:
     *   aa:                 -identical-->                aa:
     *     aaa: "aaa"                                       aaa: "aaa"
     *     aab: 1.23                                        aab: 1.23
     *     aac: "aac"                                       aac: "aac"
     *
     *   ab: 2               -(INT -> FLOAT)->            ab: 2.0
     *   ac: "ac"                                         ac: "ac"
     *
     * x:                    -identical-->              x:
     *   xa: "xa"                                         xa: "xa"
     *   xb: 3                                            xb: 3
     *   xc: "xc"                                         xc: "xc"
     *
     * z: "z"                                           z: "z"
     * }
     * </pre>
     *
     * @throws Exception if things fail
     */
    @Test
    void testPartialPromotionAndDeepCopy() throws Exception {
        final var inValue = makeRecordConstructorForPartialPromotion();

        final var coercedType = Type.Record.fromDescriptor(TestRecordsTransformProto.TransformMessageMaxTypes.getDescriptor());
        final var transformationsTrie =
                RecordQueryUpdatePlan.computeTrieForFieldPaths(RecordQueryUpdatePlan.checkAndPrepareOrderedFieldPaths(ImmutableMap.of()),
                        ImmutableMap.of());
        final var promotionsTrie = PromoteValue.computePromotionsTrie(coercedType, inValue.getResultType(), transformationsTrie);

        final var typeRepository =
                TypeRepository.newBuilder().addAllTypes(inValue.getDynamicTypes()).build();
        final var evaluationContext = EvaluationContext.forTypeRepository(typeRepository);
        final var inRecord = (Message)inValue.eval(null, evaluationContext);
        final var result = (Message)Verify.verifyNotNull(
                MessageHelpers.transformMessage(null,
                        evaluationContext,
                        transformationsTrie,
                        promotionsTrie,
                        coercedType,
                        TestRecordsTransformProto.TransformMessageMaxTypes.getDescriptor(),
                        inValue.getResultType(),
                        inRecord.getDescriptorForType(),
                        inRecord));
        Assertions.assertEquals(TestRecordsTransformProto.TransformMessageMaxTypes.getDescriptor().getFullName(), result.getDescriptorForType().getFullName());
        final var resultSerialized = result.toByteString();
        final var typedResult = TestRecordsTransformProto.TransformMessageMaxTypes.parseFrom(resultSerialized);
        Assertions.assertEquals("aaa", typedResult.getA().getAa().getAaa());
        Assertions.assertEquals(1.23d, typedResult.getA().getAa().getAab());
        Assertions.assertEquals("aac", typedResult.getA().getAa().getAac());
        Assertions.assertEquals(2.0, typedResult.getA().getAb());
        Assertions.assertEquals("ac", typedResult.getA().getAc());
        Assertions.assertEquals("z", typedResult.getZ());

        // make sure that a.aa was copied
        var inSubDescriptor = inRecord.getDescriptorForType();
        var inSubMessage = (Message)inRecord.getField(inSubDescriptor.findFieldByName("a"));
        inSubDescriptor = inSubMessage.getDescriptorForType();
        inSubMessage = (Message)inSubMessage.getField(inSubDescriptor.findFieldByName("aa"));
        var resultSubDescriptor = result.getDescriptorForType();
        var resultSubMessage = (Message)result.getField(resultSubDescriptor.findFieldByName("a"));
        resultSubDescriptor = resultSubMessage.getDescriptorForType();
        resultSubMessage = (Message)resultSubMessage.getField(resultSubDescriptor.findFieldByName("aa"));
        Assertions.assertNotSame(inSubMessage, resultSubMessage);

        // make sure that x was copied
        inSubDescriptor = inRecord.getDescriptorForType();
        inSubMessage = (Message)inRecord.getField(inSubDescriptor.findFieldByName("x"));
        resultSubDescriptor = result.getDescriptorForType();
        resultSubMessage = (Message)result.getField(resultSubDescriptor.findFieldByName("x"));
        Assertions.assertNotSame(inSubMessage, resultSubMessage);
    }

    /**
     * Make sure sub messages do get copied when necessary.
     * <pre>
     * {@code
     * a:                                               a:
     *   aa:                 -identical-->                aa:                             NO COPY
     *     aaa: "aaa"                                       aaa: "aaa"
     *     aab: 1.23                                        aab: 1.23
     *     aac: "aac"                                       aac: "aac"
     *
     *   ab: 2               -(INT -> FLOAT)->            ab: 2.0
     *   ac: "ac"                                         ac: "ac"
     *
     * x:                    -almost identical-->       x:                               COPY (because of nullability)
     *   xa: "xa"                                         xa: "xa"
     *   xb: 3                                            xb: 3
     *   xc: "xc"            -to not nullable-->          xc: "xc"
     *
     * z: "z"                                           z: "z"
     * }
     * </pre>
     *
     * @throws Exception if things fail
     */
    @Test
    void testPartialPromotionAndNoDeepCopy() throws Exception {
        final var inValue = makeRecordConstructorForPartialPromotion();

        final var coercedType = makeTargetRecordTypeForPartialPromotion();
        final var transformationsTrie =
                RecordQueryUpdatePlan.computeTrieForFieldPaths(RecordQueryUpdatePlan.checkAndPrepareOrderedFieldPaths(ImmutableMap.of()),
                        ImmutableMap.of());
        final var promotionsTrie = PromoteValue.computePromotionsTrie(coercedType, inValue.getResultType(), transformationsTrie);

        final var typeRepository =
                TypeRepository.newBuilder()
                        .addTypeIfNeeded(coercedType)
                        .addAllTypes(inValue.getDynamicTypes())
                        .build();
        final var evaluationContext = EvaluationContext.forTypeRepository(typeRepository);
        final var inRecord = (Message)inValue.eval(null, evaluationContext);
        final var result = (Message)Verify.verifyNotNull(
                MessageHelpers.transformMessage(null,
                        evaluationContext,
                        transformationsTrie,
                        promotionsTrie,
                        coercedType,
                        typeRepository.getMessageDescriptor(coercedType), // this is the dynamically-created protobuf descriptor we are coercing to
                        inValue.getResultType(),
                        inRecord.getDescriptorForType(),
                        inRecord));
        final var resultSerialized = result.toByteString();
        final var typedResult = TestRecordsTransformProto.TransformMessageMaxTypes.parseFrom(resultSerialized);
        Assertions.assertEquals("aaa", typedResult.getA().getAa().getAaa());
        Assertions.assertEquals(1.23d, typedResult.getA().getAa().getAab());
        Assertions.assertEquals("aac", typedResult.getA().getAa().getAac());
        Assertions.assertEquals(2.0, typedResult.getA().getAb());
        Assertions.assertEquals("ac", typedResult.getA().getAc());
        Assertions.assertEquals("z", typedResult.getZ());

        // make sure that a.aa wasn't copied
        var inSubDescriptor = inRecord.getDescriptorForType();
        var inSubMessage = (Message)inRecord.getField(inSubDescriptor.findFieldByName("a"));
        inSubDescriptor = inSubMessage.getDescriptorForType();
        inSubMessage = (Message)inSubMessage.getField(inSubDescriptor.findFieldByName("aa"));
        var resultSubDescriptor = result.getDescriptorForType();
        var resultSubMessage = (Message)result.getField(resultSubDescriptor.findFieldByName("a"));
        resultSubDescriptor = resultSubMessage.getDescriptorForType();
        resultSubMessage = (Message)resultSubMessage.getField(resultSubDescriptor.findFieldByName("aa"));
        Assertions.assertSame(inSubMessage, resultSubMessage);

        // make sure that x was copied since the nullability of in and target of one of its fields is different
        inSubDescriptor = inRecord.getDescriptorForType();
        inSubMessage = (Message)inRecord.getField(inSubDescriptor.findFieldByName("x"));
        resultSubDescriptor = result.getDescriptorForType();
        resultSubMessage = (Message)result.getField(resultSubDescriptor.findFieldByName("x"));
        Assertions.assertNotSame(inSubMessage, resultSubMessage);
    }

    @Test
    void testPromotionWithArrayToNotNullable() throws Exception {
        final var restaurantValue = makeRestaurantConstructor();
        final var evaluationContext = EvaluationContext.forTypeRepository(TypeRepository.newBuilder().addAllTypes(restaurantValue.getDynamicTypes()).build());
        final var restaurantRecord = (Message)restaurantValue.eval(null, evaluationContext);
        final var coercedType = Type.Record.fromDescriptor(TestRecords4Proto.RestaurantRecord.getDescriptor());
        final var transformationsTrie =
                RecordQueryUpdatePlan.computeTrieForFieldPaths(RecordQueryUpdatePlan.checkAndPrepareOrderedFieldPaths(ImmutableMap.of()),
                        ImmutableMap.of());
        final var promotionsTrie = PromoteValue.computePromotionsTrie(coercedType, restaurantValue.getResultType(), transformationsTrie);

        final var result = (Message)Verify.verifyNotNull(
                MessageHelpers.transformMessage(null,
                        evaluationContext,
                        transformationsTrie,
                        promotionsTrie,
                        coercedType,
                        TestRecords4Proto.RestaurantRecord.getDescriptor(),
                        restaurantValue.getResultType(),
                        restaurantRecord.getDescriptorForType(),
                        restaurantRecord));
        Assertions.assertEquals(TestRecords4Proto.RestaurantRecord.getDescriptor().getFullName(), result.getDescriptorForType().getFullName());
        final var resultSerialized = result.toByteString();
        final var typedResult = TestRecords4Proto.RestaurantRecord.parseFrom(resultSerialized);
        Assertions.assertEquals("McDonald's", typedResult.getName());
        Assertions.assertEquals(1000L, typedResult.getRestNo());
        final var reviews = typedResult.getReviewsList();
        Assertions.assertEquals(3, reviews.size());
        Assertions.assertEquals(1L, reviews.get(0).getReviewer());
        Assertions.assertEquals(10, reviews.get(0).getRating());
        Assertions.assertEquals(2L, reviews.get(1).getReviewer());
        Assertions.assertEquals(20, reviews.get(1).getRating());
        Assertions.assertEquals(3L, reviews.get(2).getReviewer());
        Assertions.assertEquals(30, reviews.get(2).getRating());
        Assertions.assertTrue(typedResult.getTagsList().isEmpty());
        Assertions.assertTrue(typedResult.getCustomerList().isEmpty());
    }

    @Test
    void testPromotionWithNullArrayToNullable() throws Exception {
        var restaurantValue = makeRestaurantConstructorWithNull();
        var evaluationContext = EvaluationContext.forTypeRepository(TypeRepository.newBuilder().addAllTypes(restaurantValue.getDynamicTypes()).build());
        var restaurantRecord = (Message)restaurantValue.eval(null, evaluationContext);
        final var coercedType = Type.Record.fromDescriptor(TestRecords4WrapperProto.RestaurantRecord.getDescriptor());
        final var transformationsTrie =
                RecordQueryUpdatePlan.computeTrieForFieldPaths(RecordQueryUpdatePlan.checkAndPrepareOrderedFieldPaths(ImmutableMap.of()),
                        ImmutableMap.of());
        final var promotionsTrie = PromoteValue.computePromotionsTrie(coercedType, restaurantValue.getResultType(), transformationsTrie);

        final var result = (Message)Verify.verifyNotNull(
                MessageHelpers.transformMessage(null,
                        evaluationContext,
                        transformationsTrie,
                        promotionsTrie,
                        coercedType,
                        TestRecords4WrapperProto.RestaurantRecord.getDescriptor(),
                        restaurantValue.getResultType(),
                        restaurantRecord.getDescriptorForType(),
                        restaurantRecord));
        Assertions.assertEquals(TestRecords4WrapperProto.RestaurantRecord.getDescriptor().getFullName(), result.getDescriptorForType().getFullName());
        final var resultSerialized = result.toByteString();
        final var typedResult = TestRecords4WrapperProto.RestaurantRecord.parseFrom(resultSerialized);
        Assertions.assertEquals("McDonald's", typedResult.getName());
        Assertions.assertEquals(1000L, typedResult.getRestNo());
        Assertions.assertTrue(typedResult.hasReviews());
        final var reviews = typedResult.getReviews().getValuesList();
        Assertions.assertEquals(3, reviews.size());
        Assertions.assertEquals(1L, reviews.get(0).getReviewer());
        Assertions.assertEquals(10, reviews.get(0).getRating());
        Assertions.assertEquals(2L, reviews.get(1).getReviewer());
        Assertions.assertEquals(20, reviews.get(1).getRating());
        Assertions.assertEquals(3L, reviews.get(2).getReviewer());
        Assertions.assertEquals(30, reviews.get(2).getRating());
        Assertions.assertFalse(typedResult.hasTags());
        Assertions.assertTrue(typedResult.hasCustomer());
        final var customers = typedResult.getCustomer().getValuesList();
        Assertions.assertEquals("George", customers.get(0));
        Assertions.assertEquals("Charles", customers.get(1));
        Assertions.assertEquals("William", customers.get(2));
    }

    @Test
    void testPromotionWithArrayToNullable() throws Exception {
        final var restaurantValue = makeRestaurantConstructor();
        final var evaluationContext = EvaluationContext.forTypeRepository(TypeRepository.newBuilder().addAllTypes(restaurantValue.getDynamicTypes()).build());
        final var restaurantRecord = (Message)restaurantValue.eval(null, evaluationContext);
        final var coercedType = Type.Record.fromDescriptor(TestRecords4WrapperProto.RestaurantRecord.getDescriptor());
        final var transformationsTrie =
                RecordQueryUpdatePlan.computeTrieForFieldPaths(RecordQueryUpdatePlan.checkAndPrepareOrderedFieldPaths(ImmutableMap.of()),
                        ImmutableMap.of());
        final var promotionsTrie = PromoteValue.computePromotionsTrie(coercedType, restaurantValue.getResultType(), transformationsTrie);

        final var result = (Message)Verify.verifyNotNull(
                MessageHelpers.transformMessage(null,
                        evaluationContext,
                        transformationsTrie,
                        promotionsTrie,
                        coercedType,
                        TestRecords4WrapperProto.RestaurantRecord.getDescriptor(),
                        restaurantValue.getResultType(),
                        restaurantRecord.getDescriptorForType(),
                        restaurantRecord));
        Assertions.assertEquals(TestRecords4WrapperProto.RestaurantRecord.getDescriptor().getFullName(), result.getDescriptorForType().getFullName());
        final var resultSerialized = result.toByteString();
        final var typedResult = TestRecords4WrapperProto.RestaurantRecord.parseFrom(resultSerialized);
        Assertions.assertEquals("McDonald's", typedResult.getName());
        Assertions.assertEquals(1000L, typedResult.getRestNo());
        Assertions.assertTrue(typedResult.hasReviews());
        final var reviews = typedResult.getReviews().getValuesList();
        Assertions.assertEquals(3, reviews.size());
        Assertions.assertEquals(1L, reviews.get(0).getReviewer());
        Assertions.assertEquals(10, reviews.get(0).getRating());
        Assertions.assertEquals(2L, reviews.get(1).getReviewer());
        Assertions.assertEquals(20, reviews.get(1).getRating());
        Assertions.assertEquals(3L, reviews.get(2).getReviewer());
        Assertions.assertEquals(30, reviews.get(2).getRating());
        Assertions.assertTrue(typedResult.hasTags());
        Assertions.assertTrue(typedResult.getTags().getValuesList().isEmpty());
        Assertions.assertTrue(typedResult.hasCustomer());
        Assertions.assertTrue(typedResult.getCustomer().getValuesList().isEmpty());
    }

    /**
     * Tests that a field that is {@code null} cannot be coerced into a field that is not nullable.
     */
    @Test
    void testPromotionWithNullArrayToNotNullable() {
        final var restaurantValue = makeRestaurantConstructorWithNull();
        final var evaluationContext = EvaluationContext.forTypeRepository(TypeRepository.newBuilder().addAllTypes(restaurantValue.getDynamicTypes()).build());
        final var restaurantRecord = (Message)restaurantValue.eval(null, evaluationContext);
        final var coercedType = Type.Record.fromDescriptor(TestRecords4Proto.RestaurantRecord.getDescriptor());
        final var transformationsTrie =
                RecordQueryUpdatePlan.computeTrieForFieldPaths(RecordQueryUpdatePlan.checkAndPrepareOrderedFieldPaths(ImmutableMap.of()),
                        ImmutableMap.of());
        final var promotionsTrie = PromoteValue.computePromotionsTrie(coercedType, restaurantValue.getResultType(), transformationsTrie);

        Assertions.assertThrows(SemanticException.class, () ->
                MessageHelpers.transformMessage(null,
                        evaluationContext,
                        transformationsTrie,
                        promotionsTrie,
                        coercedType,
                        TestRecords4Proto.RestaurantRecord.getDescriptor(),
                        restaurantValue.getResultType(),
                        restaurantRecord.getDescriptorForType(),
                        restaurantRecord));
    }

    @Test
    void testCoerceArrayNotNullableWithPrimitiveType() {
        testCoerceArrayWithPrimitive(false);
    }

    @Test
    void testCoerceArrayNullableWithPrimitiveType() {
        testCoerceArrayWithPrimitive(true);
    }

    private void testCoerceArrayWithPrimitive(boolean nullable) {
        final var objects = List.of("abc", "def", "ghi");
        final var strArrayValue = AbstractArrayConstructorValue.LightArrayConstructorValue.of(
                objects.stream().map(LiteralValue::ofScalar).collect(Collectors.toList()));
        final var targetType = new Type.Array(nullable, Type.primitiveType(Type.TypeCode.STRING));
        Descriptors.Descriptor wrapperDescriptor = null;
        if (nullable) {
            final var typeRepository = TypeRepository.newBuilder().addTypeIfNeeded(targetType).build();
            wrapperDescriptor = Iterables.getOnlyElement(typeRepository.getMessageDescriptors());
            Assertions.assertTrue(NullableArrayTypeUtils.describesWrappedArray(wrapperDescriptor));
        }
        var msg = MessageHelpers.coerceArray(targetType, (Type.Array) strArrayValue.getResultType(),
                wrapperDescriptor, null, objects);
        msg = nullable ? NullableArrayTypeUtils.unwrapIfArray(msg, targetType) : msg;
        final var actualList = (List) msg;
        Assertions.assertTrue(actualList instanceof List);
        Assertions.assertEquals(3, actualList.size());
        Assertions.assertEquals("abc", actualList.get(0));
        Assertions.assertEquals("def", actualList.get(1));
        Assertions.assertEquals("ghi", actualList.get(2));
    }

    @Test
    void testCoerceArrayNotNullableWithRecordType() {
        testCoerceArrayWithRecordType(false);
    }

    @Test
    void testCoerceArrayNullableWithRecordType() {
        testCoerceArrayWithRecordType(true);
    }

    private void testCoerceArrayWithRecordType(boolean nullable) {
        final var fields = List.of(
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG), Optional.of("reviewer")),
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.INT), Optional.of("rating")));
        final var records = new ArrayList<Value>();
        for (int i = 0; i < 3; i++) {
            records.add(RecordConstructorValue.ofColumns(List.of(
                    Column.of(fields.get(0), new LiteralValue<>((long)i)),
                    Column.of(fields.get(1), new LiteralValue<>(i + 1)))));
        }
        final var arrayValue = AbstractArrayConstructorValue.LightArrayConstructorValue.of(records);
        final var targetType = new Type.Array(nullable, Type.Record.fromFields(fields));
        final var typeRepository = TypeRepository.newBuilder()
                .addTypeIfNeeded(targetType)
                .addTypeIfNeeded(records.get(0).getResultType()).build();
        Descriptors.Descriptor wrapperDescriptor = null;
        if (nullable) {
            final var arrayWrappers = typeRepository.getMessageDescriptors().stream()
                    .filter(NullableArrayTypeUtils::describesWrappedArray)
                    .collect(Collectors.toList());
            Assertions.assertEquals(1, arrayWrappers.size());
            wrapperDescriptor = arrayWrappers.get(0);
        }
        var msg = MessageHelpers.coerceArray(targetType, (Type.Array) arrayValue.getResultType(),
                wrapperDescriptor, null, arrayValue.eval(null, EvaluationContext.forTypeRepository(typeRepository)));
        msg = nullable ? NullableArrayTypeUtils.unwrapIfArray(msg, targetType) : msg;
        final var actualList = (List) msg;
        Assertions.assertEquals(3, actualList.size());
        for (int i = 0; i < 3; i++) {
            final var recordValues = ((DynamicMessage) actualList.get(i)).getAllFields().values();
            Assertions.assertTrue(recordValues.contains((long)i));
            Assertions.assertTrue(recordValues.contains(i + 1));
        }
    }

    private static Type.Record someRecordType() {
        final var aaType = Type.Record.fromFields(ImmutableList.of(
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("aaa")),
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.INT), Optional.of("aab")),
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("aac"))));

        final var aType = Type.Record.fromFields(ImmutableList.of(
                Type.Record.Field.of(aaType, Optional.of("aa")),
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.INT), Optional.of("ab")),
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("ac"))));

        final var xType = Type.Record.fromFields(ImmutableList.of(
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("xa")),
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.INT), Optional.of("xb")),
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("xc"))));

        return Type.Record.fromFields(ImmutableList.of(
                Type.Record.Field.of(aType, Optional.of("a")),
                Type.Record.Field.of(xType, Optional.of("x")),
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("z"))));
    }

    @Nonnull
    private static Value makeRecordConstructor() {
        final var aaType = Type.Record.fromFields(false, ImmutableList.of(
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("aaa")),
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.INT), Optional.of("aab")),
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("aac"))));

        final var aaValue =
                RecordConstructorValue.ofColumns(
                        ImmutableList.of(
                                Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("aaa")), new LiteralValue<>("aaa")),
                                Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.INT), Optional.of("aab")), new LiteralValue<>(1)),
                                Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("aac")), new LiteralValue<>("aac"))
                        ));
        Verify.verify(aaType.equals(aaValue.getResultType()));

        final var aType = Type.Record.fromFields(false, ImmutableList.of(
                Type.Record.Field.of(aaType, Optional.of("aa")),
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.INT), Optional.of("ab")),
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("ac"))));

        final var aValue =
                RecordConstructorValue.ofColumns(
                        ImmutableList.of(
                                Column.of(Type.Record.Field.of(aaType, Optional.of("aa")), aaValue),
                                Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.INT), Optional.of("ab")), new LiteralValue<>(2)),
                                Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("ac")), new LiteralValue<>("ac"))
                        ));
        Verify.verify(aType.equals(aValue.getResultType()));

        final var xType = Type.Record.fromFields(false, ImmutableList.of(
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("xa")),
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.INT), Optional.of("xb")),
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("xc"))));

        final var xValue =
                RecordConstructorValue.ofColumns(
                        ImmutableList.of(
                                Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("xa")), new LiteralValue<>("xa")),
                                Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.INT), Optional.of("xb")), new LiteralValue<>(3)),
                                Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("xc")), new LiteralValue<>("xc"))
                        ));
        Verify.verify(xType.equals(xValue.getResultType()));

        final var returnType =
                Type.Record.fromFields(false, ImmutableList.of(
                        Type.Record.Field.of(aType, Optional.of("a")),
                        Type.Record.Field.of(xType, Optional.of("x")),
                        Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("z"))));

        final var returnValue = RecordConstructorValue.ofColumns(
                ImmutableList.of(
                        Column.of(Type.Record.Field.of(aType, Optional.of("a")), aValue),
                        Column.of(Type.Record.Field.of(xType, Optional.of("x")), xValue),
                        Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("z")), new LiteralValue<>("z"))
                ));
        Verify.verify(returnType.equals(returnValue.getResultType()));
        return returnValue;
    }

    @Nonnull
    private static Value makeRecordConstructorForPartialPromotion() {
        final var aaType = Type.Record.fromFields(false, ImmutableList.of(
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING, false), Optional.of("aaa")),
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.DOUBLE, false), Optional.of("aab")),
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING, true), Optional.of("aac"))));

        final var aaValue =
                RecordConstructorValue.ofColumns(
                        ImmutableList.of(
                                Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING, false), Optional.of("aaa")), new LiteralValue<>("aaa")),
                                Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.DOUBLE, false), Optional.of("aab")), new LiteralValue<>(1.23d)),
                                Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING, true), Optional.of("aac")), new LiteralValue<>("aac"))
                        ));
        Verify.verify(aaType.equals(aaValue.getResultType()));

        final var aType = Type.Record.fromFields(false, ImmutableList.of(
                Type.Record.Field.of(aaType, Optional.of("aa")),
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.INT, false), Optional.of("ab")),
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING, true), Optional.of("ac"))));

        final var aValue =
                RecordConstructorValue.ofColumns(
                        ImmutableList.of(
                                Column.of(Type.Record.Field.of(aaType, Optional.of("aa")), aaValue),
                                Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.INT, false), Optional.of("ab")), new LiteralValue<>(2)),
                                Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING, true), Optional.of("ac")), new LiteralValue<>("ac"))
                        ));
        Verify.verify(aType.equals(aValue.getResultType()));

        final var xType = Type.Record.fromFields(false, ImmutableList.of(
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING, false), Optional.of("xa")),
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.INT, false), Optional.of("xb")),
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING, true), Optional.of("xc"))));

        final var xValue =
                RecordConstructorValue.ofColumns(
                        ImmutableList.of(
                                Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING, false), Optional.of("xa")), new LiteralValue<>("xa")),
                                Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.INT, false), Optional.of("xb")), new LiteralValue<>(3)),
                                Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING, true), Optional.of("xc")), new LiteralValue<>("xc"))
                        ));
        Verify.verify(xType.equals(xValue.getResultType()));

        final var returnType =
                Type.Record.fromFields(false, ImmutableList.of(
                        Type.Record.Field.of(aType, Optional.of("a")),
                        Type.Record.Field.of(xType, Optional.of("x")),
                        Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING, false), Optional.of("z"))));

        final var returnValue = RecordConstructorValue.ofColumns(
                ImmutableList.of(
                        Column.of(Type.Record.Field.of(aType, Optional.of("a")), aValue),
                        Column.of(Type.Record.Field.of(xType, Optional.of("x")), xValue),
                        Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING, false), Optional.of("z")), new LiteralValue<>("z"))
                ));
        Verify.verify(returnType.equals(returnValue.getResultType()));
        return returnValue;
    }

    private static Type.Record makeTargetRecordTypeForPartialPromotion() {
        final var aaType = Type.Record.fromFields(false, ImmutableList.of(
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING, false), Optional.of("aaa")),
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.DOUBLE, false), Optional.of("aab")),
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING, true), Optional.of("aac"))));

        final var aType = Type.Record.fromFields(ImmutableList.of(
                Type.Record.Field.of(aaType, Optional.of("aa")),
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.FLOAT, false), Optional.of("ab")),
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING, true), Optional.of("ac"))));

        final var xType = Type.Record.fromFields(ImmutableList.of(
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING, false), Optional.of("xa")),
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.INT, false), Optional.of("xb")),
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING, false), Optional.of("xc"))));

        return Type.Record.fromFields(false, ImmutableList.of(
                Type.Record.Field.of(aType, Optional.of("a")),
                Type.Record.Field.of(xType, Optional.of("x")),
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING, false), Optional.of("z"))));
    }

    @Nonnull
    private static Value makeSparseRecordConstructor() {
        final var aaType = Type.Record.fromFields(false, ImmutableList.of(
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("aaa")),
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.INT), Optional.of("aab")),
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("aac"))));

        final var aaValue = new NullValue(aaType);
        Verify.verify(aaType.equals(aaValue.getResultType().notNullable()));

        final var aType = Type.Record.fromFields(false, ImmutableList.of(
                Type.Record.Field.of(aaValue.getResultType(), Optional.of("aa")),
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.INT), Optional.of("ab")),
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("ac"))));

        final var aValue =
                RecordConstructorValue.ofColumns(
                        ImmutableList.of(
                                Column.of(Type.Record.Field.of(aaValue.getResultType(), Optional.of("aa")), aaValue),
                                Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.INT), Optional.of("ab")), new LiteralValue<>(2)),
                                Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("ac")), new LiteralValue<>("ac"))
                        ));
        Verify.verify(aType.equals(aValue.getResultType()));

        final var xType = Type.Record.fromFields(false, ImmutableList.of(
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("xa")),
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.INT), Optional.of("xb")),
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("xc"))));

        final var xValue =
                RecordConstructorValue.ofColumns(
                        ImmutableList.of(
                                Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("xa")), new LiteralValue<>("xa")),
                                Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.INT), Optional.of("xb")), new LiteralValue<>(3)),
                                Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("xc")), new LiteralValue<>("xc"))
                        ));
        Verify.verify(xType.equals(xValue.getResultType()));

        final var returnType =
                Type.Record.fromFields(false, ImmutableList.of(
                        Type.Record.Field.of(aType, Optional.of("a")),
                        Type.Record.Field.of(xType, Optional.of("x")),
                        Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("z"))));

        final var returnValue = RecordConstructorValue.ofColumns(
                ImmutableList.of(
                        Column.of(Type.Record.Field.of(aType, Optional.of("a")), aValue),
                        Column.of(Type.Record.Field.of(xType, Optional.of("x")), xValue),
                        Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("z")), new LiteralValue<>("z"))
                ));
        Verify.verify(returnType.equals(returnValue.getResultType()));
        return returnValue;
    }

    @Nonnull
    private static Value makeRestaurantConstructor() {
        final var reviewType = Type.Record.fromFields(false, ImmutableList.of(
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG), Optional.of("reviewer")),
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.INT), Optional.of("rating"))));

        final var review1 =
                RecordConstructorValue.ofColumns(
                        ImmutableList.of(
                                Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG), Optional.of("reviewer")), new LiteralValue<>(1L)),
                                Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.INT), Optional.of("rating")), new LiteralValue<>(10))
                        ));
        Verify.verify(reviewType.equals(review1.getResultType()));
        final var review2 =
                RecordConstructorValue.ofColumns(
                        ImmutableList.of(
                                Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG), Optional.of("reviewer")), new LiteralValue<>(2L)),
                                Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.INT), Optional.of("rating")), new LiteralValue<>(20))
                        ));
        Verify.verify(reviewType.equals(review2.getResultType()));
        final var review3 =
                RecordConstructorValue.ofColumns(
                        ImmutableList.of(
                                Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG), Optional.of("reviewer")), new LiteralValue<>(3L)),
                                Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.INT), Optional.of("rating")), new LiteralValue<>(30))
                        ));
        Verify.verify(reviewType.equals(review3.getResultType()));

        final var reviews =
                AbstractArrayConstructorValue.LightArrayConstructorValue.of(review1, review2, review3);

        final var tagsType = Type.Record.fromFields(false, ImmutableList.of(
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("value")),
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.INT), Optional.of("weight"))));

        final var tags = AbstractArrayConstructorValue.LightArrayConstructorValue.emptyArray(tagsType);
        final var customer = AbstractArrayConstructorValue.LightArrayConstructorValue.emptyArray(Type.primitiveType(Type.TypeCode.STRING, false));

        return RecordConstructorValue.ofColumns(
                ImmutableList.of(
                        Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG), Optional.of("rest_no")), new LiteralValue<>(1000L)),
                        Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("name")), new LiteralValue<>("McDonald's")),
                        Column.of(Type.Record.Field.of(reviews.getResultType(), Optional.of("reviews")), reviews),
                        Column.of(Type.Record.Field.of(tags.getResultType(), Optional.of("tags")), tags),
                        Column.of(Type.Record.Field.of(customer.getResultType(), Optional.of("customer")), customer)
                ));
    }

    @Nonnull
    private static Value makeRestaurantConstructorWithNull() {
        final var reviewType = Type.Record.fromFields(false, ImmutableList.of(
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG), Optional.of("reviewer")),
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.INT), Optional.of("rating"))));

        final var review1 =
                RecordConstructorValue.ofColumns(
                        ImmutableList.of(
                                Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG), Optional.of("reviewer")), new LiteralValue<>(1L)),
                                Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.INT), Optional.of("rating")), new LiteralValue<>(10))
                        ));
        Verify.verify(reviewType.equals(review1.getResultType()));
        final var review2 =
                RecordConstructorValue.ofColumns(
                        ImmutableList.of(
                                Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG), Optional.of("reviewer")), new LiteralValue<>(2L)),
                                Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.INT), Optional.of("rating")), new LiteralValue<>(20))
                        ));
        Verify.verify(reviewType.equals(review2.getResultType()));
        final var review3 =
                RecordConstructorValue.ofColumns(
                        ImmutableList.of(
                                Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG), Optional.of("reviewer")), new LiteralValue<>(3L)),
                                Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.INT), Optional.of("rating")), new LiteralValue<>(30))
                        ));
        Verify.verify(reviewType.equals(review3.getResultType()));

        final var reviews =
                AbstractArrayConstructorValue.LightArrayConstructorValue.of(review1, review2, review3);

        final var tagsType = Type.Record.fromFields(false, ImmutableList.of(
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("value")),
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.INT), Optional.of("weight"))));

        final var tags = new NullValue(new Type.Array(tagsType));
        final var customer = AbstractArrayConstructorValue.LightArrayConstructorValue.of(
                LiteralValue.ofScalar("George"),
                LiteralValue.ofScalar("Charles"),
                LiteralValue.ofScalar("William")
        );

        return RecordConstructorValue.ofColumns(
                ImmutableList.of(
                        Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG), Optional.of("rest_no")), new LiteralValue<>(1000L)),
                        Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("name")), new LiteralValue<>("McDonald's")),
                        Column.of(Type.Record.Field.of(reviews.getResultType(), Optional.of("reviews")), reviews),
                        Column.of(Type.Record.Field.of(tags.getResultType(), Optional.of("tags")), tags),
                        Column.of(Type.Record.Field.of(customer.getResultType(), Optional.of("customer")), customer)
                ));
    }
}
