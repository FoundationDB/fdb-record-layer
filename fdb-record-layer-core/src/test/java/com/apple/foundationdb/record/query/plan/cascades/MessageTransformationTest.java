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
import com.apple.foundationdb.record.TestRecordsTransformProto;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.TypeRepository;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.cascades.values.MessageHelpers;
import com.apple.foundationdb.record.query.plan.cascades.values.NullValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryAbstractDataModificationPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUpdatePlan;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Optional;

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
                () -> RecordQueryAbstractDataModificationPlan.computePromotionsTrie(inValue.getResultType(), inValue.getResultType(), transformationsTrie));
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
        final var promotionsTrie = RecordQueryAbstractDataModificationPlan.computePromotionsTrie(coercedType, inValue.getResultType(), transformationsTrie);

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
        Assertions.assertEquals(2, typedResult.getA().getAa().getAab());
        Assertions.assertEquals("aac", typedResult.getA().getAa().getAac());
        Assertions.assertEquals(3, typedResult.getA().getAb());
        Assertions.assertEquals("ac", typedResult.getA().getAc());
        Assertions.assertEquals("z", typedResult.getZ());
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
}
