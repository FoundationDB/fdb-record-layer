/*
 * MetaDataValidatorTest.java
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

import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.TestNoRecordTypesProto;
import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.TestRecords4Proto;
import com.apple.foundationdb.record.TestRecordsEnumProto;
import com.apple.foundationdb.record.TestRecordsNameClashProto;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerFactoryRegistryImpl;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests for {@link MetaDataValidator}.
 */
public class MetaDataValidatorTest {

    private void validate(RecordMetaDataBuilder metaData) {
        final MetaDataValidator validator = new MetaDataValidator(metaData, IndexMaintainerFactoryRegistryImpl.instance());
        validator.validate();
    }

    private <T extends Throwable> void assertInvalid(@Nonnull Class<T> errClass, @Nonnull String errMsg, @Nonnull RecordMetaDataBuilder metaDataBuilder) {
        T err = assertThrows(errClass, () -> validate(metaDataBuilder));
        assertThat(err.getMessage(), containsString(errMsg));
    }

    private void assertInvalid(@Nonnull String errMsg, @Nonnull RecordMetaDataBuilder metaDataBuilder) {
        assertInvalid(MetaDataException.class, errMsg, metaDataBuilder);
    }

    @Test
    public void noRecordTypes() {
        RecordMetaDataBuilder metaData = RecordMetaData.newBuilder().setRecords(TestNoRecordTypesProto.getDescriptor());
        assertInvalid("No record types defined in meta-data", metaData);
    }

    /**
     * Validate that different record types that have the same name (because of imports) throw an error. In theory,
     * this should allow the user to, say, specify the fully qualified type, but it is better to throw an error
     * than to override the existing record type.
     */
    @Test
    public void duplicateRecordTypeNames() {
        MetaDataException e = assertThrows(MetaDataException.class, () -> RecordMetaData.newBuilder().setRecords(TestRecordsNameClashProto.getDescriptor()));
        assertThat(e.getMessage(), containsString("There is already a record type named MySimpleRecord"));
    }

    @Test
    public void duplicateRecordTypeKey() {
        RecordMetaDataBuilder metaData = RecordMetaData.newBuilder().setRecords(TestRecords1Proto.getDescriptor());
        metaData.getRecordType("MySimpleRecord").setPrimaryKey(Key.Expressions.concat(Key.Expressions.recordType(), Key.Expressions.field("rec_no")));
        metaData.getRecordType("MySimpleRecord").setRecordTypeKey("same");
        metaData.getRecordType("MyOtherRecord").setPrimaryKey(Key.Expressions.concat(Key.Expressions.recordType(), Key.Expressions.field("rec_no")));
        metaData.getRecordType("MyOtherRecord").setRecordTypeKey("same");
        assertInvalid("Same record type key same used by both", metaData);
    }

    @Test
    public void duplicateIntegralRecordTypeKey() {
        List<Object> alternateKeys = Arrays.asList((byte)42, (short)42, 42, BigInteger.valueOf(42L));
        for (Object alternateKey : alternateKeys) {
            RecordMetaDataBuilder metaData = RecordMetaData.newBuilder().setRecords(TestRecords1Proto.getDescriptor());
            metaData.getRecordType("MySimpleRecord").setRecordTypeKey(42L);
            metaData.getRecordType("MyOtherRecord").setRecordTypeKey(alternateKey);
            assertInvalid("Same record type key 42 used by both", metaData);
        }
    }

    @Test
    public void duplicateByteArrayRecordTypeKey() {
        RecordMetaDataBuilder metaData = RecordMetaData.newBuilder().setRecords(TestRecords1Proto.getDescriptor());
        byte[] recordTypeKey = new byte[]{(byte)0x0f, (byte)0xdb};
        metaData.getRecordType("MySimpleRecord").setRecordTypeKey(recordTypeKey);
        metaData.getRecordType("MyOtherRecord").setRecordTypeKey(Arrays.copyOf(recordTypeKey, recordTypeKey.length));
        assertInvalid("Same record type key", metaData);
    }

    @Test
    public void duplicateImplicitAndExplicitRecordTypeKey() {
        RecordMetaDataBuilder metaData = RecordMetaData.newBuilder().setRecords(TestRecords1Proto.getDescriptor());
        int simpleRecordKey = TestRecords1Proto.RecordTypeUnion.getDescriptor().findFieldByName("_MySimpleRecord").getNumber();
        metaData.getRecordType("MyOtherRecord").setRecordTypeKey(simpleRecordKey);
        assertInvalid("Same record type key " + simpleRecordKey + " used by both", metaData);

        metaData.getRecordType("MyOtherRecord").setRecordTypeKey((long)simpleRecordKey);
        assertInvalid("Same record type key " + simpleRecordKey + " used by both", metaData);
    }

    @Test
    public void primaryKeyRepeated() {
        RecordMetaDataBuilder metaData = RecordMetaData.newBuilder().setRecords(TestRecords1Proto.getDescriptor());
        metaData.getRecordType("MySimpleRecord").setPrimaryKey(Key.Expressions.field("repeater", KeyExpression.FanType.FanOut));
        assertInvalid("Primary key for MySimpleRecord can generate more than one entry", metaData);
    }

    @Test
    public void duplicateSubspaceKey() {
        RecordMetaDataBuilder metaData = RecordMetaData.newBuilder().setRecords(TestRecords1Proto.getDescriptor());
        metaData.getIndex("MySimpleRecord$str_value_indexed").setSubspaceKey("same");
        metaData.getIndex("MySimpleRecord$num_value_3_indexed").setSubspaceKey("same");
        assertInvalid("Same subspace key same used by both", metaData);
    }

    @Test
    public void duplicateIntegralSubspaceKey() {
        List<Object> alternateKeys = Arrays.asList((byte)42, (short)42, 42, BigInteger.valueOf(42L));
        for (Object alternateKey : alternateKeys) {
            RecordMetaDataBuilder metaData = RecordMetaData.newBuilder().setRecords(TestRecords1Proto.getDescriptor());
            metaData.getIndex("MySimpleRecord$str_value_indexed").setSubspaceKey(42L);
            metaData.getIndex("MySimpleRecord$num_value_3_indexed").setSubspaceKey(alternateKey);
            assertInvalid("Same subspace key 42 used by both", metaData);
        }
    }

    @Test
    public void duplicateEnumSubspaceKey() {
        // This exact use case is somewhat contrived, but one could imagine maintaining an enum with one entry per index
        RecordMetaDataBuilder metaData = RecordMetaData.newBuilder().setRecords(TestRecords1Proto.getDescriptor());
        final Descriptors.EnumValueDescriptor enumValueDescriptor = TestRecordsEnumProto.MyShapeRecord.Size.SMALL.getValueDescriptor();
        metaData.getIndex("MySimpleRecord$str_value_indexed").setSubspaceKey((long)enumValueDescriptor.getNumber());
        metaData.getIndex("MySimpleRecord$num_value_3_indexed").setSubspaceKey(enumValueDescriptor);
        assertInvalid("Same subspace key " + enumValueDescriptor.getNumber() + " used by both", metaData);
    }

    @Test
    public void duplicateByteArraySubspaceKey() {
        RecordMetaDataBuilder metaData = RecordMetaData.newBuilder().setRecords(TestRecords1Proto.getDescriptor());
        byte[] subspaceKey = new byte[]{(byte)0x0f, (byte)0xdb};
        metaData.getIndex("MySimpleRecord$str_value_indexed").setSubspaceKey(subspaceKey);
        metaData.getIndex("MySimpleRecord$num_value_3_indexed").setSubspaceKey(Arrays.copyOf(subspaceKey, subspaceKey.length));
        assertInvalid("Same subspace key", metaData);
    }

    @Test
    public void duplicateFormerSubspaceKey() {
        RecordMetaDataBuilder metaData = RecordMetaData.newBuilder().setRecords(TestRecords1Proto.getDescriptor());
        metaData.getIndex("MySimpleRecord$str_value_indexed").setSubspaceKey("same");
        metaData.removeIndex("MySimpleRecord$str_value_indexed");
        metaData.getIndex("MySimpleRecord$num_value_3_indexed").setSubspaceKey("same");
        metaData.removeIndex("MySimpleRecord$num_value_3_indexed");
        assertInvalid("Same subspace key same used by two former indexes MySimpleRecord$num_value_3_indexed and MySimpleRecord$str_value_indexed", metaData);

        RecordMetaDataProto.MetaData.Builder protoBuilder = metaData.build(false).toProto().toBuilder();
        protoBuilder.getFormerIndexesBuilder(0).clearFormerName();
        protoBuilder.getFormerIndexesBuilder(1).clearFormerName();
        metaData = RecordMetaData.newBuilder().setRecords(protoBuilder.build());
        assertInvalid("Same subspace key same used by two former indexes <unknown> and <unknown>", metaData);
    }

    @Test
    public void duplicateIntegralFormerSubspaceKey() {
        List<Object> alternateKeys = Arrays.asList((byte)42, (short)42, 42);
        for (Object alternateKey : alternateKeys) {
            RecordMetaDataBuilder metaData = RecordMetaData.newBuilder().setRecords(TestRecords1Proto.getDescriptor());
            metaData.getIndex("MySimpleRecord$str_value_indexed").setSubspaceKey(42L);
            metaData.removeIndex("MySimpleRecord$str_value_indexed");
            metaData.getIndex("MySimpleRecord$num_value_3_indexed").setSubspaceKey(alternateKey);
            metaData.removeIndex("MySimpleRecord$num_value_3_indexed");
            assertInvalid("Same subspace key 42 used by two former indexes", metaData);
        }
    }

    @Test
    public void duplicateFormerAndCurrentSubspaceKey() {
        RecordMetaDataBuilder metaData = RecordMetaData.newBuilder().setRecords(TestRecords1Proto.getDescriptor());
        metaData.getIndex("MySimpleRecord$str_value_indexed").setSubspaceKey("same");
        metaData.removeIndex("MySimpleRecord$str_value_indexed");
        metaData.getIndex("MySimpleRecord$num_value_3_indexed").setSubspaceKey("same");
        assertInvalid("Same subspace key same used by index MySimpleRecord$num_value_3_indexed and former index MySimpleRecord$str_value_indexed", metaData);

        RecordMetaDataProto.MetaData.Builder protoBuilder = metaData.build(false).toProto().toBuilder();
        protoBuilder.getFormerIndexesBuilder(0).clearFormerName();
        metaData = RecordMetaData.newBuilder().setRecords(protoBuilder.build());
        assertInvalid("Same subspace key same used by index MySimpleRecord$num_value_3_indexed and former index", metaData);
    }

    @Test
    public void duplicateIntegralFormerAndCurrentSubspaceKeys() {
        RecordMetaDataBuilder metaData = RecordMetaData.newBuilder().setRecords(TestRecords1Proto.getDescriptor());
        metaData.getIndex("MySimpleRecord$str_value_indexed").setSubspaceKey(42L);
        metaData.removeIndex("MySimpleRecord$str_value_indexed");
        metaData.getIndex("MySimpleRecord$num_value_3_indexed").setSubspaceKey(42);
        assertInvalid("Same subspace key 42 used by index MySimpleRecord$num_value_3_indexed and former index MySimpleRecord$str_value_indexed", metaData);
    }

    @Test
    public void badPrimaryKeyField() {
        RecordMetaDataBuilder metaData = RecordMetaData.newBuilder().setRecords(TestRecords1Proto.getDescriptor());
        metaData.getRecordType("MySimpleRecord").setPrimaryKey(Key.Expressions.field("no_such_field"));
        assertInvalid(KeyExpression.InvalidExpressionException.class, "Descriptor MySimpleRecord does not have field: no_such_field", metaData);
    }

    @Test
    public void badPrimaryKeyType() {
        RecordMetaDataBuilder metaData = RecordMetaData.newBuilder().setRecords(TestRecords4Proto.getDescriptor());
        metaData.getRecordType("RestaurantReviewer").setPrimaryKey(Key.Expressions.field("stats"));
        assertInvalid(Query.InvalidExpressionException.class, "stats is a nested message, but accessed as a scalar", metaData);
    }

    @Test
    public void badIndexField() {
        RecordMetaDataBuilder metaData = RecordMetaData.newBuilder().setRecords(TestRecords1Proto.getDescriptor());
        metaData.addIndex("MySimpleRecord", "no_such_field");
        assertInvalid(KeyExpression.InvalidExpressionException.class, "Descriptor MySimpleRecord does not have field: no_such_field", metaData);
    }

    @Test
    public void badIndexType() {
        RecordMetaDataBuilder metaData = RecordMetaData.newBuilder().setRecords(TestRecords4Proto.getDescriptor());
        metaData.addIndex("RestaurantReviewer", "stats");
        assertInvalid(Query.InvalidExpressionException.class, "stats is a nested message, but accessed as a scalar", metaData);
    }

    @Test
    public void badIndexAddedVersion() {
        RecordMetaDataBuilder metaData = RecordMetaData.newBuilder().setRecords(TestRecords1Proto.getDescriptor());
        metaData.getIndex("MySimpleRecord$str_value_indexed").setAddedVersion(metaData.getVersion() + 1);
        metaData.getIndex("MySimpleRecord$str_value_indexed").setLastModifiedVersion(metaData.getVersion() + 1);
        assertInvalid("Index MySimpleRecord$str_value_indexed has added version " +
                      (metaData.getVersion() + 1) + " which is greater than the meta-data version " + metaData.getVersion(),
                metaData);
    }

    @Test
    public void badIndexLastModifiedVersion() {
        RecordMetaDataBuilder metaData = RecordMetaData.newBuilder().setRecords(TestRecords1Proto.getDescriptor());
        metaData.getIndex("MySimpleRecord$str_value_indexed").setLastModifiedVersion(metaData.getVersion() + 1);
        assertInvalid("Index MySimpleRecord$str_value_indexed has last modified version " +
                      (metaData.getVersion() + 1) + " which is greater than the meta-data version " + metaData.getVersion(),
                metaData);
    }

    @Test
    public void indexAddedAfterLastModifiedVersion() {
        RecordMetaDataBuilder metaData = RecordMetaData.newBuilder().setRecords(TestRecords1Proto.getDescriptor());
        metaData.getIndex("MySimpleRecord$str_value_indexed").setAddedVersion(metaData.getVersion());
        metaData.getIndex("MySimpleRecord$str_value_indexed").setLastModifiedVersion(metaData.getVersion() - 1);
        assertInvalid("Index MySimpleRecord$str_value_indexed has added version " +
                      metaData.getVersion() + " which is greater than the last modified version " + (metaData.getVersion() - 1),
                metaData);
    }

    @Test
    public void badFormerIndexAddedVersion() {
        RecordMetaDataBuilder metaData = RecordMetaData.newBuilder().setRecords(
                RecordMetaData.build(TestRecords1Proto.getDescriptor()).toProto().toBuilder()
                        .addFormerIndexes(RecordMetaDataProto.FormerIndex.newBuilder()
                                .setSubspaceKey(ByteString.copyFrom(Tuple.from("dropped_index").pack()))
                                .setAddedVersion(10)
                                .setRemovedVersion(10))
                        .build());
        assertInvalid("Former index has added version 10 which is greater than the meta-data version " + metaData.getVersion(), metaData);

        RecordMetaDataProto.MetaData.Builder protoBuilder = metaData.build(false).toProto().toBuilder();
        protoBuilder.getFormerIndexesBuilder(0).setFormerName("dropped_index");
        metaData = RecordMetaData.newBuilder().setRecords(protoBuilder.build());
        assertInvalid("Former index dropped_index has added version 10 which is greater than the meta-data version " + metaData.getVersion(), metaData);
    }

    @Test
    public void badFormerIndexRemovedVersion() {
        RecordMetaDataBuilder metaData = RecordMetaData.newBuilder().setRecords(
                RecordMetaData.build(TestRecords1Proto.getDescriptor()).toProto().toBuilder()
                        .addFormerIndexes(RecordMetaDataProto.FormerIndex.newBuilder()
                                .setSubspaceKey(ByteString.copyFrom(Tuple.from("dropped_index").pack()))
                                .setRemovedVersion(10))
                        .build());
        assertInvalid("Former index has removed version 10 which is greater than the meta-data version " + metaData.getVersion(), metaData);

        RecordMetaDataProto.MetaData.Builder protoBuilder = metaData.build(false).toProto().toBuilder();
        protoBuilder.getFormerIndexesBuilder(0).setFormerName("dropped_index");
        metaData = RecordMetaData.newBuilder().setRecords(protoBuilder.build());
        assertInvalid("Former index dropped_index has removed version 10 which is greater than the meta-data version " + metaData.getVersion(), metaData);
    }

    @Test
    public void badFormerRemovedBeforeAdded() {
        RecordMetaDataBuilder metaData = RecordMetaData.newBuilder().setRecords(
                RecordMetaData.build(TestRecords1Proto.getDescriptor()).toProto().toBuilder()
                        .addFormerIndexes(RecordMetaDataProto.FormerIndex.newBuilder()
                                .setSubspaceKey(ByteString.copyFrom(Tuple.from("dropped_index").pack()))
                                .setAddedVersion(2)
                                .setRemovedVersion(1))
                        .build());
        assertInvalid("Former index has added version 2 which is greater than the removed version 1",  metaData);

        RecordMetaDataProto.MetaData.Builder protoBuilder = metaData.build(false).toProto().toBuilder();
        protoBuilder.getFormerIndexesBuilder(0).setFormerName("dropped_index");
        metaData = RecordMetaData.newBuilder().setRecords(protoBuilder.build());
        assertInvalid("Former index dropped_index has added version 2 which is greater than the removed version 1", metaData);
    }

    @Test
    public void badSinceVersion() {
        RecordMetaDataBuilder metaData = RecordMetaData.newBuilder().setRecords(TestRecords1Proto.getDescriptor());
        metaData.getRecordType("MySimpleRecord").setSinceVersion(metaData.getVersion() + 1);
        assertInvalid("Record type MySimpleRecord has since version of " + (metaData.getVersion() + 1) +
                      " which is greater than the meta-data version " + metaData.getVersion(),
                metaData);
    }
}
