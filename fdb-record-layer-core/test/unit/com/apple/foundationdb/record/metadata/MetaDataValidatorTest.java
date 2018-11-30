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

import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.TestRecords4Proto;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerRegistryImpl;
import com.apple.foundationdb.record.query.expressions.Query;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests for {@link MetaDataValidator}.
 */
public class MetaDataValidatorTest {
    
    private void validate(RecordMetaDataBuilder metaData) throws Exception {
        final MetaDataValidator validator = new MetaDataValidator(metaData, IndexMaintainerRegistryImpl.instance());
        validator.validate();
    }

    @Test
    public void duplicateRecordTypeKey() {
        RecordMetaDataBuilder metaData = new RecordMetaDataBuilder(TestRecords1Proto.getDescriptor());
        metaData.getRecordType("MySimpleRecord").setPrimaryKey(Key.Expressions.concat(Key.Expressions.recordType(), Key.Expressions.field("rec_no")));
        metaData.getRecordType("MySimpleRecord").setRecordTypeKey("same");
        metaData.getRecordType("MyOtherRecord").setPrimaryKey(Key.Expressions.concat(Key.Expressions.recordType(), Key.Expressions.field("rec_no")));
        metaData.getRecordType("MyOtherRecord").setRecordTypeKey("same");
        assertThrows(MetaDataException.class, () -> validate(metaData));
    }

    @Test
    public void primaryKeyRepeated() {
        RecordMetaDataBuilder metaData = new RecordMetaDataBuilder(TestRecords1Proto.getDescriptor());
        metaData.getRecordType("MySimpleRecord").setPrimaryKey(Key.Expressions.field("repeater", KeyExpression.FanType.FanOut));
        assertThrows(MetaDataException.class, () -> validate(metaData));
    }

    @Test
    public void duplicateSubspaceKey() {
        RecordMetaDataBuilder metaData = new RecordMetaDataBuilder(TestRecords1Proto.getDescriptor());
        metaData.getIndex("MySimpleRecord$str_value_indexed").setSubspaceKey("same");
        metaData.getIndex("MySimpleRecord$num_value_3_indexed").setSubspaceKey("same");
        assertThrows(MetaDataException.class, () -> validate(metaData));
    }

    @Test
    public void badIndexField() {
        RecordMetaDataBuilder metaData = new RecordMetaDataBuilder(TestRecords1Proto.getDescriptor());
        metaData.addIndex("MySimpleRecord", "no_such_field");
        assertThrows(KeyExpression.InvalidExpressionException.class, () -> validate(metaData));
    }

    @Test
    public void badIndexType() {
        RecordMetaDataBuilder metaData = new RecordMetaDataBuilder(TestRecords4Proto.getDescriptor());
        metaData.addIndex("RestaurantReviewer", "stats");
        assertThrows(Query.InvalidExpressionException.class, () -> validate(metaData));
    }

}
