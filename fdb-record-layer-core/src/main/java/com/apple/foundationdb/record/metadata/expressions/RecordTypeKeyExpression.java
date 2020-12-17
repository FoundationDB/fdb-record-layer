/*
 * RecordTypeKeyExpression.java
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

package com.apple.foundationdb.record.metadata.expressions;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.apple.foundationdb.record.query.plan.temp.view.RecordTypeElement;
import com.apple.foundationdb.record.query.plan.temp.view.RecordTypeSource;
import com.apple.foundationdb.record.query.plan.temp.view.RepeatedFieldSource;
import com.apple.foundationdb.record.query.plan.temp.view.Source;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;

/**
 * A key expression that indicates that a unique record type identifier should
 * be contained within the key. The unique value can be specified explicitly or generated automatically
 * from the corresponding field numbers in the union message descriptor.
 *
 * It is important that the unique identifiers are stable. A record type's identifier should never change.
 * If it is automatically generated, that means that fields should never be removed / reused in the union
 * message descriptor, but at most deprecated. In that way, the lowest numbered field for a given type
 * will always be the same.
 *
 * If the record type key appears at the start of every primary key, the record extent is divided by type,
 * as in other database systems.
 * @see com.apple.foundationdb.record.metadata.RecordType#getExplicitRecordTypeKey
 * @see com.apple.foundationdb.record.RecordMetaData#primaryKeyHasRecordTypePrefix
 */
@API(API.Status.MAINTAINED)
public class RecordTypeKeyExpression extends BaseKeyExpression implements AtomKeyExpression, KeyExpressionWithoutChildren {
    public static final RecordTypeKeyExpression RECORD_TYPE_KEY = new RecordTypeKeyExpression();
    public static final RecordMetaDataProto.KeyExpression RECORD_TYPE_KEY_PROTO =
            RecordMetaDataProto.KeyExpression.newBuilder().setRecordTypeKey(RECORD_TYPE_KEY.toProto()).build();

    private static final GroupingKeyExpression UNGROUPED = new GroupingKeyExpression(new RecordTypeKeyExpression(), 0);

    private RecordTypeKeyExpression() {
        // nothing to initialize
    }

    @Nonnull
    @Override
    public <M extends Message> List<Key.Evaluated> evaluateMessage(@Nullable FDBRecord<M> record, @Nullable Message message) {
        final Key.Evaluated recordType = record != null ? Key.Evaluated.scalar(record.getRecordType().getRecordTypeKey()) : Key.Evaluated.NULL;
        return Collections.singletonList(recordType);
    }

    @Override
    public boolean createsDuplicates() {
        return false;
    }

    @Override
    public List<Descriptors.FieldDescriptor> validate(@Nonnull Descriptors.Descriptor descriptor) {
        return Collections.emptyList();
    }

    @Override
    public int getColumnSize() {
        return 1;
    }

    @Override
    public boolean hasRecordTypeKey() {
        return true;
    }

    /**
     * A <code>RecordType</code> expression with no grouping keys (mostly for evaluating record functions).
     * @return a {@link GroupingKeyExpression} with no grouping keys
     */
    @Nonnull
    public GroupingKeyExpression ungrouped() {
        return UNGROUPED;
    }

    @Nonnull
    @Override
    public RecordMetaDataProto.RecordTypeKey toProto() throws SerializationException {
        return RecordMetaDataProto.RecordTypeKey.getDefaultInstance();
    }

    @Nonnull
    @Override
    public RecordMetaDataProto.KeyExpression toKeyExpression() {
        return RECORD_TYPE_KEY_PROTO;
    }

    @Nonnull
    @Override
    public KeyExpression normalizeForPlanner(@Nonnull Source source, @Nonnull List<String> fieldNamePrefix) {
        // We need the actual record type, rather than the type of the current message.
        // Walk through all of the RepeatedFieldSources until we find a RecordTypeSource.
        // This *will* break when we add more source types. :(
        Source recordTypeSource = source;
        while (recordTypeSource instanceof RepeatedFieldSource) {
            recordTypeSource = ((RepeatedFieldSource)recordTypeSource).getParent();
        }
        if (!(recordTypeSource instanceof RecordTypeSource)) {
            throw new RecordCoreException("Could not find RecordTypeSource for RecordTypeKeyExpression")
                    .addLogInfo("foundSource", recordTypeSource.getClass());
        }

        return new ElementKeyExpression(new RecordTypeElement(recordTypeSource));
    }

    @Override
    public boolean equals(Object o) {
        return o == this || !(o == null || getClass() != o.getClass());
    }

    @Override
    public int hashCode() {
        return 2;
    }

    @Override
    public int planHash(@Nonnull final PlanHashKind hashKind) {
        return 2;
    }

    @Override
    public boolean equalsAtomic(AtomKeyExpression other) {
        return equals(other);
    }

    @Override
    public String toString() {
        return "RecordTypeKey";
    }
}
