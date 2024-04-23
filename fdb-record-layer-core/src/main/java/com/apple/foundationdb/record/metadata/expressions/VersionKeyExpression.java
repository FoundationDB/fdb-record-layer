/*
 * VersionKeyExpression.java
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
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordVersion;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.VersionValue;
import com.apple.foundationdb.record.util.HashUtils;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;

/**
 * A key expression that indicates that a serialized {@link FDBRecordVersion} should
 * be contained within the key. This should then be used within version indexes to include data
 * sorted by version.
 */
@API(API.Status.MAINTAINED)
public class VersionKeyExpression extends BaseKeyExpression implements AtomKeyExpression, KeyExpressionWithoutChildren, KeyExpressionWithValue {
    public static final VersionKeyExpression VERSION = new VersionKeyExpression();
    public static final RecordMetaDataProto.KeyExpression VERSION_PROTO =
            RecordMetaDataProto.KeyExpression.newBuilder().setVersion(VERSION.toProto()).build();
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Version-Key-Expression");

    private static final GroupingKeyExpression UNGROUPED = new GroupingKeyExpression(new VersionKeyExpression(), 1);

    private VersionKeyExpression() {
        // nothing to initialize
    }

    @Nonnull
    @Override
    public <M extends Message> List<Key.Evaluated> evaluateMessage(@Nullable FDBRecord<M> record, @Nullable Message message) {
        final Key.Evaluated version = record != null && record.hasVersion() ? Key.Evaluated.scalar(record.getVersion()) : Key.Evaluated.NULL;
        return Collections.singletonList(version);
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

    /**
     * A <code>Version</code> expression with no grouping keys (mostly for evaluating record functions).
     * @return a {@link GroupingKeyExpression} with no grouping keys
     */
    @Nonnull
    public GroupingKeyExpression ungrouped() {
        return UNGROUPED;
    }

    @Nonnull
    public GroupingKeyExpression groupBy(@Nonnull KeyExpression groupByFirst, @Nonnull KeyExpression... groupByRest) {
        return GroupingKeyExpression.of(this, groupByFirst, groupByRest);
    }

    @Nonnull
    @Override
    public RecordMetaDataProto.Version toProto() throws SerializationException {
        return RecordMetaDataProto.Version.getDefaultInstance();
    }

    @Nonnull
    @Override
    public RecordMetaDataProto.KeyExpression toKeyExpression() {
        return VERSION_PROTO;
    }

    @Nonnull
    @Override
    public Value toValue(@Nonnull final CorrelationIdentifier baseAlias, @Nonnull final Type baseType, @Nonnull final List<String> fieldNamePrefix) {
        return new VersionValue(baseAlias);
    }

    @Override
    public int versionColumns() {
        return 1;
    }

    @Override
    public boolean equals(Object o) {
        return o == this || !(o == null || getClass() != o.getClass());
    }

    @Override
    public int hashCode() {
        return 1;
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        switch (mode.getKind()) {
            case LEGACY:
                return 1;
            case FOR_CONTINUATION:
                return PlanHashable.objectsPlanHash(mode, BASE_HASH);
            default:
                throw new UnsupportedOperationException("Hash Kind " + mode.name() + " is not supported");
        }
    }

    @Override
    public int queryHash(@Nonnull final QueryHashKind hashKind) {
        return HashUtils.queryHash(hashKind, BASE_HASH);
    }

    @Override
    public boolean equalsAtomic(AtomKeyExpression other) {
        return equals(other);
    }

    @Override
    public String toString() {
        return "Version";
    }
}
