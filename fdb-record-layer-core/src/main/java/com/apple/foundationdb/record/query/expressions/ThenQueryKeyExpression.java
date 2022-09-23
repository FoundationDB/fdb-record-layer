/*
 * ThenQueryKeyExpression.java
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

package com.apple.foundationdb.record.query.expressions;

import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.QueryableKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.ThenKeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;

/**
 * A simple {@link QueryableKeyExpression} that covers a {@link ThenKeyExpression}, translating
 * into a Tuple for comparison operations with {@link QueryKeyExpressionWithComparison}.
 */
public class ThenQueryKeyExpression implements QueryableKeyExpression {
    private final ThenKeyExpression thenKey;

    public ThenQueryKeyExpression(final ThenKeyExpression thenKey) {
        this.thenKey = thenKey;
    }

    @Override
    public int planHash(@Nonnull final PlanHashKind hashKind) {
        return thenKey.planHash(hashKind);
    }

    @Override
    public int queryHash(@Nonnull final QueryHashKind hashKind) {
        return thenKey.queryHash(hashKind);
    }

    @Nonnull
    @Override
    public <M extends Message> List<Key.Evaluated> evaluateMessage(@Nullable final FDBRecord<M> record, @Nullable final Message message) {
        return thenKey.evaluateMessage(record, message);
    }

    @Override
    public boolean createsDuplicates() {
        return thenKey.createsDuplicates();
    }

    @Override
    public List<Descriptors.FieldDescriptor> validate(@Nonnull final Descriptors.Descriptor descriptor) {
        return thenKey.validate(descriptor);
    }

    @Override
    public int getColumnSize() {
        return thenKey.getColumnSize();
    }

    @Nonnull
    @Override
    public Message toProto() throws SerializationException {
        return thenKey.toProto();
    }

    @Nonnull
    @Override
    public RecordMetaDataProto.KeyExpression toKeyExpression() {
        return thenKey.toKeyExpression();
    }

    @Nonnull
    @Override
    public KeyExpression getSubKey(final int start, final int end) {
        return thenKey.getSubKey(start, end);
    }

    @Override
    public boolean isPrefixKey(@Nonnull final KeyExpression key) {
        return thenKey.isPrefixKey(key);
    }

    @Nonnull
    @Override
    public Value toValue(@Nonnull final CorrelationIdentifier baseAlias, @Nonnull final List<String> fieldNamePrefix) {
        throw new UnsupportedOperationException();
    }

    @Nonnull
    @Override
    public String getName() {
        return "tuple";
    }
}
