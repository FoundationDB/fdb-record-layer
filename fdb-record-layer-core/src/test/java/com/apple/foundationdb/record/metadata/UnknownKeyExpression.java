/*
 * UnknownKeyExpression.java
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

import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.expressions.RecordKeyExpressionProto;
import com.apple.foundationdb.record.metadata.expressions.BaseKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.apple.foundationdb.record.query.plan.cascades.KeyExpressionVisitor;
import com.apple.foundationdb.record.util.HashUtils;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;

/**
 * A dummy class for use within unit tests to check paths within the code base that are supposed
 * to throw an error if they counter a {@link KeyExpression} that they don't know about.
 */
public class UnknownKeyExpression extends BaseKeyExpression {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Unknown-Key-Expression");


    /**
     * Get the instance of this singleton. If this were a real class for use by end clients, this
     * would probably be a method on the {@link Key.Expressions} class as well.
     */
    public static final UnknownKeyExpression UNKNOWN = new UnknownKeyExpression();

    private UnknownKeyExpression() {
    }

    @Nonnull
    @Override
    public <M extends Message> List<Key.Evaluated> evaluateMessage(@Nullable FDBRecord<M> record, @Nullable Message message) {
        return Collections.singletonList(Key.Evaluated.scalar("unknown!!!"));
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

    @Nonnull
    @Override
    public <S extends KeyExpressionVisitor.State, R> R expand(@Nonnull final KeyExpressionVisitor<S, R> visitor) {
        throw new UnsupportedOperationException();
    }

    @Nonnull
    @Override
    public Message toProto() throws SerializationException {
        throw new UnsupportedOperationException("UnknownKeyExpressions cannot be converted to Protobuf");
    }

    @Nonnull
    @Override
    public RecordKeyExpressionProto.KeyExpression toKeyExpression() {
        throw new UnsupportedOperationException("UnknownKeyExpressions cannot be converted to Protobuf");
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        return 1066;
    }

    @Override
    public int queryHash(@Nonnull final QueryHashKind hashKind) {
        return HashUtils.queryHash(hashKind, BASE_HASH);
    }

    @Override
    public String toString() {
        return "UNKNOWN";
    }
}
