/*
 * EmptyKeyExpression.java
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
import com.apple.foundationdb.record.query.plan.temp.ExpansionVisitor;
import com.apple.foundationdb.record.query.plan.temp.GraphExpansion;
import com.apple.foundationdb.record.query.plan.temp.KeyExpressionVisitor;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;

/**
 * A single empty key.
 */
@API(API.Status.MAINTAINED)
@SuppressWarnings("PMD.MissingSerialVersionUID") // this appears to be a false positive
public class EmptyKeyExpression extends BaseKeyExpression implements KeyExpression, KeyExpressionWithoutChildren {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Empty-Key-Expression");

    public static final EmptyKeyExpression EMPTY = new EmptyKeyExpression();
    public static final RecordMetaDataProto.KeyExpression EMPTY_PROTO =
            RecordMetaDataProto.KeyExpression.newBuilder().setEmpty(EMPTY.toProto()).build();

    private EmptyKeyExpression() {
        // nothing to initialize
    }

    @Nonnull
    @Override
    public <M extends Message> List<Key.Evaluated> evaluateMessage(@Nullable FDBRecord<M> record, @Nullable Message message) {
        return Collections.singletonList(Key.Evaluated.EMPTY);
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
        return 0;
    }

    @Nonnull
    @Override
    public RecordMetaDataProto.Empty toProto() throws SerializationException {
        return RecordMetaDataProto.Empty.getDefaultInstance();
    }

    @Nonnull
    @Override
    public <S extends KeyExpressionVisitor.State> GraphExpansion expand(@Nonnull final ExpansionVisitor<S> visitor) {
        return visitor.visitExpression(this);
    }

    @Nonnull
    @Override
    public RecordMetaDataProto.KeyExpression toKeyExpression() {
        return EMPTY_PROTO;
    }

    @Nonnull
    public GroupingKeyExpression groupBy(@Nonnull KeyExpression groupByFirst, @Nonnull KeyExpression... groupByRest) {
        return GroupingKeyExpression.of(this, groupByFirst, groupByRest);
    }

    @Override
    public String toString() {
        return "Empty";
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        return 0;
    }

    @Override
    public int planHash(@Nonnull final PlanHashKind hashKind) {
        switch (hashKind) {
            case LEGACY:
                return 0;
            case FOR_CONTINUATION:
            case STRUCTURAL_WITHOUT_LITERALS:
                return PlanHashable.planHash(hashKind, BASE_HASH);
            default:
                throw new UnsupportedOperationException("Hash kind " + hashKind.name() + " is not supported");
        }
    }
}
