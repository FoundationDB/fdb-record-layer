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
import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.apple.foundationdb.record.query.plan.temp.ComparisonRange;
import com.apple.foundationdb.record.query.plan.temp.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.temp.ExpandedPredicates;
import com.apple.foundationdb.record.query.plan.temp.view.Source;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

/**
 * A single empty key.
 */
@API(API.Status.MAINTAINED)
@SuppressWarnings("PMD.MissingSerialVersionUID") // this appears to be a false positive
public class EmptyKeyExpression extends BaseKeyExpression implements KeyExpression, KeyExpressionWithoutChildren {
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
    public KeyExpression normalizeForPlannerOld(@Nonnull Source source, @Nonnull List<String> fieldNamePrefix) {
        return this;
    }

    @Nonnull
    @Override
    public ExpandedPredicates normalizeForPlanner(@Nonnull final CorrelationIdentifier baseAlias,
                                                  @Nonnull final Supplier<ComparisonRange.Type> typeSupplier,
                                                  @Nonnull final List<String> fieldNamePrefix) {
        return ExpandedPredicates.empty();
    }

    @Nonnull
    @Override
    public RecordMetaDataProto.KeyExpression toKeyExpression() {
        return EMPTY_PROTO;
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
    public int planHash() {
        return 0;
    }
}
