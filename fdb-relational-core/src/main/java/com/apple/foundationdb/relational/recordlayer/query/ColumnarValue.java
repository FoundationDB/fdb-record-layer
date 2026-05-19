/*
 * ColumnarValue.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.recordlayer.query;

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.planprotos.PValue;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokensWithPrecedence;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Set;
import java.util.function.Supplier;

public class ColumnarValue implements Value {
    @Nonnull
    private final Value inner;
    private final int columnId;

    public ColumnarValue(@Nonnull final Value inner, final int columnId) {
        this.inner = inner;
        this.columnId = columnId;
    }

    public int getColumnId() {
        return columnId;
    }

    @Nonnull
    public Value getInner() {
        return inner;
    }

    @Nonnull
    @Override
    public Type getResultType() {
        return inner.getResultType();
    }

    @Nonnull
    @Override
    public ExplainTokensWithPrecedence explain(@Nonnull final Iterable<Supplier<ExplainTokensWithPrecedence>> explainSuppliers) {
        return inner.explain(explainSuppliers);
    }

    @Override
    public boolean isIndexOnly() {
        return inner.isIndexOnly();
    }

    @Nullable
    @Override
    public <M extends Message> Object eval(@Nullable final FDBRecordStoreBase<M> store,
                                           @Nonnull final EvaluationContext context) {
        return inner.eval(store, context);
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedToWithoutChildren() {
        return inner.getCorrelatedToWithoutChildren();
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedTo() {
        return inner.getCorrelatedTo();
    }

    @Override
    public int semanticHashCode() {
        return inner.semanticHashCode();
    }

    @Override
    public int hashCodeWithoutChildren() {
        return inner.hashCodeWithoutChildren();
    }

    @Nonnull
    @Override
    public Iterable<? extends Value> getChildren() {
        return inner.getChildren();
    }

    @Nonnull
    @Override
    public Value withChildren(@Nonnull final Iterable<? extends Value> newChildren) {
        return new ColumnarValue(inner.withChildren(newChildren), columnId);
    }

    @Override
    public int planHash(@Nonnull final PlanHashable.PlanHashMode hashMode) {
        return inner.planHash(hashMode);
    }

    @Nonnull
    @Override
    public Message toProto(@Nonnull final PlanSerializationContext serializationContext) {
        return inner.toProto(serializationContext);
    }

    @Nonnull
    @Override
    public PValue toValueProto(@Nonnull final PlanSerializationContext serializationContext) {
        return inner.toValueProto(serializationContext);
    }

    @Override
    public boolean semanticEquals(@Nullable final Object other, @Nonnull final AliasMap aliasMap) {
        if (other instanceof ColumnarValue) {
            return inner.semanticEquals(((ColumnarValue) other).inner, aliasMap);
        }
        return inner.semanticEquals(other, aliasMap);
    }
}
