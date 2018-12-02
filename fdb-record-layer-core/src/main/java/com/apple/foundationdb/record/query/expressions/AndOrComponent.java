/*
 * AndOrComponent.java
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

package com.apple.foundationdb.record.query.expressions;

import com.apple.foundationdb.API;
import com.apple.foundationdb.record.provider.foundationdb.FDBEvaluationContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.apple.foundationdb.record.query.plan.temp.ExpressionRef;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * The common base class for Boolean {@code And} and {@code Or} query components.
 */
@API(API.Status.INTERNAL)
public abstract class AndOrComponent extends SimpleComponentWithChildren implements ComponentWithChildren {

    protected abstract boolean isOr();

    public AndOrComponent(@Nonnull List<ExpressionRef<QueryComponent>> operands) {
        super(operands);
    }

    @Nullable
    private Boolean evalInternal(@Nonnull Function<QueryComponent, Boolean> evalChildFunction) {
        Boolean retVal = !isOr();
        for (QueryComponent child : getChildren()) {
            final Boolean val = evalChildFunction.apply(child);
            if (val == null) {
                retVal = null;
            } else if (val) {
                if (isOr()) {
                    return true;
                }
            } else {
                if (!isOr()) {
                    return false;
                }
            }
        }
        return retVal;
    }

    @Nullable
    @Override
    public <C extends Message, M extends C> Boolean evalMessage(@Nonnull FDBEvaluationContext<C> context, @Nullable FDBRecord<M> record, @Nullable Message message) {
        return evalInternal(child -> (child.evalMessage(context, record, message)));
    }

    @Nonnull
    @Override
    public <C extends Message, M extends C> CompletableFuture<Boolean> evalMessageAsync(@Nonnull FDBEvaluationContext<C> context, @Nullable FDBRecord<M> record, @Nullable Message message) {
        return new AsyncBoolean<>(isOr(), getChildren(), context, record, message).eval();
    }

    @Override
    public boolean isAsync() {
        return getChildren().stream().anyMatch(QueryComponent::isAsync);
    }
}
