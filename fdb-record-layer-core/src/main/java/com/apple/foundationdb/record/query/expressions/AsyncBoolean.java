/*
 * AsyncBoolean.java
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

import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.record.provider.foundationdb.FDBEvaluationContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.apple.foundationdb.async.AsyncUtil.READY_FALSE;

class AsyncBoolean<C extends Message, M extends C> {
    private final boolean isOr;
    @Nonnull
    private final Iterator<QueryComponent> operands;
    @Nonnull
    private final FDBEvaluationContext<C> context;
    @Nullable
    private final FDBRecord<M> record;
    @Nullable
    private final Message message;
    @Nullable
    private Boolean retVal;

    public AsyncBoolean(boolean isOr, @Nonnull List<QueryComponent> operands, @Nonnull FDBEvaluationContext<C> context, @Nullable FDBRecord<M> record, @Nullable Message message) {
        this.isOr = isOr;
        this.operands = operands.iterator();
        this.context = context;
        this.record = record;
        this.message = message;
        this.retVal = !isOr;
    }

    public CompletableFuture<Boolean> eval() {
        return AsyncUtil.whileTrue(() -> {
            if (!operands.hasNext()) {
                return READY_FALSE;
            } else {
                return operands.next().evalMessageAsync(context, record, message)
                    .thenApply(val -> {
                        if (val == null) {
                            retVal = null;
                        } else if (val) {
                            if (isOr) {
                                retVal = true;
                                return false;
                            }
                        } else {
                            if (!isOr) {
                                retVal = false;
                                return true;
                            }
                        }
                        return true;
                    });
            }
        }, context.getExecutor()).thenApply(vignore -> retVal);
    }
}
