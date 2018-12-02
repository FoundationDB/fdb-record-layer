/*
 * FDBRecordStoreEvaluationContext.java
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

package com.apple.foundationdb.record.provider.foundationdb;

import com.apple.foundationdb.record.Bindings;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;

/**
 * An evaluation context associated with a record store.
 * @param <M> the type of record supported by the record store,
 * which determines, for example, the type of records returned by queries using this context
 */
class FDBRecordStoreEvaluationContext<M extends Message> extends FDBEvaluationContext<M> {
    @Nonnull
    protected final FDBRecordStoreBase<M> store;

    public FDBRecordStoreEvaluationContext(@Nonnull FDBRecordStoreBase<M> store, @Nonnull Bindings bindings) {
        super(bindings);
        this.store = store;
    }

    @Override
    public FDBRecordStoreBase<M> getStore() {
        return store;
    }

    @Override
    @Nonnull
    protected FDBEvaluationContext<M> withBindings(@Nonnull Bindings newBindings) {
        return new FDBRecordStoreEvaluationContext<>(store, newBindings);
    }
}
