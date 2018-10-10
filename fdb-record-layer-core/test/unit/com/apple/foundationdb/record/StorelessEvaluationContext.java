/*
 * StorelessEvaluationContext.java
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

package com.apple.foundationdb.record;

import com.apple.foundationdb.record.provider.foundationdb.FDBEvaluationContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;

/**
 * An {@link FDBEvaluationContext} that doesn't actually link to any record store.
 *
 * For evaluation unit tests.
 */
public class StorelessEvaluationContext extends FDBEvaluationContext<Message> {
    public static final StorelessEvaluationContext EMPTY = new StorelessEvaluationContext(Bindings.EMPTY_BINDINGS);

    public StorelessEvaluationContext(@Nonnull Bindings bindings) {
        super(bindings);
    }

    @Override
    public FDBRecordStoreBase<Message> getStore() {
        throw new UnsupportedOperationException("This context does not have a record store");
    }

    @Override
    @Nonnull
    protected FDBEvaluationContext<Message> withBindings(@Nonnull Bindings newBindings) {
        return new StorelessEvaluationContext(newBindings);
    }
}
