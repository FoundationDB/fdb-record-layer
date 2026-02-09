/*
 * PreventCommitCheck.java
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

package com.apple.foundationdb.record.provider.foundationdb;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordCoreException;

import javax.annotation.Nonnull;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * Commit check that always fails, effectively preventing this transaction from committing at all.
 */
@API(API.Status.INTERNAL)
public class PreventCommitCheck implements FDBRecordContext.CommitCheckAsync {

    final Supplier<RecordCoreException> exceptionSupplier;

    public PreventCommitCheck(Supplier<RecordCoreException> exceptionSupplier) {
        this.exceptionSupplier = exceptionSupplier;
    }

    @Override
    public boolean isReady() {
        return true;
    }

    @Override
    @Nonnull
    public CompletableFuture<Void> checkAsync() {
        return CompletableFuture.failedFuture(exceptionSupplier.get());
    }
}
