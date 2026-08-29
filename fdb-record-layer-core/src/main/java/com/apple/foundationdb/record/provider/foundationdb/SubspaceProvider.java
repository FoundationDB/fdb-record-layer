/*
 * SubspaceProvider.java
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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.subspace.Subspace;
import java.util.concurrent.CompletableFuture;

/**
 * Subspace provider can provide a subspace (might be blocking) and logging information to the subspace (non-blocking).
 */
@API(API.Status.INTERNAL)
public interface SubspaceProvider {
    /**
     * This might be blocking if the subspace is never fetched before.
     *
     * @param context record context used to resolve the subspace
     *
     * @return Subspace
     */
    Subspace getSubspace(FDBRecordContext context);

    /**
     * Asynchronously resolves the subspace against the database associated with {@link FDBRecordContext}.
     *
     * @param context record context used to resolve the subspace
     *
     * @return CompletableFuture&lt;Subspace&gt;
     */
    CompletableFuture<Subspace> getSubspaceAsync(FDBRecordContext context);

    LogMessageKeys logKey();

    /**
     * This method is typically called in support of error logging; hence, implementations should not assume
     * a working {@link FDBRecordContext} but might, for example, use it to retrieve a subspace previously
     * resolved against the corresponding database.
     *
     * @param context record context used to resolve the subspace
     *
     * @return CompletableFuture&lt;Subspace&gt;
     */
    String toString(FDBRecordContext context);

    @Override
    String toString();
}
