/*
 * StoredQuery.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2026 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.api.metadata;

import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import java.util.List;

/**
 * A SELECT query persisted on a {@link SchemaTemplate}, paired with the
 * {@code CREATE [OR REPLACE]? TEMPORARY FUNCTION ...} declarations that must be installed
 * before the SELECT is planned.
 *
 * <p>The SELECT body and each temp-function declaration are kept as their original verbatim source.</p>
 */
public final class StoredQuery {
    @Nonnull
    private final String query;
    @Nonnull
    private final List<String> tempFunctions;

    public StoredQuery(@Nonnull final String storedQuery, @Nonnull final List<String> tempFunctions) {
        this.query = storedQuery;
        this.tempFunctions = ImmutableList.copyOf(tempFunctions);
    }

    @Nonnull
    public String getQuery() {
        return query;
    }

    @Nonnull
    public List<String> getTempFunctions() {
        return tempFunctions;
    }
}
