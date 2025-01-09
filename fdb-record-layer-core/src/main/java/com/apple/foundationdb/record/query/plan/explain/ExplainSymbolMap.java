/*
 * ExplainSymbolMap.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.explain;

import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Interface for simple symbol maps for the purpose of mapping an alias as supplied by the planner to a human-readable
 * string symbol.
 */
public interface ExplainSymbolMap {
    void registerAlias(@Nonnull CorrelationIdentifier alias);

    void registerAliasWithExplicitSymbol(@Nonnull CorrelationIdentifier alias, @Nonnull String symbol);

    @Nullable
    String getSymbolForAlias(@Nonnull CorrelationIdentifier alias);
}
