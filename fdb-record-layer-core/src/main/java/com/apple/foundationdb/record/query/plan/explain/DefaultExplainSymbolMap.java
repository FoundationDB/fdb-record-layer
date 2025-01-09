/*
 * DefaultExplainSymbolMap.java
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
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Symbol table that uses the alias' string representation as symbol.
 */
public class DefaultExplainSymbolMap implements ExplainSymbolMap {
    @Override
    public void registerAlias(@Nonnull final CorrelationIdentifier alias) {
        // empty
    }

    @Override
    public void registerAliasWithExplicitSymbol(@Nonnull final CorrelationIdentifier alias, @Nonnull final String symbol) {
        // empty
    }

    @Nullable
    @Override
    public String getSymbolForAlias(@Nonnull final CorrelationIdentifier alias) {
        return Quantifier.current().equals(alias) ? "_" : alias.getId();
    }
}
