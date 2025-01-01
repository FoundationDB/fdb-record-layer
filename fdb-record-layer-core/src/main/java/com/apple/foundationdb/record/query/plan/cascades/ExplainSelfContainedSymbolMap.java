/*
 * ExplainSelfContainedSymbolMap.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2022 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.cascades;

import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Formatting for {@link Value#explain}.
 */
public class ExplainSelfContainedSymbolMap implements ExplainSymbolMap {
    @Nonnull private final AtomicInteger quantifierNumber;
    @Nonnull private final BiMap<CorrelationIdentifier, String> aliasToFormattingNameMap;

    public ExplainSelfContainedSymbolMap() {
        this.quantifierNumber = new AtomicInteger(0);
        this.aliasToFormattingNameMap = HashBiMap.create();
    }

    @Override
    public void registerAlias(@Nonnull final CorrelationIdentifier alias) {
        registerAliasExplicitly(alias, "q" + quantifierNumber.getAndIncrement());
    }

    @Override
    public void registerAliasExplicitly(@Nonnull final CorrelationIdentifier alias, @Nonnull final String symbol) {
        aliasToFormattingNameMap.putIfAbsent(alias, symbol);
    }


    @Nullable
    @Override
    public String getSymbolForAlias(@Nonnull final CorrelationIdentifier alias) {
        return aliasToFormattingNameMap.get(alias);
    }
}
