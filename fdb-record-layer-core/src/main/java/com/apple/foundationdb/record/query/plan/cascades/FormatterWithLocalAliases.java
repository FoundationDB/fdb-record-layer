/*
 * Formatter.java
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

import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Maps;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Formatting for {@link Value#explain}.
 */
public class FormatterWithLocalAliases implements Formatter {
    @Nonnull private final AtomicInteger quantifierNumber;
    @Nonnull private final BiMap<CorrelationIdentifier, String> aliasToFormattingNameMap;
    @Nonnull private final Map<CorrelationIdentifier, Quantifier> aliasToQuantifierMap;

    public FormatterWithLocalAliases() {
        this.quantifierNumber = new AtomicInteger(0);
        this.aliasToFormattingNameMap = HashBiMap.create();
        this.aliasToQuantifierMap = Maps.newHashMap();
    }

    @Override
    public void registerForFormatting(@Nonnull final Quantifier quantifier) {
        aliasToFormattingNameMap.put(quantifier.getAlias(), "q" + quantifierNumber.getAndIncrement());
        aliasToQuantifierMap.put(quantifier.getAlias(), quantifier);
    }

    @Override
    public void registerForFormatting(@Nonnull final CorrelationIdentifier alias) {
        aliasToFormattingNameMap.putIfAbsent(alias, "q" + quantifierNumber.getAndIncrement());
    }

    @SpotBugsSuppressWarnings(value = "NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE",
            justification = "If we get a NP from the map because the key does not exist, a NPE" +
                    " will be thrown because of Objects.requireNonNull")
    @Nonnull
    @Override
    public String getQuantifierName(@Nonnull final CorrelationIdentifier alias) {
        return Objects.requireNonNull(aliasToFormattingNameMap.get(alias));
    }

    @Nonnull
    @Override
    public Quantifier getQuantifier(@Nonnull final CorrelationIdentifier alias) {
        return Objects.requireNonNull(aliasToQuantifierMap.get(alias));
    }
}
