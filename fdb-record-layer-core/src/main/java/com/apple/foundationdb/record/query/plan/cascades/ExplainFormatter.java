/*
 * ExplainFormatter.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
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

import javax.annotation.Nonnull;

/**
 * A formatter for tokens.
 */
public class ExplainFormatter {
    private static final ExplainFormatter FOR_DEBUGGING = new ExplainFormatter(new DefaultExplainSymbolMap());

    @Nonnull
    private final ExplainSymbolMap symbolMap;

    public ExplainFormatter(@Nonnull final ExplainSymbolMap symbolMap) {
        this.symbolMap = symbolMap;
    }

    @Nonnull
    String getSymbolForAlias(@Nonnull CorrelationIdentifier alias) {
        return symbolMap.getSymbolForAlias(alias);
    }

    @Nonnull
    public static ExplainFormatter forDebugging() {
        return FOR_DEBUGGING;
    }
}
