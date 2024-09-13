/*
 * LogicalOperatorCatalog.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.recordlayer.query;

import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class LogicalOperatorCatalog {

    @Nonnull
    private final Map<CatalogKey, LogicalOperator> logicalOperators;

    private LogicalOperatorCatalog() {
        this.logicalOperators = new LinkedHashMap<>();
    }

    @Nonnull
    public LogicalOperator lookup(@Nonnull CatalogKey key, @Nonnull Function<CatalogKey, LogicalOperator> mappingFunction) {
        return logicalOperators.computeIfAbsent(key, mappingFunction);
    }

    @Nonnull
    public LogicalOperator lookupTableAccess(@Nonnull Identifier tableId, @Nonnull SemanticAnalyzer semanticAnalyzer) {
        return lookupTableAccess(tableId, Optional.empty(), ImmutableSet.of(), semanticAnalyzer);
    }

    @Nonnull
    public LogicalOperator lookupTableAccess(@Nonnull Identifier tableId,
                                             @Nonnull Optional<Identifier> alias,
                                             @Nonnull Set<String> requestedIndexes,
                                             @Nonnull SemanticAnalyzer semanticAnalyzer) {
        return lookupTableAccess(CatalogKey.of(tableId, requestedIndexes), alias, semanticAnalyzer);
    }

    @Nonnull
    public LogicalOperator lookupTableAccess(@Nonnull CatalogKey key,
                                             @Nonnull Optional<Identifier> alias,
                                             @Nonnull SemanticAnalyzer semanticAnalyzer) {
        if (!logicalOperators.containsKey(key)) {
            final var value = LogicalOperator.generateTableAccess(key.getIdentifier(), key.getHints(), semanticAnalyzer);
            logicalOperators.put(key, value);
            return alias.map(value::withName).orElse(value);
        }
        return logicalOperators.get(key).withNewSharedReferenceAndAlias(alias);
    }

    @Nonnull
    public static LogicalOperatorCatalog newInstance() {
        return new LogicalOperatorCatalog();
    }
}
