/*
 * LogicalOperatorCatalog.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2025 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.annotation.API;

import com.google.common.collect.ImmutableSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
@API(API.Status.EXPERIMENTAL)
public final class LogicalOperatorCatalog {

    private final Map<CatalogKey, LogicalOperator> logicalOperators;

    private LogicalOperatorCatalog() {
        this.logicalOperators = new LinkedHashMap<>();
    }

    public LogicalOperator lookup(CatalogKey key, Function<CatalogKey, LogicalOperator> mappingFunction) {
        return logicalOperators.computeIfAbsent(key, mappingFunction);
    }

    public LogicalOperator lookupTableAccess(Identifier tableId, SemanticAnalyzer semanticAnalyzer) {
        return lookupTableAccess(tableId, Optional.empty(), ImmutableSet.of(), semanticAnalyzer);
    }

    public LogicalOperator lookupTableAccess(Identifier tableId,
                                             Optional<Identifier> alias,
                                             Set<String> requestedIndexes,
                                             SemanticAnalyzer semanticAnalyzer) {
        return lookupTableAccess(CatalogKey.of(tableId, requestedIndexes), alias, semanticAnalyzer);
    }

    public LogicalOperator lookupTableAccess(CatalogKey key,
                                             Optional<Identifier> alias,
                                             SemanticAnalyzer semanticAnalyzer) {
        if (!logicalOperators.containsKey(key)) {
            final var value = LogicalOperator.generateTableAccess(key.getIdentifier(), key.getHints(), semanticAnalyzer);
            logicalOperators.put(key, value);
            return alias.map(value::withName).orElse(value);
        }
        return logicalOperators.get(key).withNewSharedReferenceAndAlias(alias);
    }

    public static LogicalOperatorCatalog newInstance() {
        return new LogicalOperatorCatalog();
    }
}
