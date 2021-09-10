/*
 * ParserContext.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2021 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.norse;

import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.query.plan.temp.CorrelationIdentifier;
import com.apple.foundationdb.record.query.predicates.LiteralValue;
import com.apple.foundationdb.record.query.predicates.Type;
import com.apple.foundationdb.record.query.predicates.Value;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Objects;

import static com.apple.foundationdb.record.query.predicates.Type.primitiveType;

public class ParserContext {
    private final Deque<Scope> scopes;

    public ParserContext() {
        this.scopes = new ArrayDeque<>();
        scopes.push(new Scope(ImmutableSet.of(CorrelationIdentifier.of("constants")), ImmutableMap.of("x", new LiteralValue<>(primitiveType(Type.TypeCode.INT), 3))));
    }

    @Nonnull
    public Value resolveIdentifier(@Nonnull final String identifier) {
        return scopes.stream()
                .filter(scope -> scope.getBoundIdentifiers().containsKey(identifier))
                .map(scope -> Objects.requireNonNull(scope.getBoundIdentifiers().get(identifier)))
                .findFirst()
                .orElseThrow(() -> new RecordCoreArgumentException("unresolved identifier"));
    }
}
