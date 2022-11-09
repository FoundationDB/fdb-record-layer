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

package com.apple.foundationdb.relational.recordlayer.query;

import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.typing.TypeRepository;

import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Context for query parsing, including meta-data, generated types, and the state of available indexes.
 */
public class ParserContext {
    @Nonnull
    private final Scopes scopes;

    @Nullable
    private int limit;

    @Nullable
    private int offset;

    @Nonnull
    private final TypeRepository.Builder typeRepositoryBuilder;

    public ParserContext(@Nonnull final Scopes scopes,
                         @Nonnull TypeRepository.Builder typeRepositoryBuilder) {
        this.scopes = scopes;
        this.typeRepositoryBuilder = typeRepositoryBuilder;
        this.limit = ReadTransaction.ROW_LIMIT_UNLIMITED;
        this.offset = 0;
    }

    @Nonnull
    public TypeRepository.Builder getTypeRepositoryBuilder() {
        return typeRepositoryBuilder;
    }

    @Nonnull
    public Scopes.Scope getCurrentScope() {
        return Objects.requireNonNull(scopes.getCurrentScope());
    }

    public Scopes.Scope pushScope() {
        return scopes.push();
    }

    public Scopes.Scope siblingScope() {
        return scopes.sibling();
    }

    @Nonnull
    public Scopes.Scope popScope() {
        return scopes.pop();
    }

    @Nonnull
    public Optional<Quantifier> resolveQuantifier(@Nonnull final String identifier, boolean lookIntoSiblings) {
        return resolveQuantifier(CorrelationIdentifier.of(identifier), lookIntoSiblings);
    }

    @Nonnull
    public Optional<Quantifier> resolveQuantifier(@Nonnull final CorrelationIdentifier identifier, boolean lookIntoSiblings) {
        return scopes.resolveQuantifier(identifier, lookIntoSiblings);
    }

    public void setLimit(@Nonnull Integer limit) {
        this.limit = limit;
    }

    public void setOffset(@Nonnull Integer offset) {
        this.offset = offset;
    }

    public Integer getLimit() {
        return limit;
    }

    public Integer getOffset() {
        return offset;
    }

    @Nonnull
    public Scopes getScopes() {
        return scopes;
    }
}
