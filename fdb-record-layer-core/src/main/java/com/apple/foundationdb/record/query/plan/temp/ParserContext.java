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

package com.apple.foundationdb.record.query.plan.temp;

import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordStoreState;
import com.apple.foundationdb.record.query.plan.temp.dynamic.DynamicSchema;
import com.apple.foundationdb.record.query.predicates.Value;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class ParserContext {
    @Nonnull
    private final Scopes scopes;
    @Nonnull
    private DynamicSchema.Builder dynamicSchemaBuilder;

    @Nonnull
    private final RecordMetaData recordMetaData;
    @Nonnull
    private final RecordStoreState recordStoreState;

    public ParserContext(@Nonnull final Scopes scopes,
                         @Nonnull DynamicSchema.Builder dynamicSchemaBuilder,
                         @Nonnull final RecordMetaData recordMetaData,
                         @Nonnull final RecordStoreState recordStoreState) {
        this.scopes = scopes;
        this.dynamicSchemaBuilder = dynamicSchemaBuilder;
        this.recordMetaData = recordMetaData;
        this.recordStoreState = recordStoreState;
    }

    @Nonnull
    public DynamicSchema.Builder getDynamicSchemaBuilder() {
        return dynamicSchemaBuilder;
    }

    @Nonnull
    public RecordMetaData getRecordMetaData() {
        return recordMetaData;
    }

    @Nonnull
    public RecordStoreState getRecordStoreState() {
        return recordStoreState;
    }

    @Nonnull
    public Scopes.Scope getCurrentScope() {
        return Objects.requireNonNull(scopes.getCurrentScope());
    }

    public void pushScope(@Nonnull final Set<CorrelationIdentifier> visibleAliases,
                          @Nonnull final Map<String, Value> boundIdentifiers) {
        scopes.push(visibleAliases, boundIdentifiers);
    }

    @Nonnull
    public Scopes.Scope popScope() {
        return scopes.pop();
    }

    @Nonnull
    public Value resolveIdentifier(@Nonnull final String identifier) {
        return scopes.resolveIdentifier(identifier);
    }
}
