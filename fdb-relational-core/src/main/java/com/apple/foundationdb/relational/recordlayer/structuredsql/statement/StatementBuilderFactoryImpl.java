/*
 * StatementBuilderFactoryImpl.java
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

package com.apple.foundationdb.relational.recordlayer.structuredsql.statement;

import com.apple.foundationdb.relational.api.ParseTreeInfo;
import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.api.fleuntsql.statement.StatementBuilderFactory;
import com.apple.foundationdb.relational.api.fleuntsql.statement.UpdateStatement;
import com.apple.foundationdb.relational.api.metadata.SchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.query.ParseTreeInfoImpl;
import com.apple.foundationdb.relational.util.Assert;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;

public class StatementBuilderFactoryImpl implements StatementBuilderFactory {

    @Nonnull
    private final SchemaTemplate schemaTemplate;

    @Nonnull
    private final RelationalConnection relationalConnection;

    public StatementBuilderFactoryImpl(@Nonnull SchemaTemplate schemaTemplate, @Nonnull RelationalConnection relationalConnection) {
        this.schemaTemplate = schemaTemplate;
        this.relationalConnection = relationalConnection;
    }

    @Nonnull
    @Override
    public UpdateStatement.Builder updateStatementBuilder() {
        return new UpdateStatementImpl.BuilderImpl(relationalConnection, schemaTemplate);
    }

    @Nonnull
    @Override
    public UpdateStatement.Builder updateStatementBuilder(@Nonnull final String updateQuery) {
        return UpdateStatementImpl.BuilderImpl.fromQuery(relationalConnection, schemaTemplate, updateQuery, Map.of());
    }

    @Nonnull
    @Override
    public UpdateStatement.Builder updateStatementBuilder(@Nonnull String updateQuery, @Nonnull Map<String, List<String>> columnSynonyms) {
        return UpdateStatementImpl.BuilderImpl.fromQuery(relationalConnection, schemaTemplate, updateQuery, columnSynonyms);
    }

    @Nonnull
    @Override
    public UpdateStatement.Builder updateStatementBuilder(@Nonnull final ParseTreeInfo parseTree) {
        Assert.thatUnchecked(parseTree instanceof ParseTreeInfoImpl);
        return UpdateStatementImpl.BuilderImpl.fromParseTreeInfoImpl(relationalConnection, schemaTemplate, (ParseTreeInfoImpl) parseTree, Map.of());
    }

    @Nonnull
    @Override
    public UpdateStatement.Builder updateStatementBuilder(@Nonnull final ParseTreeInfo parseTree,
                                                          @Nonnull final Map<String, List<String>> columnSynonyms) {
        Assert.thatUnchecked(parseTree instanceof ParseTreeInfoImpl);
        return UpdateStatementImpl.BuilderImpl.fromParseTreeInfoImpl(relationalConnection, schemaTemplate, (ParseTreeInfoImpl) parseTree, columnSynonyms);
    }
}
