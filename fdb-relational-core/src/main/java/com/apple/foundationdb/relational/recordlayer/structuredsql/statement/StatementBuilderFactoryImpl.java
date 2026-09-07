/*
 * StatementBuilderFactoryImpl.java
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

package com.apple.foundationdb.relational.recordlayer.structuredsql.statement;

import com.apple.foundationdb.annotation.API;

import com.apple.foundationdb.relational.api.ParseTreeInfo;
import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.api.fluentsql.statement.StatementBuilderFactory;
import com.apple.foundationdb.relational.api.fluentsql.statement.UpdateStatement;
import com.apple.foundationdb.relational.api.metadata.SchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.query.ParseTreeInfoImpl;
import com.apple.foundationdb.relational.util.Assert;

import java.util.List;
import java.util.Map;

@API(API.Status.EXPERIMENTAL)
public class StatementBuilderFactoryImpl implements StatementBuilderFactory {

    private final SchemaTemplate schemaTemplate;

    private final RelationalConnection relationalConnection;

    public StatementBuilderFactoryImpl(SchemaTemplate schemaTemplate, RelationalConnection relationalConnection) {
        this.schemaTemplate = schemaTemplate;
        this.relationalConnection = relationalConnection;
    }

    @Override
    public UpdateStatement.Builder updateStatementBuilder() {
        return new UpdateStatementImpl.BuilderImpl(relationalConnection, schemaTemplate);
    }

    @Override
    public UpdateStatement.Builder updateStatementBuilder(final String updateQuery) {
        return UpdateStatementImpl.BuilderImpl.fromQuery(relationalConnection, schemaTemplate, updateQuery, Map.of());
    }

    @Override
    public UpdateStatement.Builder updateStatementBuilder(String updateQuery, Map<String, List<String>> columnSynonyms) {
        return UpdateStatementImpl.BuilderImpl.fromQuery(relationalConnection, schemaTemplate, updateQuery, columnSynonyms);
    }

    @Override
    public UpdateStatement.Builder updateStatementBuilder(final ParseTreeInfo parseTree) {
        Assert.thatUnchecked(parseTree instanceof ParseTreeInfoImpl);
        return UpdateStatementImpl.BuilderImpl.fromParseTreeInfoImpl(relationalConnection, schemaTemplate, (ParseTreeInfoImpl) parseTree, Map.of());
    }

    @Override
    public UpdateStatement.Builder updateStatementBuilder(final ParseTreeInfo parseTree,
                                                          final Map<String, List<String>> columnSynonyms) {
        Assert.thatUnchecked(parseTree instanceof ParseTreeInfoImpl);
        return UpdateStatementImpl.BuilderImpl.fromParseTreeInfoImpl(relationalConnection, schemaTemplate, (ParseTreeInfoImpl) parseTree, columnSynonyms);
    }
}
