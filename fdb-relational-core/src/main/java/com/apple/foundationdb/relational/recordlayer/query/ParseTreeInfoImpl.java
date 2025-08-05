/*
 * ParseTreeInfoImpl.java
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

import com.apple.foundationdb.relational.api.ParseTreeInfo;
import com.apple.foundationdb.relational.generated.RelationalParser;
import com.apple.foundationdb.relational.generated.RelationalParserBaseVisitor;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;

import javax.annotation.Nonnull;

/**
 * this holds query parsing information.
 */
@API(API.Status.EXPERIMENTAL)
public final class ParseTreeInfoImpl implements ParseTreeInfo {

    @Nonnull
    private final RelationalParser.RootContext rootContext;

    @Nonnull
    private final Supplier<QueryType> queryTypeSupplier;

    private static class QueryTypeVisitor extends RelationalParserBaseVisitor<QueryType> {

        @Override
        public QueryType visitUpdateStatement(RelationalParser.UpdateStatementContext ctx) {
            return QueryType.UPDATE;
        }

        @Override
        public QueryType visitInsertStatement(RelationalParser.InsertStatementContext ctx) {
            return QueryType.INSERT;
        }

        @Override
        public QueryType visitDeleteStatement(RelationalParser.DeleteStatementContext ctx) {
            return QueryType.DELETE;
        }

        @Override
        public QueryType visitQuery(RelationalParser.QueryContext ctx) {
            return QueryType.SELECT;
        }

        @Override
        public QueryType visitCreateDatabaseStatement(RelationalParser.CreateDatabaseStatementContext ctx) {
            return QueryType.CREATE;
        }

        @Override
        public QueryType visitCreateSchemaStatement(RelationalParser.CreateSchemaStatementContext ctx) {
            return QueryType.CREATE;
        }

        @Override
        public QueryType visitCreateSchemaTemplateStatement(RelationalParser.CreateSchemaTemplateStatementContext ctx) {
            return QueryType.CREATE;
        }

        @Override
        protected QueryType aggregateResult(QueryType aggregate, QueryType nextResult) {
            if (nextResult != null) {
                return nextResult;
            }
            return aggregate;
        }
    }

    private ParseTreeInfoImpl(@Nonnull final RelationalParser.RootContext rootContext) {
        this.rootContext = rootContext;
        this.queryTypeSupplier = Suppliers.memoize(this::calculateQueryType);
    }

    @Nonnull
    private QueryType calculateQueryType() {
        final var queryTypeVisitor = new QueryTypeVisitor();
        return queryTypeVisitor.visit(rootContext);
    }

    @Nonnull
    @Override
    public QueryType getQueryType() {
        return queryTypeSupplier.get();
    }

    @Nonnull
    public RelationalParser.RootContext getRootContext() {
        return rootContext;
    }

    @Nonnull
    public static ParseTreeInfoImpl from(@Nonnull final RelationalParser.RootContext root) {
        return new ParseTreeInfoImpl(root);
    }
}
