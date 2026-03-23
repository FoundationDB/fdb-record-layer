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

import com.apple.foundationdb.relational.util.Assert;
import org.antlr.v4.runtime.CommonToken;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.RuleNode;
import org.antlr.v4.runtime.tree.TerminalNode;

import javax.annotation.Nonnull;

/**
 * this holds query parsing information.
 */
@API(API.Status.EXPERIMENTAL)
public final class ParseTreeInfoImpl implements ParseTreeInfo {

    @Nonnull
    private final ParseTree rootContext;

    @Nonnull
    private final QueryType queryType;

    private ParseTreeInfoImpl(@Nonnull final ParseTree effectiveContext, @Nonnull final QueryType queryType) {
        this.rootContext = effectiveContext;
        this.queryType = queryType;
    }

    private static class QueryTypeVisitor extends RelationalParserBaseVisitor<ParseTreeInfoImpl> {

        @Nonnull
        private final ParseTree rootContext;

        private QueryTypeVisitor(@Nonnull final ParseTree rootContext) {
            this.rootContext = rootContext;
        }

        @Override
        public ParseTreeInfoImpl visitUpdateStatement(RelationalParser.UpdateStatementContext ctx) {
            return new ParseTreeInfoImpl(rootContext, QueryType.UPDATE);
        }

        @Override
        public ParseTreeInfoImpl visitInsertStatement(RelationalParser.InsertStatementContext ctx) {
            return new ParseTreeInfoImpl(rootContext, QueryType.INSERT);
        }

        @Override
        public ParseTreeInfoImpl visitDeleteStatement(RelationalParser.DeleteStatementContext ctx) {
            return new ParseTreeInfoImpl(rootContext, QueryType.DELETE);
        }

        @Override
        public ParseTreeInfoImpl visitQuery(RelationalParser.QueryContext ctx) {
            return new ParseTreeInfoImpl(rootContext, QueryType.SELECT);
        }

        @Override
        public ParseTreeInfoImpl visitCreateDatabaseStatement(RelationalParser.CreateDatabaseStatementContext ctx) {
            return new ParseTreeInfoImpl(rootContext, QueryType.CREATE);
        }

        @Override
        public ParseTreeInfoImpl visitCreateSchemaStatement(RelationalParser.CreateSchemaStatementContext ctx) {
            return new ParseTreeInfoImpl(rootContext, QueryType.CREATE);
        }

        @Override
        public ParseTreeInfoImpl visitCreateSchemaTemplateStatement(RelationalParser.CreateSchemaTemplateStatementContext ctx) {
            return new ParseTreeInfoImpl(rootContext, QueryType.CREATE);
        }

        @Override
        public ParseTreeInfoImpl visitChildren(final RuleNode node) {
            for (int i = 0; i < node.getChildCount(); i++) {
                final var child = Assert.notNullUnchecked(node.getChild(i));
                final var childResult = child.accept(this);
                if (childResult != null) {
                    return childResult;
                }
            }
            return new ParseTreeInfoImpl(rootContext, QueryType.OTHER);
        }

        private static void remapParseTree(@Nonnull final ParseTree src) {
            final int offset = src instanceof TerminalNode
                    ? ((TerminalNode) src).getSymbol().getTokenIndex()
                    : ((ParserRuleContext) src).start.getTokenIndex();
            shiftTokenIndices(src, offset);
        }

        private static void shiftTokenIndices(@Nonnull final ParseTree tree, final int offset) {
            if (tree instanceof TerminalNode) {
                final CommonToken token = (CommonToken) ((TerminalNode) tree).getSymbol();
                token.setTokenIndex(token.getTokenIndex() - offset);
                return;
            }
            for (int i = 0; i < tree.getChildCount(); i++) {
                shiftTokenIndices(tree.getChild(i), offset);
            }
        }

        @Override
        public ParseTreeInfoImpl visitFullDescribeStatement(final RelationalParser.FullDescribeStatementContext ctx) {
            final var describeObjectClause = ctx.describeObjectClause();
            remapParseTree(describeObjectClause);
            return new ParseTreeInfoImpl(describeObjectClause, QueryType.DESCRIBE_QUERY);
        }

        @Override
        protected ParseTreeInfoImpl aggregateResult(ParseTreeInfoImpl aggregate, ParseTreeInfoImpl nextResult) {
            if (nextResult != null) {
                return nextResult;
            }
            return aggregate;
        }
    }

    @Nonnull
    @Override
    public QueryType getQueryType() {
        return queryType;
    }

    @Nonnull
    public ParseTree getRootContext() {
        return rootContext;
    }

    @Nonnull
    public static ParseTreeInfoImpl from(@Nonnull final RelationalParser.RootContext root) {
        return new QueryTypeVisitor(root).visit(root);
    }
}
