/*
 * UpdateStatementImpl.java
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

import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.ParseTreeInfo;
import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.fluentsql.SqlVisitor;
import com.apple.foundationdb.relational.api.fluentsql.expression.BooleanExpressionTrait;
import com.apple.foundationdb.relational.api.fluentsql.expression.Expression;
import com.apple.foundationdb.relational.api.fluentsql.expression.ExpressionFactory;
import com.apple.foundationdb.relational.api.fluentsql.expression.Field;
import com.apple.foundationdb.relational.api.fluentsql.statement.UpdateStatement;
import com.apple.foundationdb.relational.api.metadata.SchemaTemplate;
import com.apple.foundationdb.relational.api.metadata.Table;
import com.apple.foundationdb.relational.generated.RelationalLexer;
import com.apple.foundationdb.relational.generated.RelationalParser;
import com.apple.foundationdb.relational.generated.RelationalParserBaseVisitor;
import com.apple.foundationdb.relational.recordlayer.query.CaseInsensitiveCharStream;
import com.apple.foundationdb.relational.recordlayer.query.ParseTreeInfoImpl;
import com.apple.foundationdb.relational.recordlayer.query.SemanticAnalyzer;
import com.apple.foundationdb.relational.recordlayer.structuredsql.expression.FieldImpl;
import com.apple.foundationdb.relational.util.Assert;

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.antlr.v4.runtime.CommonToken;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.antlr.v4.runtime.tree.TerminalNodeImpl;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

@API(API.Status.EXPERIMENTAL)
public class UpdateStatementImpl implements UpdateStatement {

    @Nonnull
    private final Table table;

    @Nonnull
    private final String originalTableName;

    @Nonnull
    private final Map<Field<?>, Expression<?>> setClauses;

    @Nullable
    private final BooleanExpressionTrait whereClause;

    @Nonnull
    private final List<Expression<?>> returning;

    @Nonnull
    private final Set<QueryOptions> queryOptions;

    @Nonnull
    private final Supplier<String> sqlGenerationMemoizer;

    @Nonnull
    private final RelationalConnection connection;

    private final boolean caseSensitive;

    @SuppressWarnings("this-escape")
    public UpdateStatementImpl(@Nonnull Table table,
                               @Nonnull final String originalTableName,
                               @Nonnull Map<Field<?>, Expression<?>> setClauses,
                               @Nullable BooleanExpressionTrait whereClause,
                               @Nonnull List<Expression<?>> returning,
                               @Nonnull Set<QueryOptions> queryOptions,
                               @Nonnull RelationalConnection connection) {
        this.table = table;
        this.originalTableName = originalTableName;
        this.setClauses = ImmutableMap.copyOf(setClauses);
        this.whereClause = whereClause;
        this.returning = ImmutableList.copyOf(returning);
        this.queryOptions = ImmutableSet.copyOf(queryOptions);
        this.connection = connection;
        this.sqlGenerationMemoizer = Suppliers.memoize(this::generateQuery);
        this.caseSensitive = connection.getOptions().getOption(Options.Name.CASE_SENSITIVE_IDENTIFIERS);
    }

    @Nonnull
    @Override
    public PreparedStatement getPreparedStatement() throws SQLException {
        return connection.prepareStatement(getSqlQuery());
    }

    @Nonnull
    @Override
    public String getSqlQuery() {
        return sqlGenerationMemoizer.get();
    }

    @Nonnull
    @Override
    public Map<Field<?>, Expression<?>> getSetClauses() {
        return ImmutableMap.copyOf(setClauses);
    }

    @Nonnull
    @Override
    public List<Expression<?>> getReturning() {
        return ImmutableList.copyOf(returning);
    }

    @Nullable
    @Override
    public BooleanExpressionTrait getWhereClause() {
        return whereClause;
    }

    @Nonnull
    @Override
    public Set<QueryOptions> getOptions() {
        return ImmutableSet.copyOf(queryOptions);
    }

    @Nonnull
    @Override
    public String getTable() {
        return table.getName();
    }

    @Nonnull
    private String generateQuery() {
        final var sb = new StringBuilder();
        final var sqlVisitor = new SqlVisitor();
        sb.append("UPDATE ").append("\"").append(SemanticAnalyzer.normalizeString(originalTableName, caseSensitive)).append("\"").append(" SET ");
        final var setClauses = getSetClauses().entrySet();
        final var size = setClauses.size();
        int counter = 0;
        for (final var setClause : getSetClauses().entrySet()) {
            setClause.getKey().accept(sqlVisitor, sb);
            sb.append(" = ");
            setClause.getValue().accept(sqlVisitor, sb);
            if (counter < size - 1) {
                sb.append(",");
            }
            counter++;
        }
        if (whereClause != null) {
            sb.append(" WHERE ");
            whereClause.accept(sqlVisitor, sb);
        }
        if (!returning.isEmpty()) {
            sb.append(" RETURNING ");
            returning.get(0).accept(sqlVisitor, sb);
            for (int i = 1; i < returning.size(); i++) {
                sb.append(", ");
                returning.get(i).accept(sqlVisitor, sb);
            }
        }
        if (!queryOptions.isEmpty()) {
            sb.append(" OPTIONS (");
            sb.append(queryOptions.stream().map(QueryOptions::getName).collect(Collectors.joining(", ")));
            sb.append(")");
        }
        return sb.toString();
    }

    public static class BuilderImpl implements UpdateStatement.Builder {

        @Nonnull
        private final RelationalConnection connection;

        @Nonnull
        private final SchemaTemplate schemaTemplate;

        private Table table;

        private String originalTableName;

        @Nonnull
        private Map<Field<?>, Expression<?>> setClauses;

        @Nullable
        private BooleanExpressionTrait whereClause;

        @Nonnull
        private final List<Expression<?>> returning;

        @Nonnull
        private final ImmutableSet.Builder<QueryOptions> queryOptionsBuilder;

        private final boolean isCaseSensitive;

        public BuilderImpl(@Nonnull final RelationalConnection connection,
                             @Nonnull final SchemaTemplate schemaTemplate) {
            this.connection = connection;
            this.schemaTemplate = schemaTemplate;
            this.setClauses = new LinkedHashMap<>();
            this.returning = new ArrayList<>();
            this.queryOptionsBuilder = ImmutableSet.builder();
            this.isCaseSensitive = connection.getOptions().getOption(Options.Name.CASE_SENSITIVE_IDENTIFIERS);
        }

        @Nonnull
        @Override
        public Map<Field<?>, Expression<?>> getSetClauses() {
            return setClauses;
        }

        @Nonnull
        @Override
        public Builder addSetClause(@Nonnull Field<?> field, @Nonnull Expression<?> newValue) {
            setClauses.put(field, newValue);
            return this;
        }

        @Nonnull
        @Override
        public Builder clearSetClauses() {
            this.setClauses.clear();
            return this;
        }

        @Nonnull
        @Override
        public Builder removeSetClause(@Nonnull Field<?> field) {
            setClauses = setClauses.entrySet().stream().filter(pair -> !pair.getKey().equals(field)).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            return this;
        }

        @Nonnull
        @Override
        public List<Expression<?>> getReturning() {
            return returning;
        }

        @Nonnull
        @Override
        public Builder addReturning(@Nonnull Expression<?> expression) {
            this.returning.add(expression);
            return this;
        }

        @Nonnull
        @Override
        public Builder clearReturning() {
            this.returning.clear();
            return this;
        }

        @Nullable
        @Override
        public BooleanExpressionTrait getWhereClause() {
            return whereClause;
        }

        @Nonnull
        @Override
        public Builder addWhereClause(@Nonnull BooleanExpressionTrait expression) {
            if (whereClause != null) {
                whereClause = whereClause.and(expression);
            } else {
                whereClause = expression;
            }
            return this;
        }

        @Nonnull
        @Override
        public Builder clearWhereClause() {
            this.whereClause = null;
            return this;
        }

        @Nonnull
        @Override
        public String getTable() {
            return originalTableName;
        }

        @Nonnull
        @Override
        public Builder setTable(@Nonnull final String table) {
            final var normalizedTableName =
                    Assert.notNullUnchecked(SemanticAnalyzer.normalizeString(table, isCaseSensitive));
            Optional<Table> maybeTable;
            try {
                maybeTable = schemaTemplate.findTableByName(normalizedTableName);
            } catch (RelationalException e) {
                throw e.toUncheckedWrappedException();
            }
            Assert.thatUnchecked(
                    maybeTable.isPresent(), ErrorCode.UNDEFINED_TABLE, "table '%s' is not found", table);
            this.originalTableName = table;
            this.table = maybeTable.get();
            return this;
        }

        @Nonnull
        @Override
        public Builder resolveSetFields(@Nonnull final ExpressionFactory expressionFactory) {
            if (setClauses.entrySet().stream().allMatch(entry -> entry.getKey() instanceof FieldImpl<?>)) {
                return this;
            }
            setClauses = setClauses.entrySet().stream().map(entry -> {
                if (entry.getKey() instanceof FieldImpl<?>) {
                    return entry;
                } else {
                    return Map.entry(expressionFactory.field(originalTableName, entry.getKey().getParts()), entry.getValue());
                }
            }).collect(Collectors.toMap(
                    Map.Entry::getKey,
                    Map.Entry::getValue));
            return this;
        }

        @Nonnull
        @Override
        public Builder withOption(@Nonnull final QueryOptions... options) {
            Arrays.stream(options).forEach(queryOptionsBuilder::add);
            return this;
        }

        @Nonnull
        @Override
        public Set<QueryOptions> getOptions() {
            return queryOptionsBuilder.build();
        }

        @Nonnull
        @Override
        public UpdateStatement build() throws RelationalException {
            Assert.notNull(table, ErrorCode.UNDEFINED_TABLE, "table is not set");
            Assert.that(!setClauses.isEmpty(), ErrorCode.INTERNAL_ERROR, "update set clauses is empty"); // improve error code.
            return new UpdateStatementImpl(table, originalTableName, setClauses, whereClause, returning, queryOptionsBuilder.build(), connection);
        }

        @SuppressWarnings("PMD.FormalParameterNamingConventions")
        private static final class UpdateVisitor extends RelationalParserBaseVisitor<Void> {
            @Nonnull
            @SuppressWarnings("PMD.AvoidStringBufferField") // the visitor is used within the builder, it should be very short-lived.
            private final StringBuilder queryStringScratchpad;

            @Nonnull
            private final Builder updateBuilder;

            @Nonnull
            private final ExpressionFactory expressionFactory;

            private boolean isUpdateStatement;

            public UpdateVisitor(@Nonnull final RelationalConnection connection,
                                 @Nonnull final SchemaTemplate schemaTemplate,
                                 @Nonnull final RelationalParser.RootContext ast) {
                this(connection, schemaTemplate, ast, Map.of());
            }

            public UpdateVisitor(@Nonnull final RelationalConnection connection,
                                 @Nonnull final SchemaTemplate schemaTemplate,
                                 @Nonnull final RelationalParser.RootContext ast,
                                 @Nonnull final Map<String, List<String>> columnSynonyms) {
                this.queryStringScratchpad = new StringBuilder();
                this.updateBuilder = new BuilderImpl(connection, schemaTemplate);
                try {
                    this.expressionFactory = connection.createExpressionBuilderFactory();
                } catch (SQLException e) {
                    throw new RelationalException(e).toUncheckedWrappedException();
                }
                this.isUpdateStatement = false;
                // TODO: remove once (TODO ([POST] Synonym support in Relational Metadata) is implemented.
                if (!columnSynonyms.isEmpty()) {
                    final var uidReplacer = new UidReplacer(columnSynonyms, connection.getOptions().getOption(Options.Name.CASE_SENSITIVE_IDENTIFIERS));
                    uidReplacer.visit(ast);
                }
                visit(ast);
                if (!isUpdateStatement) {
                    throw new RelationalException("expecting an update statement", ErrorCode.INTERNAL_ERROR).toUncheckedWrappedException();
                }
            }

            @Nonnull
            Builder getUpdateBuilder() {
                return updateBuilder;
            }

            @Nonnull
            private String withNewScratchPad(@Nonnull final Consumer<Void> consumer) {
                queryStringScratchpad.setLength(0);
                consumer.accept(null);
                return queryStringScratchpad.toString().trim();
            }

            @Override
            public Void visitUpdateStatement(RelationalParser.UpdateStatementContext ctx) {
                isUpdateStatement = true;
                final var table = withNewScratchPad(Null -> visit(ctx.tableName()));
                updateBuilder.setTable(table);

                if (ctx.uid() != null) {
                    Assert.failUnchecked(ErrorCode.UNSUPPORTED_QUERY, "Construction of 'UPDATE' query builder with aliased target table is not supported");
                }

                ctx.updatedElement().forEach(updatedElementContext -> {
                    final var updatedColumn = handleFullId(updatedElementContext.fullColumnName().fullId());
                    final var setValue = withNewScratchPad(Null -> visit(updatedElementContext.expression()));
                    updateBuilder.addSetClause(expressionFactory.field(table, updatedColumn), expressionFactory.parseFragment(setValue));
                });

                if (ctx.WHERE() != null) {
                    final var whereClauseParts = withNewScratchPad(Null -> visit(ctx.whereExpr().expression()));
                    updateBuilder.addWhereClause(expressionFactory.parseFragment(whereClauseParts).asBoolean());
                }

                if (ctx.RETURNING() != null) {
                    final var projectionList = handleReturningSelectElements(ctx.selectElements());
                    for (final var projectionItem : projectionList) {
                        updateBuilder.addReturning(expressionFactory.parseFragment(projectionItem));
                    }
                }

                if (ctx.queryOptions() != null) {
                    visit(ctx.queryOptions());
                }
                return null;
            }

            @Override
            public Void visitQueryOption(RelationalParser.QueryOptionContext ctx) {
                if (ctx.NOCACHE() != null) {
                    updateBuilder.withOption(QueryOptions.NOCACHE);
                } else if (ctx.LOG() != null) {
                    updateBuilder.withOption(QueryOptions.LOG_QUERY);
                } else if (ctx.DRY() != null) {
                    updateBuilder.withOption(QueryOptions.DRY_RUN);
                }
                return null;
            }

            @Nonnull
            private List<String> handleFullId(RelationalParser.FullIdContext ctx) {
                Assert.thatUnchecked(!ctx.uid().isEmpty());
                return ctx.uid().stream().map(this::handleUid).map(Assert::notNullUnchecked).collect(Collectors.toList());
            }

            @Nonnull
            private String handleUid(RelationalParser.UidContext ctx) {
                if (ctx.simpleId() != null) {
                    return Assert.notNullUnchecked(ctx.simpleId().getText());
                } else {
                    return Assert.notNullUnchecked(ctx.getText());
                }
            }

            @Nonnull
            private List<String> handleReturningSelectElements(RelationalParser.SelectElementsContext selectElementsContext) {
                final ImmutableList.Builder<String> result = ImmutableList.builder();
                selectElementsContext.selectElement().forEach(selectElement -> result.add(withNewScratchPad(Null -> visit(selectElement))));
                return result.build();
            }

            @Override
            public Void visitTerminal(TerminalNode node) {
                if (node.getSymbol().getType() == Token.EOF) {
                    return null;
                }
                queryStringScratchpad.append(node.getText()).append(" ");
                return null;
            }
        }

        public static final class CustomSimpleId extends RelationalParser.UidContext {

            public CustomSimpleId(ParserRuleContext parent, int invokingState, @Nonnull final String name) {
                super(parent, invokingState);
                addChild(new TerminalNodeImpl(new CustomCommonToken(name)));
            }

            public static class CustomCommonToken extends CommonToken {

                private static final long serialVersionUID = 3459762348795L;

                @Nonnull
                private final String name;

                public CustomCommonToken(@Nonnull final String name) {
                    super(0); // maybe use something else.
                    this.name = name;
                }

                @Override
                public String getText() {
                    return name;
                }
            }
        }

        private static final class UidReplacer extends RelationalParserBaseVisitor<Void> {

            @Nonnull
            private final ImmutableMap<String, List<String>> synonymsMap;

            private final boolean isCaseSensitive;

            private UidReplacer(@Nonnull final Map<String, List<String>> symnonymsMap, boolean isCaseSensitive) {
                ImmutableMap.Builder<String, List<String>> normalizedSynonymsMap = ImmutableMap.builder();
                for (final var synonymsPair : symnonymsMap.entrySet()) {
                    final var normalizedKey = Assert.notNullUnchecked(SemanticAnalyzer.normalizeString(synonymsPair.getKey(), isCaseSensitive));
                    final var normalizedValue = synonymsPair.getValue().stream().map(part -> Assert.notNullUnchecked(SemanticAnalyzer.normalizeString(part, isCaseSensitive))).collect(Collectors.toUnmodifiableList());
                    normalizedSynonymsMap.put(normalizedKey, normalizedValue);
                }
                this.synonymsMap = normalizedSynonymsMap.build();
                this.isCaseSensitive = isCaseSensitive;
            }

            @Override
            public Void visitUid(RelationalParser.UidContext ctx) {
                final var normalizeUid = SemanticAnalyzer.normalizeString(ctx.getText(), isCaseSensitive);
                final var synonymsValueMaybe = synonymsMap.get(normalizeUid);
                if (synonymsValueMaybe != null) {
                    if (!(ctx.parent instanceof ParserRuleContext)) {
                        return null;
                    }
                    final var parent = (ParserRuleContext) ctx.parent;
                    int childIndex = -1;
                    for (int i = 0; i < parent.getChildCount(); i++) {
                        if (parent.children.get(i) == ctx) {
                            childIndex = i;
                            break;
                        }
                    }
                    if (childIndex < 0) {
                        return null;
                    }

                    ImmutableList.Builder<ParseTree> newNodes = ImmutableList.builder();
                    newNodes.addAll(parent.children.subList(0, childIndex));
                    for (int i = 0; i < synonymsValueMaybe.size(); i++) {
                        newNodes.add(new CustomSimpleId(parent, 1, "\"" + synonymsValueMaybe.get(i) + "\""));
                        if (i < synonymsValueMaybe.size() - 1) {
                            newNodes.add(new TerminalNodeImpl(new CustomSimpleId.CustomCommonToken(".")));
                        }
                    }
                    newNodes.addAll(parent.children.subList(childIndex + 1, parent.children.size()));
                    parent.children = newNodes.build();
                }
                return null;
            }
        }

        @Nonnull
        public static Builder fromQuery(@Nonnull final RelationalConnection relationalConnection,
                                        @Nonnull final SchemaTemplate schemaTemplate,
                                        @Nonnull final String updateQuery,
                                        @Nonnull final Map<String, List<String>> columnSynonyms) {
            final var tokenSource = new RelationalLexer(new CaseInsensitiveCharStream(updateQuery));
            final var parseTree = new RelationalParser(new CommonTokenStream(tokenSource));
            return fromParseTreeInfoImpl(relationalConnection, schemaTemplate, ParseTreeInfoImpl.from(parseTree.root()), columnSynonyms);
        }

        @Nonnull
        public static Builder fromParseTreeInfoImpl(@Nonnull final RelationalConnection relationalConnection,
                                                    @Nonnull final SchemaTemplate schemaTemplate,
                                                    @Nonnull final ParseTreeInfoImpl parseTreeInfo,
                                                    @Nonnull final Map<String, List<String>> columnSynonyms) {
            Assert.thatUnchecked(parseTreeInfo.getQueryType().equals(ParseTreeInfo.QueryType.UPDATE),
                    String.format(Locale.ROOT, "Expecting update statement, got '%s' statement", parseTreeInfo.getQueryType().name()));
            final var updateVisitor = new UpdateVisitor(relationalConnection, schemaTemplate, parseTreeInfo.getRootContext(), columnSynonyms);
            return updateVisitor.getUpdateBuilder();
        }
    }
}
