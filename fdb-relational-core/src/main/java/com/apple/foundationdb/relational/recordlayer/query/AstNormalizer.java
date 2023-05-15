/*
 * AstHasher.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2023 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.metadata.SchemaTemplate;
import com.apple.foundationdb.relational.generated.RelationalLexer;
import com.apple.foundationdb.relational.generated.RelationalParser;
import com.apple.foundationdb.relational.generated.RelationalParserBaseVisitor;
import com.apple.foundationdb.relational.recordlayer.query.cache.QueryCacheKey;
import com.apple.foundationdb.relational.recordlayer.util.Assert;
import com.apple.foundationdb.relational.recordlayer.util.ExceptionUtil;

import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.RuleNode;
import org.antlr.v4.runtime.tree.TerminalNode;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;
import java.math.BigInteger;
import java.sql.SQLException;
import java.util.Base64;
import java.util.BitSet;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * Visitor of SQL query abstract syntax tree (AST) that does the following:
 * <ul>
 *     <li>strip literals from the query and put them in a separate ordered list, to be used later during planning and execution</li>
 *     <li>process prepared parameters and replace their values in the literals ordered list</li>
 *     <li>generate canonical representation of the query syntax</li>
 *     <li>generate a query hash</li>
 *     <li>understand parameters necessary for execution: limit, offset, continuation, and explain flag. These parameters are
 *     necessary for Relational to prepare the plan for execution.</li>
 *     <li>process in-predicate and generate array literal of the in-list contains simple constants.</li>
 *     <li>identify query caching flags that might influence its interaction with plan cache (see {@link Result.QueryCachingFlags}).</li>
 * </ul>
 *
 * The visitor is designed to be very fast, it does not perform any semantic checks leaving that to {@link AstVisitor}.
 * Its main purpose is to lookup queries in the plan cache, and generate enough context to be able to execute a matching
 * physical plan.
 * <br>
 * @implNote this class is currently not thread-safe, I do not see currently any reason for making it so as it is mainly a
 * short-lived CPU-bound closure that visits a query's AST.
 * <br>
 * this does not support array literals yet, we have some ad-hoc support for flat literal arrays for handling in-predicate.
 */
@SuppressWarnings({"UnstableApiUsage", "PMD.AvoidStringBufferField"}) // AstHasher is short-lived, therefore, using StringBuilder is ok.
@NotThreadSafe
public final class AstNormalizer extends RelationalParserBaseVisitor<Object> {

    @Nonnull
    private final Hasher hashFunction;

    @Nonnull
    private final StringBuilder sqlCanonicalizer;

    /**
     * Controls whether a token should be considered for the hash function or not. This is necessary for handling
     * e.g. {@code LIMIT} clause, and effectively ignore it from hash calculation, so we end up having two other-wise
     * identical queries, one with {@code LIMIT} and other without it, having the same hash value.
     */
    private boolean allowTokenAddition;

    /**
     * Controls whether a literal should be added as a new entry in the literals array or not. This is necessary
     * for handling {@code IN}-predicate literals, such that we gather all of them in a single-dimensional array
     * instead of adding them separately to the literals array.
     */
    private boolean allowLiteralAddition;

    @Nonnull
    private final QueryHasherContext.Builder context;

    @Nonnull
    private final PreparedStatementParameters preparedStatementParameters;

    @Nonnull
    private final Set<Result.QueryCachingFlags> queryCachingFlags;

    @Nonnull
    private Options.Builder queryOptions;

    @Nonnull
    private static Map<Class<?>, Function<ParserRuleContext, Object>> literalNodes = new HashMap<>();

    static {
        literalNodes.put(RelationalParser.BooleanLiteralContext.class, context -> {
            final var ctx = (RelationalParser.BooleanLiteralContext) context;
            return ctx.FALSE() == null;
        });
        literalNodes.put(RelationalParser.HexadecimalConstantContext.class, context -> new BigInteger(context.getText().substring(2, context.getText().length() - 1), 16).longValue());
        literalNodes.put(RelationalParser.StringConstantContext.class, context -> ParserUtils.normalizeString(context.getText()));
        literalNodes.put(RelationalParser.DecimalConstantContext.class, context -> ParserUtils.parseDecimal(context.getText()).getLiteralValue());
        literalNodes.put(RelationalParser.NegativeDecimalConstantContext.class, context -> ParserUtils.parseDecimal(context.getText()).getLiteralValue());
    }

    private AstNormalizer(@Nonnull final PreparedStatementParameters preparedStatementParameters) {
        hashFunction = Hashing.murmur3_32_fixed().newHasher();
        sqlCanonicalizer = new StringBuilder();
        // needed to collect information that guide query execution (explain flag, continuation string, offset int, and limit int).
        context = QueryHasherContext.newBuilder().setPreparedStatementParameters(preparedStatementParameters);
        this.preparedStatementParameters = preparedStatementParameters;
        allowTokenAddition = true;
        allowLiteralAddition = true;
        queryCachingFlags = EnumSet.noneOf(Result.QueryCachingFlags.class);
        queryOptions = Options.builder();
    }

    @Override
    public Void visitChildren(RuleNode node) {
        if (literalNodes.containsKey(node.getClass())) {
            processLiteral(literalNodes.get(node.getClass()).apply((ParserRuleContext) node));
            return null;
        }
        if (allowTokenAddition) {
            hashFunction.putInt(node.getClass().hashCode());
        }
        for (int i = 0; i < node.getChildCount(); i++) {
            final var child = Assert.notNullUnchecked(node.getChild(i));
            child.accept(this);
        }
        return null;
    }

    @Override
    public Void visitTerminal(TerminalNode node) {
        if (node.getSymbol().getType() != Token.EOF) {
            sqlCanonicalizer.append(node.getText()).append(" ");
        }
        return null;
    }

    public int getHash() {
        return hashFunction.hash().asInt();
    }

    @Nonnull
    public String getCanonicalSqlString() {
        return sqlCanonicalizer.toString();
    }

    @Nonnull
    public Set<Result.QueryCachingFlags> getQueryCachingFlags() {
        return queryCachingFlags;
    }

    @Nonnull
    public Options getQueryOptions() {
        return queryOptions.build();
    }

    @Nonnull
    public QueryExecutionParameters getQueryExecutionParameters() {
        return context.build();
    }

    @Override
    public Void visitFullDescribeStatement(RelationalParser.FullDescribeStatementContext ctx) {
        // (yhatem) this is probably not needed, since a cached physical plan _knows_ it is either forExplain or not.
        //          we should remove this, but ok for now.
        context.setForExplain(ctx.EXPLAIN() != null);
        return visitChildren(ctx);
    }

    @Override
    public Void visitLimitClause(RelationalParser.LimitClauseContext ctx) {
        if (ctx.offset != null) {
            // Owing to TODO
            Assert.failUnchecked("OFFSET clause is not supported.");
        }
        ctx.limit.accept(this);
        return null;
    }

    @Override
    public Void visitLimitClauseAtom(RelationalParser.LimitClauseAtomContext ctx) {
        allowTokenAddition = false;
        allowLiteralAddition = false;
        if (ctx.preparedStatementParameter() != null) {
            final var parameter = visit(ctx.preparedStatementParameter());
            Assert.thatUnchecked(parameter instanceof Integer, "argument for LIMIT must be integer", ErrorCode.DATATYPE_MISMATCH);
            Assert.thatUnchecked((Integer) parameter > 0, "LIMIT must be positive", ErrorCode.INVALID_ROW_COUNT_IN_LIMIT_CLAUSE);
            context.setLimit((Integer) parameter);
        } else {
            final var limit = ParserUtils.parseDecimal(ctx.getText());
            Assert.thatUnchecked(limit.getLiteralValue() instanceof Integer, "argument for LIMIT must be integer", ErrorCode.DATATYPE_MISMATCH);
            final var limitAsInteger = (Integer) limit.getLiteralValue();
            Assert.thatUnchecked(limitAsInteger > 0, "LIMIT must be positive", ErrorCode.INVALID_ROW_COUNT_IN_LIMIT_CLAUSE);
            context.setLimit(limitAsInteger);
        }
        allowLiteralAddition = true;
        allowTokenAddition = true;
        return null;
    }

    @Override
    public Object visitSelectStatementWithContinuation(RelationalParser.SelectStatementWithContinuationContext ctx) {
        if (queryCachingFlags.isEmpty()) {
            queryCachingFlags.add(Result.QueryCachingFlags.IS_DQL_STATEMENT);
        }
        ctx.selectStatement().accept(this);
        if (ctx.continuationAtom() != null) {
            ctx.continuationAtom().accept(this);
        }
        return null;
    }

    @Override
    public Object visitQueryOption(RelationalParser.QueryOptionContext ctx) {
        try {
            if (ctx.NOCACHE() != null) {
                queryCachingFlags.add(Result.QueryCachingFlags.WITH_NO_CACHE_OPTION);
            }
            if (ctx.LOG() != null) {
                queryOptions.withOption(Options.Name.LOG_QUERY, true);
            }
            return visitChildren(ctx);
        } catch (SQLException e) {
            throw ExceptionUtil.toRelationalException(e).toUncheckedWrappedException();
        }
    }

    @Override
    public Object visitDdlStatement(RelationalParser.DdlStatementContext ctx) {
        queryCachingFlags.add(Result.QueryCachingFlags.IS_DDL_STATEMENT);
        return visitChildren(ctx);
    }

    @Override
    public Object visitInsertStatement(RelationalParser.InsertStatementContext ctx) {
        queryCachingFlags.add(Result.QueryCachingFlags.IS_DML_STATEMENT);
        return visitChildren(ctx);
    }

    @Override
    public Object visitUpdatedElement(RelationalParser.UpdatedElementContext ctx) {
        queryCachingFlags.add(Result.QueryCachingFlags.IS_DML_STATEMENT);
        return visitChildren(ctx);
    }

    @Override
    public Object visitDeleteStatement(RelationalParser.DeleteStatementContext ctx) {
        queryCachingFlags.add(Result.QueryCachingFlags.IS_DML_STATEMENT);
        return visitChildren(ctx);
    }

    @Override
    public Object visitAdministrationStatement(RelationalParser.AdministrationStatementContext ctx) {
        queryCachingFlags.add(Result.QueryCachingFlags.IS_ADMIN_STATEMENT);
        return visitChildren(ctx);
    }

    @Override
    public Object visitUtilityStatement(RelationalParser.UtilityStatementContext ctx) {
        queryCachingFlags.add(Result.QueryCachingFlags.IS_UTILITY_STATEMENT);
        return visitChildren(ctx);
    }

    @Override
    public Void visitContinuationAtom(RelationalParser.ContinuationAtomContext ctx) {
        allowLiteralAddition = false;
        allowTokenAddition = false;
        if (ctx.stringLiteral() != null) {
            final var continuationStr = ParserUtils.normalizeString(ctx.stringLiteral().getText());
            Assert.notNullUnchecked(continuationStr, "Illegal query with BEGIN continuation.", ErrorCode.INVALID_CONTINUATION);
            Assert.thatUnchecked(!continuationStr.isEmpty(), "Illegal query with END continuation.", ErrorCode.INVALID_CONTINUATION);
            final var continuationBytes = Base64.getDecoder().decode(continuationStr);
            context.setContinuation(continuationBytes);
            processLiteral(continuationBytes);
        } else {
            final var continuation = visit(ctx.preparedStatementParameter());
            Assert.thatUnchecked(continuation instanceof byte[]);
            context.setContinuation((byte[]) continuation);
        }
        allowLiteralAddition = true;
        allowTokenAddition = true;
        return null;
    }

    @Override
    public Object visitPreparedStatementParameter(RelationalParser.PreparedStatementParameterContext ctx) {
        Object param;
        if (ctx.QUESTION() != null) {
            param = preparedStatementParameters.getNextParameter();
            processLiteral(param);
        } else {
            // Note we preserve named parameters in canonical representation, otherwise we could mix up different queries
            // if we use '?' ubiquitously.
            // e.g. select * from t1 where col1 = ?P1 and col2 = ?P2
            //      select * from t1 where col1 = ?P2 and col2 = ?P1
            param = preparedStatementParameters.getNamedParameter(ctx.NAMED_PARAMETER().getText().substring(1));
            processLiteral(param, ctx.NAMED_PARAMETER().getText());
        }
        return param;
    }

    @Override
    public Object visitInPredicate(RelationalParser.InPredicateContext ctx) {
        ctx.predicate().accept(this);
        ctx.IN().accept(this);

        sqlCanonicalizer.append("( ");
        if (ParserUtils.isConstant(ctx.expressions())) {
            // todo (yhatem) we should prevent making the constant expressions
            //   contribute to the hash or the canonical query representation.
            context.startArrayLiteral();
            allowTokenAddition = false;
            sqlCanonicalizer.append("[ ");
            for (int i = 0; i < ctx.expressions().expression().size(); i++) {
                visit(ctx.expressions().expression(i));
            }
            context.finishArrayLiteral();
            allowTokenAddition = true;
            sqlCanonicalizer.append("] ");
        } else {
            final var size = ctx.expressions().expression().size();
            for (int i = 0; i < size; i++) {
                visit(ctx.expressions().expression(i));
                if (i < size - 1) {
                    sqlCanonicalizer.append(", ");
                }
            }
        }

        sqlCanonicalizer.append(") ");
        return null;
    }

    private void processLiteral(@Nonnull final Object literal) {
        processLiteral(literal, "?");
    }

    private void processLiteral(@Nonnull final Object literal, @Nonnull final String canonicalName) {
        if (allowLiteralAddition) {
            context.addLiteral(literal);
        }
        if (allowTokenAddition) {
            sqlCanonicalizer.append(canonicalName).append(" ");
        }
    }

    @Nonnull
    public static Result normalizeQuery(@Nonnull final SchemaTemplate schemaTemplate,
                                        @Nonnull final String query,
                                        int userVersion,
                                        @Nonnull final BitSet readableIndexes) throws RelationalException {
        return normalizeQuery(schemaTemplate, query, PreparedStatementParameters.empty(), userVersion, readableIndexes);
    }

    @Nonnull
    public static Result normalizeQuery(@Nonnull final SchemaTemplate schemaTemplate,
                                        @Nonnull final String query,
                                        @Nonnull final PreparedStatementParameters preparedStatementParameters,
                                        int userVersion,
                                        @Nonnull final BitSet readableIndexes) throws RelationalException {
        final RelationalLexer tokenSource = new RelationalLexer(new CaseInsensitiveCharStream(query));
        final RelationalParser parser = new RelationalParser(new CommonTokenStream(tokenSource));
        parser.removeErrorListeners();
        final SyntaxErrorListener listener = new SyntaxErrorListener();
        parser.addErrorListener(listener);
        RelationalParser.RootContext rootContext = parser.root();
        if (!listener.getSyntaxErrors().isEmpty()) {
            throw listener.getSyntaxErrors().get(0).toRelationalException();
        }
        return normalizeAst(schemaTemplate, rootContext, preparedStatementParameters, userVersion, readableIndexes);
    }

    @Nonnull
    private static Result normalizeAst(@Nonnull final SchemaTemplate schemaTemplate,
                                       @Nonnull final RelationalParser.RootContext context,
                                       @Nonnull final PreparedStatementParameters preparedStatementParameters,
                                       int userVersion,
                                       @Nonnull final BitSet readableIndexes) {
        final var astNormalizer = new AstNormalizer(preparedStatementParameters);
        astNormalizer.visit(context);
        return new Result(QueryCacheKey.of(astNormalizer.getCanonicalSqlString(), astNormalizer.getHash(), schemaTemplate.getName(), schemaTemplate.getVersion(), readableIndexes, userVersion),
                astNormalizer.getQueryExecutionParameters(),
                context,
                astNormalizer.getQueryCachingFlags(),
                astNormalizer.getQueryOptions());
    }

    public static class Result {

        /**
         * A set of flags that determine how the query should interact with the plan cache.
         * <br>
         * Note: this is not designed to work with SQL Scripts or stored procedures
         */
        public enum QueryCachingFlags {
            IS_DDL_STATEMENT,
            IS_DML_STATEMENT,
            IS_DQL_STATEMENT,
            IS_UTILITY_STATEMENT,
            IS_ADMIN_STATEMENT,
            /**
             * user explicitly wants to avoid using plan cache with this query via {@code NOCACHE} option.
             */
            WITH_NO_CACHE_OPTION;
        }

        @Nonnull
        private final QueryCacheKey queryCacheKey;

        @Nonnull
        private final QueryExecutionParameters queryExecutionParameters;

        @Nonnull
        private final ParseTree parseTree;

        @Nonnull
        private final Set<QueryCachingFlags> queryCachingFlags;

        @Nonnull
        private final Options queryOptions;

        public Result(@Nonnull final QueryCacheKey queryCacheKey,
                      @Nonnull final QueryExecutionParameters queryExecutionParameters,
                      @Nonnull final ParseTree parseTree,
                      @Nonnull final Set<QueryCachingFlags> queryCachingFlags,
                      @Nonnull final Options queryOptions) {
            this.queryCacheKey = queryCacheKey;
            this.queryExecutionParameters = queryExecutionParameters;
            this.parseTree = parseTree;
            this.queryCachingFlags = queryCachingFlags;
            this.queryOptions = queryOptions;
        }

        @Nonnull
        public QueryCacheKey getQueryCacheKey() {
            return queryCacheKey;
        }

        @Nonnull
        public QueryExecutionParameters getQueryExecutionParameters() {
            return queryExecutionParameters;
        }

        @Nonnull
        public ParseTree getParseTree() {
            return parseTree;
        }

        @Nonnull
        public Set<QueryCachingFlags> getQueryCachingFlags() {
            return queryCachingFlags;
        }

        @Nonnull
        public Options getQueryOptions() {
            return queryOptions;
        }

    }
}
