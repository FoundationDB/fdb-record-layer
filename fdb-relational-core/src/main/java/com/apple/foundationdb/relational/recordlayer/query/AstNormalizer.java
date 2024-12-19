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

import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.SqlTypeSupport;
import com.apple.foundationdb.relational.api.RelationalStruct;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.metadata.SchemaTemplate;
import com.apple.foundationdb.relational.api.metrics.RelationalMetric;
import com.apple.foundationdb.relational.generated.RelationalParser;
import com.apple.foundationdb.relational.generated.RelationalParserBaseVisitor;
import com.apple.foundationdb.relational.recordlayer.query.cache.QueryCacheKey;
import com.apple.foundationdb.relational.recordlayer.util.ExceptionUtil;
import com.apple.foundationdb.relational.util.Assert;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Suppliers;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.RuleNode;
import org.antlr.v4.runtime.tree.TerminalNode;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import java.nio.charset.StandardCharsets;
import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Struct;
import java.util.BitSet;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

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
 * <p>
 * The visitor is designed to be very fast;
 * it does not perform any semantic checks
 * leaving that to {@link com.apple.foundationdb.relational.recordlayer.query.visitors.BaseVisitor}, et al.
 * Its main purpose is to lookup queries in the plan cache, and generate enough context to be able to execute a matching
 * physical plan.
 * <br>
 *
 * @implNote this class is currently not thread-safe, I do not see currently any reason for making it so as it is mainly a
 * short-lived CPU-bound closure that visits a query's AST.
 * <br>
 * this does not support array literals yet, we have some ad-hoc support for flat literal arrays for handling in-predicate.
 */
@SuppressWarnings({"UnstableApiUsage", "PMD.AvoidStringBufferField"})
// AstHasher is short-lived, therefore, using StringBuilder is ok.
@NotThreadSafe
public final class AstNormalizer extends RelationalParserBaseVisitor<Object> {

    @Nonnull
    private final Hasher hashFunction;

    private final Hasher parameterHash;

    private final Supplier<Integer> parameterHashSupplier;

    @Nonnull
    private final StringBuilder sqlCanonicalizer;

    private final boolean caseSensitive;

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
    private final PreparedParams preparedStatementParameters;

    @Nonnull
    private final Set<Result.QueryCachingFlags> queryCachingFlags;

    @Nonnull
    private final Options.Builder queryOptions;

    @Nonnull
    private static Map<Class<?>, Function<ParserRuleContext, Object>> literalNodes = new HashMap<>();

    static {
        literalNodes.put(RelationalParser.BooleanLiteralContext.class, context -> {
            final var ctx = (RelationalParser.BooleanLiteralContext) context;
            return ctx.FALSE() == null;
        });
        literalNodes.put(RelationalParser.BytesConstantContext.class, context -> ParseHelpers.parseBytes(context.getText()));
        literalNodes.put(RelationalParser.StringConstantContext.class, context -> SemanticAnalyzer.normalizeString(context.getText(), false));
        literalNodes.put(RelationalParser.DecimalConstantContext.class, context -> ParseHelpers.parseDecimal(context.getText()));
        literalNodes.put(RelationalParser.NegativeDecimalConstantContext.class, context -> ParseHelpers.parseDecimal(context.getText()));
    }

    private AstNormalizer(@Nonnull final PreparedParams preparedStatementParameters, boolean caseSensitive,
                          @Nonnull final PlanHashable.PlanHashMode currentPlanHashMode) {
        hashFunction = Hashing.murmur3_32_fixed().newHasher();
        parameterHash = Hashing.murmur3_32_fixed().newHasher().putInt("ParameterHash".hashCode());
        parameterHashSupplier = Suppliers.memoize(() -> parameterHash.hash().asInt())::get;
        sqlCanonicalizer = new StringBuilder();
        // needed to collect information that guide query execution (explain flag, continuation string, offset int, and limit int).
        context = QueryHasherContext.newBuilder().setPlanHashMode(currentPlanHashMode);
        this.preparedStatementParameters = preparedStatementParameters;
        allowTokenAddition = true;
        allowLiteralAddition = true;
        queryCachingFlags = EnumSet.noneOf(Result.QueryCachingFlags.class);
        queryOptions = Options.builder();
        this.caseSensitive = caseSensitive;
    }

    @Override
    public Void visitChildren(@Nonnull RuleNode node) {
        if (literalNodes.containsKey(node.getClass())) {
            final var ruleContext = (ParserRuleContext) node;
            processScalarLiteral(literalNodes.get(node.getClass()).apply(ruleContext), ruleContext.getStart().getTokenIndex());
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
    public Void visitTerminal(@Nonnull TerminalNode node) {
        if (node.getSymbol().getType() != Token.EOF) {
            sqlCanonicalizer.append(node.getText()).append(" ");
        }
        return null;
    }

    @Override
    public Value visitUid(@Nonnull RelationalParser.UidContext ctx) {
        String uid = SemanticAnalyzer.normalizeString(ctx.getText(), caseSensitive);
        sqlCanonicalizer.append("\"").append(uid).append("\"").append(" ");
        return null;
    }

    public int getHash() {
        return hashFunction.hash().asInt();
    }

    public int getParameterHash() {
        // Hash function can only be called once, so memoizing to cache the return value after the first time it was called
        return parameterHashSupplier.get();
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
    public QueryExecutionContext getQueryExecutionParameters() {
        context.setParameterHash(getParameterHash());
        return context.build();
    }

    @Override
    public Void visitFullDescribeStatement(@Nonnull RelationalParser.FullDescribeStatementContext ctx) {
        // (yhatem) this is probably not needed, since a cached physical plan _knows_ it is either forExplain or not.
        //          we should remove this, but ok for now.
        context.setForExplain(ctx.EXPLAIN() != null);
        return visitChildren(ctx);
    }

    @Override
    public Void visitLimitClause(@Nonnull RelationalParser.LimitClauseContext ctx) {
        if (ctx.offset != null) {
            // Owing to TODO
            Assert.failUnchecked(ErrorCode.UNSUPPORTED_QUERY, "OFFSET clause is not supported.");
        }
        if (ctx.limit != null) {
            Assert.failUnchecked(ErrorCode.UNSUPPORTED_QUERY, "LIMIT clause is not supported.");
        }
        return null;
    }

    @Override
    public Void visitLimitClauseAtom(RelationalParser.LimitClauseAtomContext ctx) {
        return null;
    }

    @Override
    public Object visitQuery(@Nonnull RelationalParser.QueryContext ctx) {
        if (queryCachingFlags.isEmpty()) {
            queryCachingFlags.add(Result.QueryCachingFlags.IS_DQL_STATEMENT);
        }
        if (ctx.ctes() != null) {
            visit(ctx.ctes());
        }
        ctx.queryExpressionBody().accept(this);
        if (ctx.continuation() != null) {
            ctx.continuation().accept(this);
        }
        return null;
    }

    @Override
    public Object visitContinuation(@Nonnull RelationalParser.ContinuationContext ctx) {
        return ctx.continuationAtom().accept(this);
    }

    @Override
    public RelationalExpression visitQueryOptions(@Nonnull RelationalParser.QueryOptionsContext ctx) {
        for (final var opt : ctx.queryOption()) {
            visit(opt);
        }
        return null;
    }

    @Override
    public Object visitQueryOption(@Nonnull RelationalParser.QueryOptionContext ctx) {
        try {
            if (ctx.NOCACHE() != null) {
                queryCachingFlags.add(Result.QueryCachingFlags.WITH_NO_CACHE_OPTION);
            }
            if (ctx.LOG() != null) {
                queryOptions.withOption(Options.Name.LOG_QUERY, true);
            }
            if (ctx.DRY() != null) {
                queryOptions.withOption(Options.Name.DRY_RUN, true);
            }
            if (ctx.CONTINUATION() != null) {
                queryOptions.withOption(Options.Name.CONTINUATIONS_CONTAIN_COMPILED_STATEMENTS, true);
            }
            return null;
        } catch (SQLException e) {
            throw ExceptionUtil.toRelationalException(e).toUncheckedWrappedException();
        }
    }

    @Override
    public Object visitDdlStatement(@Nonnull RelationalParser.DdlStatementContext ctx) {
        queryCachingFlags.add(Result.QueryCachingFlags.IS_DDL_STATEMENT);
        return visitChildren(ctx);
    }

    @Override
    public Object visitDmlStatement(@Nonnull RelationalParser.DmlStatementContext ctx) {
        queryCachingFlags.add(Result.QueryCachingFlags.IS_DML_STATEMENT);
        return visitChildren(ctx);
    }

    @Override
    public Object visitAdministrationStatement(@Nonnull RelationalParser.AdministrationStatementContext ctx) {
        queryCachingFlags.add(Result.QueryCachingFlags.IS_ADMIN_STATEMENT);
        return visitChildren(ctx);
    }

    @Override
    public Object visitUtilityStatement(@Nonnull RelationalParser.UtilityStatementContext ctx) {
        queryCachingFlags.add(Result.QueryCachingFlags.IS_UTILITY_STATEMENT);
        return visitChildren(ctx);
    }

    @Override
    public Void visitContinuationAtom(@Nonnull RelationalParser.ContinuationAtomContext ctx) {
        allowLiteralAddition = false;
        allowTokenAddition = false;
        if (ctx.bytesLiteral() != null) {
            final var continuation = ParseHelpers.parseBytes(ctx.bytesLiteral().getText());
            Assert.notNullUnchecked(continuation, ErrorCode.INVALID_CONTINUATION, "Illegal query with BEGIN continuation.");
            Assert.thatUnchecked(continuation.length != 0, ErrorCode.INVALID_CONTINUATION, "Illegal query with END continuation.");
            context.setContinuation(continuation);
            processScalarLiteral(continuation, ctx.getStart().getTokenIndex());
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
    public Void visitScalarFunctionCall(@Nonnull RelationalParser.ScalarFunctionCallContext ctx) {
        final var functionName = ctx.scalarFunctionName().getText();
        boolean skipFirstFunctionArgument = "JAVA_CALL".equals(SemanticAnalyzer.normalizeString(functionName, false));
        for (int i = 0; i < ctx.getChildCount(); i++) {
            final var child = Assert.notNullUnchecked(ctx.getChild(i));
            if (child == ctx.functionArgs()) {
                final var args = (RelationalParser.FunctionArgsContext) child;
                for (int j = 0; j < args.getChildCount(); j++) {
                    final var arg = args.getChild(j);
                    if (j == 0 && skipFirstFunctionArgument) {
                        sqlCanonicalizer.append(arg.getText()).append(" ");
                        hashFunction.putBytes(arg.getText().getBytes(StandardCharsets.UTF_8));
                    } else {
                        arg.accept(this);
                    }
                }
            } else {
                child.accept(this);
            }
        }
        return null;
    }

    @Override
    public Object visitPreparedStatementParameter(@Nonnull RelationalParser.PreparedStatementParameterContext ctx) {
        Object param;
        if (ctx.QUESTION() != null) {
            final int currentUnnamedParameterIndex = preparedStatementParameters.currentUnnamedParamIndex();
            param = preparedStatementParameters.nextUnnamedParamValue();
            if (param instanceof Array || param instanceof Struct) {
                allowLiteralAddition = false;
            }
            processUnnamedParameter(param,  currentUnnamedParameterIndex, ctx.getStart().getTokenIndex());
            if (param instanceof Array || param instanceof Struct) {
                allowLiteralAddition = true;
            }

            if (param instanceof Array) {
                allowTokenAddition = false;
                processArrayParameter((Array) param, currentUnnamedParameterIndex, null, ctx.getStart().getTokenIndex());
                allowTokenAddition = true;
            } else if (param instanceof Struct) {
                allowTokenAddition = false;
                processStructParameter((Struct) param, currentUnnamedParameterIndex, null, ctx.getStart().getTokenIndex());
                allowTokenAddition = true;
            }
        } else {
            // Note we preserve named parameters in canonical representation, otherwise we could mix up different queries
            // if we use '?' ubiquitously.
            // e.g. select * from t1 where col1 = ?P1 and col2 = ?P2
            //      select * from t1 where col1 = ?P2 and col2 = ?P1
            final var namedParameterContext = ctx.NAMED_PARAMETER();
            final var parameterName = namedParameterContext.getText().substring(1);
            param = preparedStatementParameters.namedParamValue(parameterName);
            if (param instanceof Array || param instanceof Struct) {
                allowLiteralAddition = false;
            }
            processNamedParameter(param, parameterName, namedParameterContext.getSymbol().getTokenIndex());
            if (param instanceof Array || param instanceof Struct) {
                allowLiteralAddition = true;
            }

            if (param instanceof Array) {
                allowTokenAddition = false;
                processArrayParameter((Array) param, null, parameterName, ctx.getStart().getTokenIndex());
                allowTokenAddition = true;
            } else if (param instanceof Struct) {
                allowTokenAddition = false;
                processStructParameter((Struct) param, null, parameterName, ctx.getStart().getTokenIndex());
                allowTokenAddition = true;
            }
        }

        return param;
    }

    @Override
    public Object visitInPredicate(@Nonnull RelationalParser.InPredicateContext ctx) {
        ctx.expressionAtom().accept(this);
        ctx.IN().accept(this);

        if (ctx.inList().preparedStatementParameter() != null) {
            visit(ctx.inList().preparedStatementParameter());
        } else {
            sqlCanonicalizer.append("( ");
            if (ParseHelpers.isConstant(ctx.inList().expressions())) {
                // todo (yhatem) we should prevent making the constant expressions
                //   contribute to the hash or the canonical query representation.
                context.startArrayLiteral();
                allowTokenAddition = false;
                sqlCanonicalizer.append("[ ");
                for (int i = 0; i < ctx.inList().expressions().expression().size(); i++) {
                    visit(ctx.inList().expressions().expression(i));
                }
                context.finishArrayLiteral(null, null, ctx.inList().getStart().getTokenIndex());
                allowTokenAddition = true;
                sqlCanonicalizer.append("] ");
            } else {
                final var size = ctx.inList().expressions().expression().size();
                for (int i = 0; i < size; i++) {
                    visit(ctx.inList().expressions().expression(i));
                    if (i < size - 1) {
                        sqlCanonicalizer.append(", ");
                    }
                }
            }
            sqlCanonicalizer.append(") ");
        }

        return null;
    }

    @Override
    public Object visitExecuteContinuationStatement(@Nonnull RelationalParser.ExecuteContinuationStatementContext ctx) {
        queryCachingFlags.add(Result.QueryCachingFlags.IS_EXECUTE_CONTINUATION_STATEMENT);
        queryCachingFlags.add(Result.QueryCachingFlags.WITH_NO_CACHE_OPTION);
        if (ctx.queryOptions() != null) {
            ctx.queryOptions().accept(this);
        }
        return ctx.packageBytes.accept(this);
    }

    private void processArrayParameter(@Nonnull final Array param, @Nullable Integer unnamedParameterIndex,
                                       @Nullable String parameterName, final int tokenIndex) {
        try {
            context.startArrayLiteral();
            try (ResultSet rs = param.getResultSet()) {
                int i = 0;
                while (rs.next()) {
                    processParameterValue(rs.getObject(2), unnamedParameterIndex, parameterName, i);
                    i++;
                }
            }
            context.finishArrayLiteral(unnamedParameterIndex, parameterName, tokenIndex);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void processStructParameter(@Nonnull final Struct param, @Nullable Integer unnamedParameterIndex,
                                        @Nullable String parameterName, final int tokenIndex) {
        try {
            context.startStructLiteral();
            Object[] attributes = param.getAttributes();
            for (int i = 0; i < attributes.length; i++) {
                processParameterValue(attributes[i], unnamedParameterIndex, parameterName, i);
            }
            final var resolvedType = SqlTypeSupport.structMetadataToRecordType(((RelationalStruct) param).getMetaData(), false);
            context.finishStructLiteral(resolvedType, unnamedParameterIndex, parameterName, tokenIndex);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void processParameterValue(@Nonnull final Object parameterValue,
                                       @Nullable Integer unnamedParameterIndex,
                                       @Nullable String parameterName,
                                       final int tokenIndex) {
        if (parameterValue instanceof Array) {
            processArrayParameter((Array) parameterValue, unnamedParameterIndex, parameterName, tokenIndex);
        } else if (parameterValue instanceof Struct) {
            processStructParameter((Struct) parameterValue, unnamedParameterIndex, parameterName, tokenIndex);
        } else {
            processScalarLiteral(parameterValue, tokenIndex);
        }
    }

    private void processScalarLiteral(@Nonnull final Object literal, final int tokenIndex) {
        processLiteral(literal, tokenIndex, null, null);
    }

    private void processUnnamedParameter(@Nonnull final Object literal, final int unnamedParameterIndex,
                                         final int tokenIndex) {
        processLiteral(literal, tokenIndex, unnamedParameterIndex, null);
    }

    private void processNamedParameter(@Nonnull final Object literal, @Nonnull final String parameterName,
                                       final int tokenIndex) {
        processLiteral(literal, tokenIndex, null, parameterName);
    }

    private void processLiteral(@Nonnull final Object literal, final int tokenIndex,
                                @Nullable final Integer unnamedParameterIndex, @Nullable final String parameterName) {
        if (allowLiteralAddition) {
            context.addLiteral(new QueryExecutionContext.OrderedLiteral(Type.any(), literal, unnamedParameterIndex, parameterName, tokenIndex));
        }
        if (allowTokenAddition) {
            final String canonicalName = parameterName == null ? "?" : "?" + parameterName;
            sqlCanonicalizer.append(canonicalName).append(" ");
            parameterHash.putInt(Objects.hash(canonicalName, literal));
        }
    }

    @Nonnull
    public static Result normalizeQuery(@Nonnull final PlanContext context, @Nonnull final String query, boolean caseSensitive,
                                        @Nonnull PlanHashable.PlanHashMode currentPlanHashMode) throws RelationalException {
        final var rootContext = context.getMetricsCollector().clock(RelationalMetric.RelationalEvent.LEX_PARSE, () -> QueryParser.parse(query).getRootContext());
        return context.getMetricsCollector().clock(RelationalMetric.RelationalEvent.NORMALIZE_QUERY,
                () -> normalizeAst(
                        context.getSchemaTemplate(), rootContext,
                        PreparedParams.copyOf(context.getPreparedStatementParameters()),
                        context.getUserVersion(),
                        context.getSchemaTemplate().getIndexEntriesAsBitset(context.getPlannerConfiguration().getReadableIndexes()),
                        caseSensitive,
                        currentPlanHashMode
                ));
    }

    @Nonnull
    @VisibleForTesting
    public static Result normalizeAst(@Nonnull final SchemaTemplate schemaTemplate,
                                      @Nonnull final RelationalParser.RootContext context,
                                      @Nonnull final PreparedParams preparedStatementParameters,
                                      int userVersion,
                                      @Nonnull final BitSet readableIndexes,
                                      boolean caseSensitive,
                                      @Nonnull final PlanHashable.PlanHashMode currentPlanHashMode) {
        final var astNormalizer = new AstNormalizer(preparedStatementParameters, caseSensitive, currentPlanHashMode);
        astNormalizer.visit(context);
        return new Result(
                schemaTemplate.getName(),
                QueryCacheKey.of(astNormalizer.getCanonicalSqlString(), astNormalizer.getHash(), schemaTemplate.getVersion(), readableIndexes, userVersion),
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
            IS_EXECUTE_CONTINUATION_STATEMENT,
            /**
             * user explicitly wants to avoid using plan cache with this query via {@code NOCACHE} option.
             */
            WITH_NO_CACHE_OPTION;
        }

        @Nonnull
        private final String schemaTemplateName;

        @Nonnull
        private final QueryCacheKey queryCacheKey;

        @Nonnull
        private final QueryExecutionContext queryExecutionParameters;

        @Nonnull
        private final ParseTree parseTree;

        @Nonnull
        private final Set<QueryCachingFlags> queryCachingFlags;

        @Nonnull
        private final Options queryOptions;

        public Result(@Nonnull final String schemaTemplateName,
                      @Nonnull final QueryCacheKey queryCacheKey,
                      @Nonnull final QueryExecutionContext queryExecutionParameters,
                      @Nonnull final ParseTree parseTree,
                      @Nonnull final Set<QueryCachingFlags> queryCachingFlags,
                      @Nonnull final Options queryOptions) {
            this.schemaTemplateName = schemaTemplateName;
            this.queryCacheKey = queryCacheKey;
            this.queryExecutionParameters = queryExecutionParameters;
            this.parseTree = parseTree;
            this.queryCachingFlags = queryCachingFlags;
            this.queryOptions = queryOptions;
        }

        public String getSchemaTemplateName() {
            return schemaTemplateName;
        }

        @Nonnull
        public QueryCacheKey getQueryCacheKey() {
            return queryCacheKey;
        }

        @Nonnull
        public QueryExecutionContext getQueryExecutionParameters() {
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
