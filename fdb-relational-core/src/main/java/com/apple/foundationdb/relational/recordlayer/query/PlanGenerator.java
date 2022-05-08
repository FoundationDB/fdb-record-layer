/*
 * PlanGenerator.java
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

package com.apple.foundationdb.relational.recordlayer.query;

import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordStoreState;
import com.apple.foundationdb.record.metadata.MetaDataException;
import com.apple.foundationdb.record.query.IndexQueryabilityFilter;
import com.apple.foundationdb.record.query.ParameterRelationshipGraph;
import com.apple.foundationdb.record.query.plan.cascades.CascadesPlanner;
import com.apple.foundationdb.record.query.plan.cascades.GroupExpressionRef;
import com.apple.foundationdb.record.query.plan.cascades.ParserContext;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.Scopes;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalSortExpression;
import com.apple.foundationdb.record.query.plan.cascades.typing.TypeRepository;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.UncheckedRelationalException;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.generated.RelationalLexer;
import com.apple.foundationdb.relational.generated.RelationalParser;
import com.apple.foundationdb.relational.recordlayer.utils.Assert;

import com.google.common.base.VerifyException;
import org.antlr.v4.runtime.CommonTokenStream;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Optional;
import java.util.function.Consumer;

import javax.annotation.Nonnull;

/**
 * This class contains a set of utility methods that helps in parsing a query, generating continuation, a logical
 * plan, and physical execution plan.
 */
public final class PlanGenerator {

    /**
     * Parses a query generating an equivalent abstract syntax tree.
     *
     * @param query The query.
     * @return The abstract syntax tree.
     * @throws RelationalException if something goes wrong.
     */
    @Nonnull
    public static RelationalParser.RootContext parseQuery(@Nonnull final String query) throws RelationalException {
        final RelationalLexer tokenSource = new RelationalLexer(new CaseInsensitiveCharStream(query));
        final RelationalParser parser = new RelationalParser(new CommonTokenStream(tokenSource));
        SyntaxErrorListener listener = new SyntaxErrorListener();
        parser.addErrorListener(listener);
        RelationalParser.RootContext rootContext = parser.root();
        if (!listener.getSyntaxErrors().isEmpty()) {
            throw listener.getSyntaxErrors().get(0).toRelationalException();
        }
        return rootContext;
    }

    /**
     * Parses a query and generates an equivalent logical plan.
     *
     * @param query         The query.
     * @param metaData      The record store metadata.
     * @param storeState    The record store state.
     * @param postProcessor A post-processing hook that is activated after the generation of the logical plan.
     * @return The logical plan and continuation of the query.
     * @throws RelationalException if something goes wrong.
     */
    @Nonnull
    public static Pair<RelationalExpression, byte[]> generateLogicalPlan(@Nonnull final String query,
                                                                         @Nonnull final RecordMetaData metaData,
                                                                         @Nonnull final RecordStoreState storeState,
                                                                         @Nonnull final Consumer<AstVisitor> postProcessor) throws RelationalException {
        return generateLogicalPlan(parseQuery(query), query, metaData, storeState, postProcessor);
    }

    /**
     * Parses a query and generates an equivalent logical plan.
     *
     * @param ast           The abstract syntax tree.
     * @param query         The query string, required for logging.
     * @param metaData      The record store metadata.
     * @param storeState    The record store state.
     * @param postProcessor A post-processing hook that is activated after the generation of the logical plan.
     * @return The logical plan and continuation of the query.
     * @throws RelationalException if something goes wrong.
     */
    @Nonnull
    public static Pair<RelationalExpression, byte[]> generateLogicalPlan(@Nonnull final RelationalParser.RootContext ast,
                                                                         @Nonnull final String query,
                                                                         @Nonnull final RecordMetaData metaData,
                                                                         @Nonnull final RecordStoreState storeState,
                                                                         @Nonnull final Consumer<AstVisitor> postProcessor) throws RelationalException {
        final ParserContext astContext = new ParserContext(new Scopes(), TypeRepository.newBuilder(), metaData, storeState);
        final AstVisitor astWalker = new AstVisitor(astContext);
        try {
            final Object maybePlan = astWalker.visit(ast);
            Assert.that(maybePlan instanceof RelationalExpression, String.format("Could not generate a logical plan for query '%s'", query));
            postProcessor.accept(astWalker);
            return new ImmutablePair<>((RelationalExpression) maybePlan, astWalker.getContinuation());
        } catch (UncheckedRelationalException uve) {
            throw uve.unwrap();
        } catch (MetaDataException mde) {
            // we need a better way to pass-thru / translate errors codes between record layer and Relational as SQL exceptions
            throw new RelationalException(mde.getMessage(), ErrorCode.SYNTAX_OR_ACCESS_VIOLATION, mde);
        } catch (VerifyException ve) {
            throw new RelationalException(ve.getMessage(), ErrorCode.INTERNAL_ERROR, ve);
        }
    }

    /**
     * Record(  Record(String,Int) as field1,
     *          Int as field2);
     *
     * Record(String,Int) <--- constructed later
     *
     * TypeRepository
     * Record(  Record(String,Int) as field1,
     *          Int as field2);                  Foo
     *
     * Record(String,Int)                        __type__field1
     */


    /**
     * Parses a query, generates an equivalent logical plan and calls the planner to generate an execution plan.
     *
     * @param query      The query string.
     * @param metaData   The record store metadata.
     * @param storeState The record store state.
     * @return The execution plan and continuation of the query.
     * @throws RelationalException if something goes wrong.
     */
    @Nonnull
    public static Pair<RecordQueryPlan, byte[]> generatePlan(@Nonnull final String query,
                                                             @Nonnull final RecordMetaData metaData,
                                                             @Nonnull final RecordStoreState storeState) throws RelationalException {
        final CascadesPlanner planner = new CascadesPlanner(metaData, storeState);
        final Collection<String> recordTypeNames = new LinkedHashSet<>();
        final RelationalParser.RootContext ast = parseQuery(query);

        // need to do this step, so we can populate the record type names, and get the continuation (if any).
        final byte[] continuation = generateLogicalPlan(ast, query, metaData, storeState, astWalker -> recordTypeNames.addAll(astWalker.getFilteredRecords())).getRight();

        try {
            return new ImmutablePair<>(planner.planGraph(
                    () -> {
                        final RelationalExpression relationalExpression;
                        try {
                            relationalExpression = generateLogicalPlan(ast, query, metaData, storeState, astWalker -> recordTypeNames.addAll(astWalker.getFilteredRecords())).getLeft();
                        } catch (RelationalException e) {
                            throw e.toUncheckedWrappedException();
                        }
                        final Quantifier qun = Quantifier.forEach(GroupExpressionRef.of(relationalExpression));
                        return GroupExpressionRef.of(new LogicalSortExpression(null, false, qun));
                    },
                    Optional.ofNullable(recordTypeNames.isEmpty() ? null : recordTypeNames),
                    Optional.empty(),
                    IndexQueryabilityFilter.TRUE,
                    false, ParameterRelationshipGraph.empty()), continuation);
        } catch (UncheckedRelationalException uve) {
            throw uve.unwrap();
        }
    }

    private PlanGenerator() {
    }

}
