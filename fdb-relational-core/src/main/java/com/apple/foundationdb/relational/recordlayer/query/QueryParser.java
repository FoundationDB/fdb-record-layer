/*
 * QueryParser.java
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

import com.apple.foundationdb.annotation.API;

import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.generated.RelationalLexer;
import com.apple.foundationdb.relational.generated.RelationalParser;
import com.apple.foundationdb.relational.generated.RelationalParserBaseVisitor;
import com.apple.foundationdb.relational.util.Environment;

import com.google.common.annotations.VisibleForTesting;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.Parser;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.atn.ATNConfigSet;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.misc.Interval;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Locale;

/**
 * This parses a given SQL statement and returns an abstract syntax tree.
 */
@API(API.Status.EXPERIMENTAL)
public class QueryParser {

    @VisibleForTesting
    public static class ErrorStringifier extends BaseErrorListener {
        @Nonnull
        private final List<String> syntaxErrors;

        @Nonnull
        private final List<String> ambiguityErrors;

        @VisibleForTesting
        public ErrorStringifier() {
            syntaxErrors = new ArrayList<>();
            ambiguityErrors = new ArrayList<>();
        }

        @Nonnull
        List<String> getSyntaxErrors() {
            return syntaxErrors;
        }

        @Nonnull
        List<String> getAmbiguityErrors() {
            return ambiguityErrors;
        }

        @Override
        public void syntaxError(Recognizer<?, ?> recognizer,
                                Object offendingSymbol,
                                int line, int charPositionInLine,
                                String msg, RecognitionException e) {
            syntaxErrors.add(ParseHelpers.underlineParsingError(recognizer, (Token)offendingSymbol, line, charPositionInLine));
        }

        @Override
        public void reportAmbiguity(Parser recognizer,
                                    DFA dfa,
                                    int startIndex,
                                    int stopIndex,
                                    boolean exact,
                                    BitSet ambigAlts,
                                    ATNConfigSet configs) {
            ambiguityErrors.add(String.format(Locale.ROOT, "Ambiguity: %s, Exact: %b",
                    recognizer.getInputStream().getText(Interval.of(startIndex, recognizer.getInputStream().size() - 1)), exact));
        }
    }

    @Nonnull
    public static ParseTreeInfoImpl parse(@Nonnull final String query) throws RelationalException {
        final var tokenSource = new RelationalLexer(new CaseInsensitiveCharStream(query));
        final var parser = new RelationalParser(new CommonTokenStream(tokenSource));
        setInterpreterMode(parser);
        parser.removeErrorListeners();
        final var listener = new ErrorStringifier();
        parser.addErrorListener(listener);
        RelationalParser.RootContext rootContext = parser.root();

        if (Environment.isDebug() && !listener.getAmbiguityErrors().isEmpty()) {
            var errorMessage = String.join(", ", listener.getAmbiguityErrors());
            if (!listener.getSyntaxErrors().isEmpty()) {
                errorMessage = errorMessage.concat(listener.getAmbiguityErrors().get(0));
            }
            throw new RelationalException(errorMessage, ErrorCode.INTERNAL_ERROR);
        }

        if (!listener.getSyntaxErrors().isEmpty()) {
            throw new RelationalException("syntax error:\n" + listener.getSyntaxErrors().get(0), ErrorCode.SYNTAX_ERROR);
        }

        return ParseTreeInfoImpl.from(rootContext);
    }

    @Nonnull
    public static RelationalParser.SqlInvokedFunctionContext parseFunction(@Nonnull final String functionString) throws RelationalException {
        final var tokenSource = new RelationalLexer(new CaseInsensitiveCharStream(functionString));
        // the routine here is assumed to start with CREATE,
        // however due to how the parser rules are structured,
        // parsing invoked function starts immediately after CREATE
        // therefore, to trigger it correctly, we'll remove the first (CREATE) token.
        final var tokensStream = new CommonTokenStream(tokenSource);
        tokensStream.consume();
        final var parser = new RelationalParser(tokensStream);
        setInterpreterMode(parser);
        parser.removeErrorListeners();
        final var listener = new QueryParser.ErrorStringifier();
        parser.addErrorListener(listener);
        var result = parser.sqlInvokedFunction();

        if (Environment.isDebug() && !listener.getAmbiguityErrors().isEmpty()) {
            var errorMessage = String.join(", ", listener.getAmbiguityErrors());
            if (!listener.getSyntaxErrors().isEmpty()) {
                errorMessage = errorMessage.concat(listener.getAmbiguityErrors().get(0));
            }
            throw new RelationalException(errorMessage, ErrorCode.INTERNAL_ERROR);
        }

        if (!listener.getSyntaxErrors().isEmpty()) {
            throw new RelationalException("syntax error:\n" + listener.getSyntaxErrors().get(0), ErrorCode.SYNTAX_ERROR);
        }

        return result;
    }

    private static final class PreparedParamsReplacer extends RelationalParserBaseVisitor<Void> {

        @Nonnull
        private final StringBuilder sqlBuilder;

        @Nonnull
        private final PreparedParams preparedStatementParameters;

        private PreparedParamsReplacer(@Nonnull final PreparedParams preparedStatementParameters) {
            this.preparedStatementParameters = preparedStatementParameters;
            sqlBuilder = new StringBuilder();
        }

        @Override
        public Void visitPreparedStatementParameter(@Nonnull RelationalParser.PreparedStatementParameterContext ctx) {
            Object param;
            if (ctx.QUESTION() != null) {
                param = preparedStatementParameters.nextUnnamedParamValue();
                sqlBuilder.append(PreparedParams.prettyPrintParam(param)).append(" ");
            } else {
                // Note we preserve named parameters in canonical representation, otherwise we could mix up different queries
                // if we use '?' ubiquitously.
                // e.g. select * from t1 where col1 = ?P1 and col2 = ?P2
                //      select * from t1 where col1 = ?P2 and col2 = ?P1
                final var namedParameterContext = ctx.NAMED_PARAMETER();
                final var parameterName = namedParameterContext.getText().substring(1);
                param = preparedStatementParameters.namedParamValue(parameterName);
                sqlBuilder.append("?").append(parameterName).append(":").append(PreparedParams.prettyPrintParam(param)).append(" ");
            }
            return null;
        }

        @Override
        public Void visitTerminal(@Nonnull TerminalNode node) {
            if (node.getSymbol().getType() != Token.EOF) {
                sqlBuilder.append(node.getText()).append(" ");
            }
            return null;
        }

        @Nonnull
        String getQuery() {
            return sqlBuilder.toString();
        }
    }

    @Nonnull
    public static String replacePreparedParams(@Nonnull final ParseTree context,
                                               @Nonnull final PreparedParams preparedParams) {
        final var replacer = new PreparedParamsReplacer(preparedParams);
        replacer.visit(context);
        return replacer.getQuery();
    }

    private static void setInterpreterMode(@Nonnull final RelationalParser parser) {
        if (Environment.isDebug()) {
            parser.getInterpreter().setPredictionMode(PredictionMode.LL_EXACT_AMBIG_DETECTION);
        } else {
            parser.getInterpreter().setPredictionMode(PredictionMode.LL);
        }
    }
}
