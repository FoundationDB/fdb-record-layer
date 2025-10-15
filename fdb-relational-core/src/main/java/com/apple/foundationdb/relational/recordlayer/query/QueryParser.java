/*
 * QueryParser.java
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

import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.generated.RelationalLexer;
import com.apple.foundationdb.relational.generated.RelationalParser;
import com.apple.foundationdb.relational.generated.RelationalParserBaseVisitor;
import com.apple.foundationdb.relational.util.Assert;
import com.apple.foundationdb.relational.util.Environment;

import com.google.common.annotations.VisibleForTesting;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.BufferedTokenStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.Parser;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.TokenStream;
import org.antlr.v4.runtime.atn.ATNConfigSet;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.misc.Interval;
import org.antlr.v4.runtime.tree.ParseTree;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Locale;
import java.util.function.Consumer;
import java.util.function.Function;

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

        static <T> T withParseErrorHandling(@Nonnull final Function<ErrorStringifier, T> parsingRoutine) throws RelationalException {
            final var listener = new ErrorStringifier();
            final var result = parsingRoutine.apply(listener);
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
    }

    @Nonnull
    public static ParseTreeInfoImpl parse(@Nonnull final String query) throws RelationalException {
        final var tokenSource = new RelationalLexer(new CaseInsensitiveCharStream(query));
        final var parser = getParserInstance(new CommonTokenStream(tokenSource));

        final var rootContext = ErrorStringifier.withParseErrorHandling(listener -> {
            parser.removeErrorListeners();
            parser.addErrorListener(listener);
            return parser.root();
        });

        return ParseTreeInfoImpl.from(rootContext);
    }

    @Nonnull
    private static <P> P parse(@Nonnull final String query, @Nonnull final Consumer<CommonTokenStream> tokenConsumer,
                               @Nonnull final Function<RelationalParser, P> parse) {
        final var tokenSource = new RelationalLexer(new CaseInsensitiveCharStream(query));
        final var tokensStream = new CommonTokenStream(tokenSource);
        tokenConsumer.accept(tokensStream);
        final var parser = getParserInstance(tokensStream);

        try {
            return ErrorStringifier.withParseErrorHandling(listener -> {
                parser.removeErrorListeners();
                parser.addErrorListener(listener);
                return parse.apply(parser);
            });
        } catch (RelationalException e) {
            throw e.toUncheckedWrappedException();
        }
    }


    @Nonnull
    public static RelationalParser.SqlInvokedFunctionContext parseFunction(@Nonnull final String functionString) {
        // the routine here is assumed to start with CREATE,
        // however due to how the parser rules are structured,
        // parsing invoked function starts immediately after CREATE
        // therefore, to trigger it correctly, we'll remove the first (CREATE) token.
        return parse(functionString, BufferedTokenStream::consume, RelationalParser::sqlInvokedFunction);
    }

    @Nonnull
    public static RelationalParser.TempSqlInvokedFunctionContext parseTemporaryFunction(@Nonnull final String functionString) {
        return parse(functionString, ignored -> { }, RelationalParser::tempSqlInvokedFunction);
    }

    @Nonnull
    public static RelationalParser.QueryContext parseView(@Nonnull final String viewDefinition) {
        return parse(viewDefinition, ignored -> { } , RelationalParser::query);
    }

    private static final class PreparedParamsValidator extends RelationalParserBaseVisitor<Void> {
        @Override
        public Void visitPreparedStatementParameter(final RelationalParser.PreparedStatementParameterContext ctx) {
            Assert.failUnchecked(ErrorCode.SYNTAX_ERROR, "found prepared parameter(s) in SQL statement");
            return null;
        }
    }

    /**
     * visits the parse tree and throws if it encounters a prepared parameter.
     * @param context The parse tree of the query.
     */
    public static void validateNoPreparedParams(@Nonnull final ParseTree context) {
        final var validator = new PreparedParamsValidator();
        validator.visit(context);
    }

    @Nonnull
    private static RelationalParser getParserInstance(@Nonnull final TokenStream tokenStream) {
        final var parser = new RelationalParser(tokenStream);
        if (Environment.isDebug()) {
            parser.getInterpreter().setPredictionMode(PredictionMode.LL_EXACT_AMBIG_DETECTION);
        } else {
            parser.getInterpreter().setPredictionMode(PredictionMode.LL);
        }
        return parser;
    }
}
