/*
 * SyntaxErrorListener.java
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

import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;

import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.misc.Utils;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;

public class SyntaxErrorListener extends BaseErrorListener
{
    private final List<SyntaxError> syntaxErrors = new ArrayList<>();

    public static class SyntaxError
    {
        @Nonnull
        private final Recognizer<?, ?> recognizer;
        @Nonnull
        private final Object offendingSymbol;
        private final int line;
        private final int charPositionInLine;
        @Nonnull
        private final String message;
        @Nonnull
        private final RecognitionException recognitionException;

        SyntaxError(@Nonnull final Recognizer<?, ?> recognizer,
                    @Nonnull final Object offendingSymbol,
                    int line,
                    int charPositionInLine,
                    @Nonnull final RecognitionException recognitionException)
        {
            this.recognizer = recognizer;
            this.offendingSymbol = offendingSymbol;
            this.line = line;
            this.charPositionInLine = charPositionInLine;
            this.message = ParserUtils.underlineParsingError(recognizer, (Token) offendingSymbol, line, charPositionInLine);
            this.recognitionException = recognitionException;
        }

        public Recognizer<?, ?> getRecognizer()
        {
            return recognizer;
        }

        public Object getOffendingSymbol()
        {
            return offendingSymbol;
        }

        public int getLine()
        {
            return line;
        }

        public int getCharPositionInLine()
        {
            return charPositionInLine;
        }

        @Nonnull
        public String getMessage()
        {
            return message;
        }

        public RecognitionException getException()
        {
            return recognitionException;
        }

        RelationalException toRelationalException() {
            return new RelationalException("syntax error:\n" + message, ErrorCode.SYNTAX_ERROR);
        }
    }

    List<SyntaxError> getSyntaxErrors()
    {
        return syntaxErrors;
    }

    @Override
    public void syntaxError(Recognizer<?, ?> recognizer,
                            Object offendingSymbol,
                            int line, int charPositionInLine,
                            String msg, RecognitionException e)
    {
        syntaxErrors.add(new SyntaxError(recognizer, offendingSymbol, line, charPositionInLine, e));
    }

    @Override
    public String toString()
    {
        return Utils.join(syntaxErrors.iterator(), "\n");
    }
}
