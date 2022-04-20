/*
 * DdlStatement.java
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

package com.apple.foundationdb.relational.api.ddl;

import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.generated.DdlLexer;
import com.apple.foundationdb.relational.generated.DdlParser;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ConsoleErrorListener;
import org.antlr.v4.runtime.InputMismatchException;
import org.antlr.v4.runtime.NoViableAltException;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.TokenSource;
import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.antlr.v4.runtime.tree.ParseTreeWalker;

import java.net.URI;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

/**
 * Class responsible for processing DDL statements (e.g. parsing, building the templates, and so forth).
 * <p>
 * TODO(bfines) eventually this will need to be merged with the more-general purpose SQL parsing structures.
 */
public class DdlStatement implements AutoCloseable {
    private final DdlConnection conn;
    private final ConstantActionFactory constantActionFactory;
    //TODO(bfines) eventually this should probably be replaced by some kind of Planning process
    private final DdlQueryFactory queryFactory;
    private final URI rootUri;

    private List<RelationalResultSet> resultSets = new LinkedList<>();

    private final Transaction txn;

    public DdlStatement(Transaction txn,
                        DdlConnection conn,
                        ConstantActionFactory constantActionFactory,
                        DdlQueryFactory ddlQueryFactory) {
        this(txn, conn, constantActionFactory, ddlQueryFactory, URI.create("/"));
    }

    public DdlStatement(Transaction txn,
                        DdlConnection conn,
                        ConstantActionFactory constantActionFactory,
                        DdlQueryFactory ddlQueryFactory,
                        URI rootUri) {
        this.rootUri = rootUri;
        this.conn = conn;
        this.txn = txn;
        this.constantActionFactory = constantActionFactory;
        this.queryFactory = ddlQueryFactory;
    }

    @Override
    public void close() {
        //no-op
    }

    //TODO(bfines) should we standardize on either SQLException or RelationalException, but not both?

    /**
     * Similar to {@link java.sql.Statement#execute(String)}, explicitly for DDL statements.
     *
     * @param statement the statement to execute
     * @return true if there is a {@code RelationalResultSet} in the result sequence.
     * @throws RelationalException if something goes wrong
     */
    @SuppressWarnings("PMD.PreserveStackTrace") //suppressed because we don't want the ANTLR parser trace
    public boolean execute(String statement) throws RelationalException {
        final TokenSource ddlLexer = new DdlLexer(CharStreams.fromString(statement));
        CommonTokenStream tokens = new CommonTokenStream(ddlLexer);
        DdlParser parser = new DdlParser(tokens);
        parser.removeErrorListener(ConsoleErrorListener.INSTANCE);
        ParseTreeWalker walker = new ParseTreeWalker();
        final DdlListener listener = new DdlListener(rootUri, constantActionFactory, queryFactory, conn::getCurrentDatabase);
        try {
            walker.walk(listener, parser.statements());
        } catch (ParseCancellationException pce) {
            final Throwable cause = pce.getCause();
            if (cause instanceof InputMismatchException) {
                Token badToken = ((InputMismatchException) cause).getOffendingToken();
                throw new RelationalException("Syntax error: unexpected token at position " + badToken.getTokenIndex() + ": " + badToken.getText(), ErrorCode.SYNTAX_ERROR);
            } else if (cause instanceof NoViableAltException) {
                Token badToken = ((NoViableAltException) cause).getOffendingToken();
                throw new RelationalException("Syntax error: unexpected token at position " + badToken.getTokenIndex() + ": " + badToken.getText(), ErrorCode.SYNTAX_ERROR);
            } else if (cause instanceof RelationalException) {
                throw (RelationalException) cause;
            } else {
                throw new RelationalException("Unexpected syntax error:" + cause.getMessage(), ErrorCode.SYNTAX_ERROR);
            }
        }

        return processStatements(listener.getParsedActions());
    }

    private boolean processStatements(Queue<DdlPreparedAction<?>> actions) throws RelationalException {

        DdlPreparedAction<?> nextAction;
        while ((nextAction = actions.poll()) != null) {
            Object o = nextAction.executeAction(txn);
            if (o instanceof RelationalResultSet) {
                resultSets.add((RelationalResultSet) o);
            }
        }

        return !resultSets.isEmpty();
    }

    public RelationalResultSet getNextResultSet() {
        return resultSets.remove(0);
    }
}
