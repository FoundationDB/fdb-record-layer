/*
 * TransactionRequestHandler.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.server.jdbc.v1;

import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.jdbc.grpc.v1.CommitResponse;
import com.apple.foundationdb.relational.jdbc.grpc.v1.RollbackResponse;
import com.apple.foundationdb.relational.jdbc.grpc.v1.StatementRequest;
import com.apple.foundationdb.relational.jdbc.grpc.v1.StatementResponse;
import com.apple.foundationdb.relational.jdbc.grpc.v1.TransactionalRequest;
import com.apple.foundationdb.relational.jdbc.grpc.v1.TransactionalResponse;
import com.apple.foundationdb.relational.server.FRL;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;

/**
 * Handles client requests in transactional (autoCommit=off) mode.
 * Note: This class can only handle EXECUTE, COMMIT and ROLLBACK requests at this time. Additional message types
 * (such as INSERT and UPDATE) may be necessary.
 * This class is the server-side state that is maintained when the client enters transactional mode. It is created
 * once the client connection enters transactional mode (via handleAutoCommitOff call) and gets destroyed
 * when the client connection exits that mode or the connection is terminated.
 */
public class TransactionRequestHandler implements StreamObserver<TransactionalRequest> {
    private static final Logger logger = LoggerFactory.getLogger(TransactionRequestHandler.class);

    private StreamObserver<TransactionalResponse> responseObserver;
    private FRL frl;

    public TransactionRequestHandler(final StreamObserver<TransactionalResponse> responseObserver, final FRL frl) {
        this.responseObserver = responseObserver;
        this.frl = frl;
    }

    @Override
    public void onNext(final TransactionalRequest transactionRequest) {
        TransactionalResponse.Builder responseBuilder = TransactionalResponse.newBuilder();
        if (transactionRequest.hasExecuteRequest()) {
            final StatementRequest request = transactionRequest.getExecuteRequest();
            logger.info("Handling execute request: " + request.getSql());

            try {
                final FRL.Response response = frl.transactionalExecute(request.getDatabase(), request.getSchema(), request.getSql(),
                        request.getParameters().getParameterList(),
                        Options.builder()
                                .withOption(Options.Name.MAX_ROWS, request.getOptions().getMaxRows()).build());

                StatementResponse.Builder statementResponseBuilder = StatementResponse.newBuilder()
                        .setRowCount(response.getRowCount());

                responseBuilder.setExecuteResponse(statementResponseBuilder);
            } catch (SQLException | RuntimeException e) {
                logger.warn("Execute: Error caught", e);
                responseBuilder.setExecuteResponse(StatementResponse.newBuilder()
                        .setRowCount(0)
                        .setErrorMessage(e.getMessage())
                );
            }
        } else if (transactionRequest.hasCommitRequest()) {
            // handle commit
            logger.info("Handling commit request");
            try {
                frl.transactionalCommit();
                responseBuilder.setCommitResponse(CommitResponse.newBuilder().build());
            }  catch (SQLException | RuntimeException e) {
                logger.warn("Commit: Error caught", e);
                responseBuilder.setCommitResponse(CommitResponse.newBuilder()
                        .setErrorMessage(e.getMessage())
                        .build());
            }
        } else if (transactionRequest.hasRollbackRequest()) {
            // handle rollback
            logger.info("Handling rollback request");
            try {
                frl.transactionalRollback();
                responseBuilder.setRollbackResponse(RollbackResponse.newBuilder().build());
            }  catch (SQLException | RuntimeException e) {
                logger.warn("Rollback: Error caught", e);
                responseBuilder.setRollbackResponse(RollbackResponse.newBuilder()
                        .setErrorMessage(e.getMessage())
                        .build());
            }
        } else {
            throw new IllegalArgumentException("Unknown transactional request type in" + transactionRequest);
        }
        responseObserver.onNext(responseBuilder.build());
    }

    @Override
    public void onError(final Throwable throwable) {
        logger.warn("executeInTransaction: onError called", throwable);
        // todo: Rollback uncommitted stuff?
    }

    @Override
    public void onCompleted() {
        // todo: If uncommitted stuff, rollback

        // Close the client connection
        responseObserver.onCompleted();
    }
}
