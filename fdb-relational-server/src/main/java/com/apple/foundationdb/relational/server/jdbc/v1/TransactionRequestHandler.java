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

import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.jdbc.TypeConversion;
import com.apple.foundationdb.relational.jdbc.grpc.GrpcSQLExceptionUtil;
import com.apple.foundationdb.relational.jdbc.grpc.v1.CommitResponse;
import com.apple.foundationdb.relational.jdbc.grpc.v1.EnableAutoCommitResponse;
import com.apple.foundationdb.relational.jdbc.grpc.v1.InsertRequest;
import com.apple.foundationdb.relational.jdbc.grpc.v1.InsertResponse;
import com.apple.foundationdb.relational.jdbc.grpc.v1.RollbackResponse;
import com.apple.foundationdb.relational.jdbc.grpc.v1.StatementRequest;
import com.apple.foundationdb.relational.jdbc.grpc.v1.StatementResponse;
import com.apple.foundationdb.relational.jdbc.grpc.v1.TransactionalRequest;
import com.apple.foundationdb.relational.jdbc.grpc.v1.TransactionalResponse;
import com.apple.foundationdb.relational.server.FRL;
import com.apple.foundationdb.relational.server.TransactionalToken;
import com.google.protobuf.Any;
import com.google.rpc.Status;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;

/**
 * Handles client requests in transactional (autoCommit=off) mode.
 * This class is the server-side state that is maintained when the client enters transactional mode. It is created
 * once the client connection enters transactional mode (via handleAutoCommitOff call) and gets destroyed
 * when the client connection exits that mode or the connection is terminated.
 * In order to maintain a connection, SQLExceptions are passed back as payload. {@link StreamObserver#onError}
 * is only called in case of processing error and will terminate the connection.
 */
public class TransactionRequestHandler implements StreamObserver<TransactionalRequest> {
    private static final Logger logger = LoggerFactory.getLogger(TransactionRequestHandler.class);

    private final StreamObserver<TransactionalResponse> responseObserver;
    private final FRL frl;
    private TransactionalToken transactionalToken;

    public TransactionRequestHandler(final StreamObserver<TransactionalResponse> responseObserver, final FRL frl) {
        this.responseObserver = responseObserver;
        this.frl = frl;
    }

    @Override
    public void onNext(final TransactionalRequest transactionRequest) {
        TransactionalResponse.Builder responseBuilder = TransactionalResponse.newBuilder();
        try {
            if (transactionRequest.hasExecuteRequest()) {
                final StatementRequest request = transactionRequest.getExecuteRequest();
                final Options options = TypeConversion.fromProtobuf(request.getOptions());
                if (logger.isInfoEnabled()) {
                    logger.info(KeyValueLogMessage.build("Handling execute request")
                            .addKeyAndValue(LogMessageKeys.QUERY, request.getSql())
                            .toString());
                }
                if (transactionalToken == null || transactionalToken.expired()) {
                    transactionalToken = frl.createTransactionalToken(request.getDatabase(), request.getSchema(), options);
                }
                final FRL.Response response = frl.transactionalExecute(transactionalToken, request.getSql(),
                        request.getParameters().getParameterList(), options);

                StatementResponse.Builder statementResponseBuilder = StatementResponse.newBuilder();
                if (response.isQuery()) {
                    statementResponseBuilder.setResultSet(response.getResultSet());
                }
                if (response.isMutation()) {
                    statementResponseBuilder.setRowCount(response.getRowCount());
                }

                responseBuilder.setExecuteResponse(statementResponseBuilder);
            } else if (transactionRequest.hasInsertRequest()) {
                final InsertRequest request = transactionRequest.getInsertRequest();
                final Options options = TypeConversion.fromProtobuf(request.getOptions());
                if (logger.isInfoEnabled()) {
                    logger.info(KeyValueLogMessage.build("Handling insert request")
                            .addKeyAndValue(LogMessageKeys.RECORD_TYPE, request.getTableName())
                            .toString());
                }
                if (transactionalToken == null || transactionalToken.expired()) {
                    transactionalToken = frl.createTransactionalToken(request.getDatabase(), request.getSchema(), options);
                }
                int recordsInserted = frl.transactionalInsert(transactionalToken, request.getTableName(), TypeConversion.fromResultSetProtobuf(request.getDataResultSet()));
                responseBuilder.setInsertResponse(InsertResponse.newBuilder().setRowCount(recordsInserted));
            } else if (transactionRequest.hasCommitRequest()) {
                // handle commit
                logger.info("Handling commit request");
                frl.transactionalCommit(transactionalToken);
                responseBuilder.setCommitResponse(CommitResponse.newBuilder().build());
            } else if (transactionRequest.hasRollbackRequest()) {
                // handle rollback
                logger.info("Handling rollback request");
                frl.transactionalRollback(transactionalToken);
                responseBuilder.setRollbackResponse(RollbackResponse.newBuilder().build());
            } else if (transactionRequest.hasEnableAutoCommitRequest()) {
                logger.info("Enabling autoCommit");
                // we don't actually call setAutoCommit(false) until an operation happens, so if there is no token
                // there's no connection that needs updating
                if (transactionalToken != null) {
                    frl.enableAutoCommit(transactionalToken);
                }
                responseBuilder.setEnableAutoCommitResponse(EnableAutoCommitResponse.newBuilder().build());
            } else {
                throw new IllegalArgumentException("Unknown transactional request type: " + transactionRequest);
            }
            responseObserver.onNext(responseBuilder.build());
        } catch (SQLException e) {
            // SQL exceptions are returned as payload as the connection can stay open
            if (logger.isInfoEnabled()) {
                logger.info("Caught SQL exception: returning to client: {}", e.getMessage());
            }
            Status status = GrpcSQLExceptionUtil.create(e);
            responseBuilder.setErrorResponse(Any.pack(status));
            responseObserver.onNext(responseBuilder.build());
        } catch (RuntimeException e) {
            logger.warn("Caught unknown exception", e);
            throw JDBCService.handleUncaughtException(e);
        }
    }

    @Override
    public void onError(final Throwable throwable) {
        logger.warn("executeInTransaction: onError called", throwable);
        closeConnectionIfExists();
    }

    @Override
    public void onCompleted() {
        // Close the client connection
        responseObserver.onCompleted();
        closeConnectionIfExists();
    }

    private void closeConnectionIfExists() {
        try {
            frl.transactionalClose(transactionalToken);
        } catch (SQLException e) {
            if (logger.isWarnEnabled()) {
                logger.warn(KeyValueLogMessage.build("Error while closing transactional connection").toString(), e);
            }
        }
    }
}
