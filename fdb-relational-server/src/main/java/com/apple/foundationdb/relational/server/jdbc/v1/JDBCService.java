/*
 * JDBCService.java
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

package com.apple.foundationdb.relational.server.jdbc.v1;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.jdbc.TypeConversion;
import com.apple.foundationdb.relational.jdbc.grpc.GrpcSQLExceptionUtil;
import com.apple.foundationdb.relational.jdbc.grpc.v1.DatabaseMetaDataRequest;
import com.apple.foundationdb.relational.jdbc.grpc.v1.DatabaseMetaDataResponse;
import com.apple.foundationdb.relational.jdbc.grpc.v1.GetRequest;
import com.apple.foundationdb.relational.jdbc.grpc.v1.GetResponse;
import com.apple.foundationdb.relational.jdbc.grpc.v1.InsertRequest;
import com.apple.foundationdb.relational.jdbc.grpc.v1.InsertResponse;
import com.apple.foundationdb.relational.jdbc.grpc.v1.JDBCServiceGrpc;
import com.apple.foundationdb.relational.jdbc.grpc.v1.ResultSet;
import com.apple.foundationdb.relational.jdbc.grpc.v1.ScanRequest;
import com.apple.foundationdb.relational.jdbc.grpc.v1.ScanResponse;
import com.apple.foundationdb.relational.jdbc.grpc.v1.StatementRequest;
import com.apple.foundationdb.relational.jdbc.grpc.v1.StatementResponse;
import com.apple.foundationdb.relational.jdbc.grpc.v1.TransactionalRequest;
import com.apple.foundationdb.relational.jdbc.grpc.v1.TransactionalResponse;
import com.apple.foundationdb.relational.server.FRL;
import com.apple.foundationdb.relational.util.BuildVersion;
import com.google.common.annotations.VisibleForTesting;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.StatusProto;
import io.grpc.stub.StreamObserver;

import java.sql.SQLException;

/**
 * Field the Relational JDBC Service.
 */
// If you add a method, be sure to catch RuntimeExceptions; doing it here is cleaner
// than trying to do it in some global server interceptor. See comment in RelationalServer
// where we register grpc Services on the Server instance.
@API(API.Status.EXPERIMENTAL)
public class JDBCService extends JDBCServiceGrpc.JDBCServiceImplBase {
    private final FRL frl;

    public JDBCService(FRL frl) {
        this.frl = frl;
    }

    @Override
    public void getMetaData(DatabaseMetaDataRequest request,
                            StreamObserver<DatabaseMetaDataResponse> responseObserver) {
        // Of note, there is no 'RelationalDatabaseMetaData' implementation on the server-side
        // currently -- just an Interface -- and there probably won't be; it is not needed. An
        // implementation is needed on the JDBC client-side. Here we are passing the client info
        // that can be used on the client-side in its implementation of RelationalDatabaseMetaData.
        // We'll pull from wherever we need to.
        DatabaseMetaDataResponse databaseMetaDataResponse = DatabaseMetaDataResponse.newBuilder()
                .setDatabaseProductVersion(BuildVersion.getInstance().getVersion())
                .setUrl(BuildVersion.getInstance().getURL())
                .build();
        responseObserver.onNext(databaseMetaDataResponse);
        responseObserver.onCompleted();
    }

    @Override
    public void execute(StatementRequest request, StreamObserver<StatementResponse> responseObserver) {
        if (!checkStatementRequest(request, responseObserver)) {
            return;
        }
        try {
            StatementResponse.Builder statementResponseBuilder = StatementResponse.newBuilder();
            FRL.Response response = this.frl.execute(request.getDatabase(), request.getSchema(), request.getSql(),
                    request.hasParameters() ? request.getParameters().getParameterList() : null, TypeConversion.fromProtobuf(request.getOptions()));
            if (response.isQuery()) {
                // Setting row count like this might not be right... It is for updates. Might have to do something
                // better than this count.
                ResultSet rs = response.getResultSet();
                statementResponseBuilder.setRowCount(rs.getRowCount());
                statementResponseBuilder.setResultSet(rs);
            } else {
                //this is an update statement, so just set the row count directly and leave off the result set
                statementResponseBuilder.setRowCount(response.getRowCount());
            }
            responseObserver.onNext(statementResponseBuilder.build());
            responseObserver.onCompleted();
        } catch (SQLException e) {
            System.out.println("WHOOOOPS");
            e.printStackTrace();
            responseObserver.onError(StatusProto.toStatusRuntimeException(GrpcSQLExceptionUtil.create(e)));
        } catch (RuntimeException e) {
            System.out.println("WHOOOOPSY DAISY");
            e.printStackTrace();
            throw handleUncaughtException(e);
        }
    }

    /**
     * This is called when the client connection enters "autoCommit=off" state.
     * Requests coming in after that point will be accepted through the returned TransactionRequestHandler,
     * while responses will be sent back through the responseObserver.
     * This stream will remain open for the duration of the client being in autoCommit=off mode.
     *
     * @param responseObserver the stream to send transactional responses to
     * @return the stream that can handle transactional requests
     */
    @Override
    public StreamObserver<TransactionalRequest> handleAutoCommitOff(final StreamObserver<TransactionalResponse> responseObserver) {
        return new TransactionRequestHandler(responseObserver, frl);
    }

    @Override
    public void update(StatementRequest request, StreamObserver<StatementResponse> responseObserver) {
        if (!checkStatementRequest(request, responseObserver)) {
            return;
        }
        try {
            int rowCount = this.frl.update(request.getDatabase(), request.getSchema(), request.getSql(), TypeConversion.fromProtobuf(request.getOptions()));
            StatementResponse statementResponse = StatementResponse.newBuilder().setRowCount(rowCount).build();
            responseObserver.onNext(statementResponse);
            responseObserver.onCompleted();
        } catch (SQLException e) {
            responseObserver.onError(StatusProto.toStatusRuntimeException(GrpcSQLExceptionUtil.create(e)));
        } catch (RuntimeException e) {
            throw handleUncaughtException(e);
        }
    }

    /**
     * Check the StatementRequest is wholesome.
     */
    @VisibleForTesting
    static boolean checkStatementRequest(StatementRequest request, StreamObserver<?> responseObserver) {
        if (!request.hasDatabase() || request.getDatabase().isEmpty()) {
            responseObserver.onError(createStatusRuntimeException("Empty database name"));
            return false;
        }
        if (!request.hasSchema() || request.getSchema().isEmpty()) {
            responseObserver.onError(createStatusRuntimeException("Empty schema name"));
            return false;
        }
        if (!request.hasSql() || request.getSql().isEmpty()) {
            responseObserver.onError(createStatusRuntimeException("Empty sql statement"));
            return false;
        }
        return true;
    }

    @Override
    public void insert(InsertRequest request, StreamObserver<InsertResponse> responseObserver) {
        if (!checkInsertRequest(request, responseObserver)) {
            return;
        }
        try {
            int rowCount = this.frl.insert(request.getDatabase(), request.getSchema(), request.getTableName(),
                    TypeConversion.fromResultSetProtobuf(request.getDataResultSet()), TypeConversion.fromProtobuf(request.getOptions()));
            InsertResponse insertResponse = InsertResponse.newBuilder().setRowCount(rowCount).build();
            responseObserver.onNext(insertResponse);
            responseObserver.onCompleted();
        } catch (SQLException e) {
            responseObserver.onError(StatusProto.toStatusRuntimeException(GrpcSQLExceptionUtil.create(e)));
        } catch (RuntimeException e) {
            throw handleUncaughtException(e);
        }
    }

    private static boolean checkInsertRequest(InsertRequest request, StreamObserver<InsertResponse> responseObserver) {
        if (!request.hasDatabase() || request.getDatabase().isEmpty()) {
            responseObserver.onError(createStatusRuntimeException("Empty database name"));
            return false;
        }
        if (!request.hasSchema() || request.getSchema().isEmpty()) {
            responseObserver.onError(createStatusRuntimeException("Empty schema name"));
            return false;
        }
        if (!request.hasTableName() || request.getTableName().isEmpty()) {
            responseObserver.onError(createStatusRuntimeException("Empty table name"));
            return false;
        }
        if (request.getDataResultSet().getRowCount() <= 0) {
            responseObserver.onError(createStatusRuntimeException("No data in insert"));
            return false;
        }
        return true;
    }

    @Override
    public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
        if (!checkGetRequest(request, responseObserver)) {
            return;
        }
        try (RelationalResultSet rs = frl.get(request.getDatabase(), request.getSchema(), request.getTableName(),
                    TypeConversion.fromProtobuf(request.getKeySet()), TypeConversion.fromProtobuf(request.getOptions()))) {
            GetResponse getResponse = GetResponse.newBuilder().setResultSet(TypeConversion.toProtobuf(rs)).build();
            responseObserver.onNext(getResponse);
            responseObserver.onCompleted();
        } catch (SQLException e) {
            responseObserver.onError(StatusProto.toStatusRuntimeException(GrpcSQLExceptionUtil.create(e)));
        } catch (RuntimeException e) {
            throw handleUncaughtException(e);
        }
    }

    /**
     * Make up a StatusRuntimeException with an {@link Status#INVALID_ARGUMENT} Status.
     * @param invalidStr String to use in Status description.
     * @return {@link Status#INVALID_ARGUMENT} StatusRuntimeException.
     */
    private static StatusRuntimeException createStatusRuntimeException(String invalidStr) {
        return Status.INVALID_ARGUMENT.withDescription(invalidStr).asRuntimeException();
    }

    private static boolean checkGetRequest(GetRequest request, StreamObserver<GetResponse> responseObserver) {
        if (!request.hasDatabase() || request.getDatabase().isEmpty()) {
            responseObserver.onError(createStatusRuntimeException("Empty database name"));
            return false;
        }
        if (!request.hasSchema() || request.getSchema().isEmpty()) {
            responseObserver.onError(createStatusRuntimeException("Empty schema name"));
            return false;
        }
        if (!request.hasTableName() || request.getTableName().isEmpty()) {
            responseObserver.onError(createStatusRuntimeException("Empty table name"));
            return false;
        }
        if (!request.hasKeySet()) {
            responseObserver.onError(createStatusRuntimeException("Has no keyset"));
            return false;
        }
        return true;
    }

    @Override
    public void scan(ScanRequest request, StreamObserver<ScanResponse> responseObserver) {
        if (!checkScanRequest(request, responseObserver)) {
            return;
        }
        try (RelationalResultSet rs = this.frl.scan(request.getDatabase(), request.getSchema(), request.getTableName(),
                    TypeConversion.fromProtobuf(request.getKeySet()), TypeConversion.fromProtobuf(request.getOptions()))) {
            ScanResponse scanResponse = ScanResponse.newBuilder().setResultSet(TypeConversion.toProtobuf(rs)).build();
            responseObserver.onNext(scanResponse);
            responseObserver.onCompleted();
        } catch (SQLException e) {
            responseObserver.onError(StatusProto.toStatusRuntimeException(GrpcSQLExceptionUtil.create(e)));
        } catch (RuntimeException e) {
            throw handleUncaughtException(e);
        }
    }

    private static boolean checkScanRequest(ScanRequest request, StreamObserver<ScanResponse> responseObserver) {
        if (!request.hasDatabase() || request.getDatabase().isEmpty()) {
            responseObserver.onError(createStatusRuntimeException("Empty database name"));
            return false;
        }
        if (!request.hasSchema() || request.getSchema().isEmpty()) {
            responseObserver.onError(createStatusRuntimeException("Empty schema name"));
            return false;
        }
        if (!request.hasTableName() || request.getTableName().isEmpty()) {
            responseObserver.onError(createStatusRuntimeException("Empty table name"));
            return false;
        }
        if (!request.hasKeySet()) {
            responseObserver.onError(createStatusRuntimeException("Has no keyset"));
            return false;
        }
        return true;
    }

    static StatusRuntimeException handleUncaughtException(Throwable t) {
        return Status.INTERNAL.withDescription("Uncaught exception")
                .augmentDescription(GrpcSQLExceptionUtil.stacktraceToString(t))
                .withCause(t).asRuntimeException();
    }

}
