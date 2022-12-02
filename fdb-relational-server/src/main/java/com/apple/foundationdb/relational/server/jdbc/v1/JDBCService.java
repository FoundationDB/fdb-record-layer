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

import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.grpc.GrpcSQLException;
import com.apple.foundationdb.relational.grpc.jdbc.v1.DatabaseMetaDataRequest;
import com.apple.foundationdb.relational.grpc.jdbc.v1.DatabaseMetaDataResponse;
import com.apple.foundationdb.relational.grpc.jdbc.v1.JDBCServiceGrpc;
import com.apple.foundationdb.relational.grpc.jdbc.v1.StatementRequest;
import com.apple.foundationdb.relational.grpc.jdbc.v1.StatementResponse;
import com.apple.foundationdb.relational.server.FRL;
import com.apple.foundationdb.relational.util.BuildVersion;

import com.google.protobuf.ListValue;
import com.google.protobuf.Value;
import com.google.spanner.v1.ResultSetMetadata;
import com.google.spanner.v1.StructType;
import com.google.spanner.v1.Type;
import com.google.spanner.v1.TypeCode;
import io.grpc.Status;
import io.grpc.protobuf.StatusProto;
import io.grpc.stub.StreamObserver;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

// TODO: Read version and product name from elsewhere.
// TODO: Fill out JDBC functionality. Float Relational instance in here.
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

    static com.google.spanner.v1.ResultSet map(ResultSet resultSet) throws SQLException {
        var relationalResultSet = (RelationalResultSet) resultSet;
        var resultSetBuilder = com.google.spanner.v1.ResultSet.newBuilder();
        // TODO: Presumes that the metadata does not change as we traverse rows. Is this ok assumption?
        var relationalResultSetMetaData = relationalResultSet.getMetaData();
        while (relationalResultSet.next()) {
            var listValueBuilder = ListValue.newBuilder();
            for (int i = 0; i < relationalResultSetMetaData.getColumnCount(); i++) {
                int index = i + 1;
                var columnType = relationalResultSetMetaData.getColumnType(index);
                listValueBuilder.addValues(toValue(columnType, index, relationalResultSet));
            }
            resultSetBuilder.addRows(listValueBuilder.build());
        }

        var structTypeBuilder = StructType.newBuilder();
        for (int i = 0; i < relationalResultSetMetaData.getColumnCount(); i++) {
            int index = i + 1; /*JDBC is 1-based here!*/
            var columnType = relationalResultSetMetaData.getColumnType(index);
            // TODO: This should be the columnname when get and columnname when sql query.
            var columName = relationalResultSetMetaData.getColumnName(index);
            var field = StructType.Field.newBuilder().setName(columName)
                    .setType(Type.newBuilder().setCode(toTypeCode(columnType)).build()).build();
            structTypeBuilder.addFields(field);
        }
        resultSetBuilder.setMetadata(ResultSetMetadata.newBuilder().setRowType(structTypeBuilder.build()).build());
        return resultSetBuilder.build();
    }

    static Value toValue(int columnType, int columnIndex, RelationalResultSet relationalResultSet) throws SQLException {
        Value value = null;
        switch (columnType) {
            case Types.BOOLEAN:
                value = Value.newBuilder().setBoolValue(relationalResultSet.getBoolean(columnIndex)).build();
                break;
            case Types.INTEGER:
                value = Value.newBuilder().setNumberValue(relationalResultSet.getInt(columnIndex)).build();
                break;
            case Types.VARCHAR:
                value = Value.newBuilder().setStringValue(relationalResultSet.getString(columnIndex)).build();
                break;
            default:
                throw new SQLException("Type " + columnType + " not implemented ",
                        ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
        }
        return value;
    }

    private static TypeCode toTypeCode(int columnType) throws SQLException {
        TypeCode typeCode = null;
        switch (columnType) {
            case Types.BOOLEAN:
                typeCode = TypeCode.BOOL;
                break;
            case Types.INTEGER:
                typeCode = TypeCode.INT64;
                break;
            case Types.VARCHAR:
                typeCode = TypeCode.STRING;
                break;
            default:
                throw new SQLException("Type " + columnType + " not implemented ",
                        ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
        }
        return typeCode;
    }

    static boolean checkStatementRequest(StatementRequest statementRequest,
                                  StreamObserver<StatementResponse> responseObserver) {
        if (statementRequest.getDatabase().isEmpty()) {
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription("Empty database name")
                    .asRuntimeException());
            return false;
        }
        if (statementRequest.getSchema().isEmpty()) {
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription("Empty schema name")
                    .asRuntimeException());
            return false;
        }
        if (statementRequest.getSql().isEmpty()) {
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription("Empty sql statement")
                    .asRuntimeException());
            return false;
        }
        return true;
    }

    @Override
    public void execute(StatementRequest request, StreamObserver<StatementResponse> responseObserver) {
        if (!checkStatementRequest(request, responseObserver)) {
            return;
        }
        try (ResultSet resultSet = this.frl.execute(request.getDatabase(), request.getSchema(), request.getSql())) {
            StatementResponse statementResponse = resultSet == null ? StatementResponse.newBuilder().build() :
                    StatementResponse.newBuilder().setResultSet(map(resultSet)).build();
            responseObserver.onNext(statementResponse);
            responseObserver.onCompleted();
        } catch (SQLException e) {
            responseObserver.onError(StatusProto.toStatusRuntimeException(GrpcSQLException.create(e)));
        }
    }

    @Override
    public void update(StatementRequest request, StreamObserver<StatementResponse> responseObserver) {
        if (!checkStatementRequest(request, responseObserver)) {
            return;
        }
        try {
            int rowCount = this.frl.update(request.getDatabase(), request.getSchema(), request.getSql());
            StatementResponse statementResponse = StatementResponse.newBuilder().setRowCount(rowCount).build();
            responseObserver.onNext(statementResponse);
            responseObserver.onCompleted();
        } catch (SQLException e) {
            responseObserver.onError(StatusProto.toStatusRuntimeException(GrpcSQLException.create(e)));
        }
    }
}
