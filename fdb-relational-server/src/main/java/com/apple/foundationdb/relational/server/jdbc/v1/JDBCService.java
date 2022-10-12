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

import com.apple.foundationdb.relational.grpc.jdbc.v1.DatabaseMetaDataRequest;
import com.apple.foundationdb.relational.grpc.jdbc.v1.DatabaseMetaDataResponse;
import com.apple.foundationdb.relational.grpc.jdbc.v1.JDBCServiceGrpc;
import com.apple.foundationdb.relational.grpc.jdbc.v1.StatementRequest;
import com.apple.foundationdb.relational.grpc.jdbc.v1.StatementResponse;

import io.grpc.stub.StreamObserver;
import org.apple.relational.grpc.GrpcConstants;

// TODO: Read version and product name from elsewhere.
// TODO: Fill out JDBC functionality. Float Relational instance in here.
public class JDBCService extends JDBCServiceGrpc.JDBCServiceImplBase {
    @Override
    public void getMetaData(DatabaseMetaDataRequest request,
                            StreamObserver<DatabaseMetaDataResponse> responseObserver) {
        DatabaseMetaDataResponse databaseMetaDataResponse = DatabaseMetaDataResponse.newBuilder()
                .setDatabaseProductName(GrpcConstants.PRODUCT_NAME)
                .setDatabaseProductVersion(GrpcConstants.PRODUCT_VERSION)
                .build();
        responseObserver.onNext(databaseMetaDataResponse);
        responseObserver.onCompleted();
    }

    @Override
    public void executeStatement(StatementRequest request,
                                 StreamObserver<StatementResponse> responseObserver) {
        responseObserver.onNext(StatementResponse.newBuilder().build());
        responseObserver.onCompleted();
    }
}
