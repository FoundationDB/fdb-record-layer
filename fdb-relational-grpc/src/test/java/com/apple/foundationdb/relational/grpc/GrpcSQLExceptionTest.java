/*
 * GrpcSQLExceptionTest.java
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

package com.apple.foundationdb.relational.grpc;

import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.StatusProto;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.SQLNonTransientException;

public class GrpcSQLExceptionTest {
    @Test
    public void testCreate() {
        final String message = "Some old status message";
        com.google.rpc.Status status = GrpcSQLException.create(new SQLException(message));
        Assertions.assertTrue(status.getMessage().contains(message));
    }

    @Test
    public void testFromAndToSQLException() throws InvalidProtocolBufferException {
        final String message = "Some old status message";
        com.google.rpc.Status status = GrpcSQLException.create(new SQLException(message));
        StatusRuntimeException statusRuntimeException = StatusProto.toStatusRuntimeException(status);
        SQLException sqlException = GrpcSQLException.map(statusRuntimeException);
        Assertions.assertEquals(message, sqlException.getMessage());
    }

    /**
     * Test that we can handle subclasses of SQLException and that we can re-create the type on
     * deserialization.
     */
    @Test
    public void testFromAndToSQLNonTransientException() throws InvalidProtocolBufferException {
        final String message = "Some old status message";
        // Arbitrary 'cause'.
        IOException cause = new IOException(message);
        com.google.rpc.Status status = GrpcSQLException.create(new SQLNonTransientException(message, cause));
        StatusRuntimeException statusRuntimeException = StatusProto.toStatusRuntimeException(status);
        SQLException sqlException = GrpcSQLException.map(statusRuntimeException);
        Assertions.assertEquals(message, sqlException.getMessage());
        Assertions.assertTrue(sqlException instanceof SQLNonTransientException);
        Assertions.assertTrue(sqlException.getCause() instanceof IOException);
        Assertions.assertEquals(sqlException.getCause().getMessage(), message);
    }
}
