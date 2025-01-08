/*
 * JDBCServiceTest.java
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

import com.apple.foundationdb.relational.jdbc.grpc.v1.StatementRequest;
import com.apple.foundationdb.relational.jdbc.grpc.v1.StatementResponse;

import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

@API(API.Status.EXPERIMENTAL)
public class JDBCServiceTest {

    @Test
    public void testCheckStatementRequest() {
        var observer = new StreamObserver<StatementResponse>() {
            int error = 0;

            @Override
            public void onNext(StatementResponse value) {
            }

            @Override
            public void onError(Throwable t) {
                this.error++;
            }

            @Override
            public void onCompleted() {
            }
        };
        JDBCService.checkStatementRequest(StatementRequest.newBuilder().build(), observer);
        Assertions.assertEquals(1, observer.error);
    }
}
