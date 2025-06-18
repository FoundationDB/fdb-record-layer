/*
 * ServerResponseHandler.java
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

package com.apple.foundationdb.relational.jdbc;

import com.apple.foundationdb.relational.jdbc.grpc.v1.TransactionalResponse;
import io.grpc.stub.StreamObserver;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * A class that can handle transactional responses from the server.
 * This is the client's local state that corresponds to the server's transaction state. It is created
 * when the client connection enters autoCommit=off state and is closed when the client exits that mode
 * or when the connection is closed.
 */
public class ServerResponseHandler implements StreamObserver<TransactionalResponse> {
    public static final long TIMEOUT_IN_SECONDS = 30;

    // Queue with a capacity of 1 since we expect to synchronize getting each result from its request
    private BlockingQueue<TransactionalResponse> responseQueue = new LinkedBlockingQueue<>(1);

    @Override
    public void onNext(final TransactionalResponse transactionResponse) {
        // Get the response to the handler
        try {
            final boolean success = responseQueue.offer(transactionResponse, TIMEOUT_IN_SECONDS, TimeUnit.SECONDS);
            if (!success) {
                throw new JdbcConnectionException("Failed to add result to queue");
            }
        } catch (Exception ex) {
            throw new JdbcConnectionException("Failed to deliver result", ex);
        }
    }

    @Override
    public void onError(final Throwable throwable) {
        // log error, close connection, send done to handler??
    }

    @Override
    public void onCompleted() {
        // this shouldn't really happen??
    }

    /**
     * Wait for, and return the server response.
     * @return the response received from the server, or error if timed out
     */
    public TransactionalResponse getResponse() {
        try {
            final TransactionalResponse response = responseQueue.poll(TIMEOUT_IN_SECONDS, TimeUnit.SECONDS);
            if (response == null) {
                throw new JdbcConnectionException("Timed out waiting for result from server");
            }
            return response;
        } catch (Exception e) {
            throw new JdbcConnectionException("Failed to get result from server", e);
        }
    }
}
