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

import com.apple.foundationdb.relational.jdbc.grpc.v1.TransactionalRequest;
import com.apple.foundationdb.relational.jdbc.grpc.v1.TransactionalResponse;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * A class that handles a stateful connection to the server.
 * This is the client's local state that corresponds to the server's transaction state. It is created
 * when the client connection enters autoCommit=off state and is closed when the client exits that mode
 * or when the connection is closed.
 */
public final class StatefulServerConnection implements StreamObserver<TransactionalResponse>, AutoCloseable {
    public static final long TIMEOUT_IN_SECONDS = 30;

    private static final Logger logger = LoggerFactory.getLogger(StatefulServerConnection.class);

    private boolean closed = false;

    /**
     * An RPC observer for outgoing requests.
     */
    private StreamObserver<TransactionalRequest> requestSender;

    /**
     * Queue with a capacity of 1 since we expect to synchronize getting each result from its request.
     */
    private BlockingQueue<CompletableFuture<TransactionalResponse>> responseQueue = new LinkedBlockingQueue<>(1);

    public StatefulServerConnection(Function<StreamObserver<TransactionalResponse>, StreamObserver<TransactionalRequest>> connectionFactory) {
        // Create the request sender. This should be the last statement in the constructor.
        // The use of the factory (as opposed to straight calling of the method) is because the gRPC API requires a receiving
        // stream (this) in order to create the sending one (the result of the RPC call). We want to manage both streams from this class.
        this.requestSender = connectionFactory.apply(this);
    }

    /**
     * Send a request to the server, wait for a response.
     * This method serializes a request-response flow between the client and the server.
     * Since we implement a single request-response protocol, sending a request will wait for a response (similar
     * to the way a synchronized RPC does), returning the response (or error), hiding the fact that this is an async
     * protocol.
     *
     * @param request the request to send
     *
     * @return the response returned by the server
     */
    public TransactionalResponse sendRequest(TransactionalRequest request) {
        checkClosed();
        try {
            requestSender.onNext(request);
        } catch (StatusRuntimeException ex) {
            // Close the connection when failed to send to server
            close(ex);
            throw ex;
        }

        try {
            // Wait for the response
            final CompletableFuture<TransactionalResponse> response = responseQueue.poll(TIMEOUT_IN_SECONDS, TimeUnit.SECONDS);
            // timed out waiting
            if (response == null) {
                final JdbcConnectionException exception = new JdbcConnectionException("Timed out waiting for response from server");
                close(exception);
                throw exception;
            }
            return response.join();
        } catch (InterruptedException ex) {
            final JdbcConnectionException exception = new JdbcConnectionException("Failed to get response from server", ex);
            close(exception);
            Thread.currentThread().interrupt();
            throw exception;
        } catch (CompletionException e) {
            // join failed
            Throwable cause = e.getCause();
            if (cause instanceof StatusRuntimeException) {
                // RPC error - just throw the cause (will be dealt with at the caller
                throw (StatusRuntimeException)cause;
            } else {
                // Some other unknown error
                throw e;
            }
        }
    }

    /**
     * Got a response from the server.
     *
     * @param transactionResponse the value passed to the stream
     */
    @Override
    public void onNext(final TransactionalResponse transactionResponse) {
        logger.info("Got response from server");
        // Get the response to the queue
        try {
            final boolean success = responseQueue.offer(CompletableFuture.completedFuture(transactionResponse), TIMEOUT_IN_SECONDS, TimeUnit.SECONDS);
            if (!success) {
                close(new JdbcConnectionException("Failed to add result to queue"));
            }
        } catch (InterruptedException ex) {
            close(new JdbcConnectionException("Failed to deliver result", ex));
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Got an error from the server.
     * This is closing the connection, and no other calls can be made.
     * @param t the error occurred on the stream
     */
    @Override
    public void onError(final Throwable t) {
        if (logger.isWarnEnabled()) {
            logger.warn("Got error from server: {}", t.getMessage());
        }
        try {
            // Put the failure on the queue so that the client will get it and stop waiting
            responseQueue.offer(CompletableFuture.failedFuture(t), TIMEOUT_IN_SECONDS, TimeUnit.SECONDS);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        } finally {
            close(t);
        }
    }

    @Override
    public void onCompleted() {
        close();
    }

    @Override
    public void close() {
        close(null);
    }

    private synchronized void close(@Nullable Throwable exception) {
        if (closed) {
            return;
        }
        closed = true;
        if (requestSender != null) {
            if (exception == null) {
                requestSender.onCompleted();
            } else {
                requestSender.onError(exception);
            }
            requestSender = null;
        }
    }

    private void checkClosed() {
        if (closed) {
            throw new IllegalStateException("Connection is closed");
        }
    }
}
