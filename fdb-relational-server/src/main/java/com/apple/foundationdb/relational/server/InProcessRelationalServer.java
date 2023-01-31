/*
 * InProcessRelationalServer.java
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

package com.apple.foundationdb.relational.server;

import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.server.jdbc.v1.JDBCService;

import io.grpc.Server;
import io.grpc.inprocess.InProcessServerBuilder;

import java.io.Closeable;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.time.Instant;

/**
 * Launch an in-process 'direct-invocation' (no RPC) Relational GRPC Server.
 * Each in-process server is identified by 'name'.
 * Like {@link RelationalServer} only in-process and only runs the Relational GRPC Service and no other (no HealthCheck
 * Service, etc.).
 * @see RelationalServer
 */
@SuppressWarnings({"PMD.SystemPrintln", "PMD.DoNotCallSystemExit"})
public class InProcessRelationalServer implements Closeable {
    private Server grpcInProcessServer;
    private FRL frl;
    private final String serverName;

    public InProcessRelationalServer() {
        this(InProcessServerBuilder.generateName());
    }

    InProcessRelationalServer(String serverName) {
        this.serverName = serverName;
    }

    public String getServerName() {
        return this.serverName;
    }

    @Override
    public String toString() {
        return getServerName();
    }

    InProcessRelationalServer start() throws IOException {
        // Create access to backing database.
        // TODO: Make this multi-query/-tenant/-database!
        try {
            this.frl = new FRL();
        } catch (RelationalException ve) {
            throw new IOException(ve);
        }
        // Build Server with Services.
        this.grpcInProcessServer = InProcessServerBuilder.forName(getServerName()).directExecutor()
                .addService(new JDBCService(frl)).build();
        this.grpcInProcessServer.start();
        // From https://github.com/grpc/grpc-java/blob/master/examples/src/main/java/io/grpc/examples/routeguide/RouteGuideServer.java
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                // Use stderr here since the logger may have been reset by its JVM shutdown hook.
                System.err.println(Instant.now() + " Waiting on in-process Server " + getServerName() + " termination");
                try {
                    InProcessRelationalServer.this.grpcInProcessServer.shutdown();
                    InProcessRelationalServer.this.awaitTermination();
                } catch (InterruptedIOException e) {
                    throw new RuntimeException(e);
                }
                System.err.println(Instant.now() + " Server shutdown");
            }
        });
        // This is what is used on the cli. It is overkill for here but easy for now.
        return this;
    }

    void awaitTermination() throws InterruptedIOException {
        try {
            if (this.grpcInProcessServer != null) {
                // Server waits here while working.
                this.grpcInProcessServer.awaitTermination();
            }
        } catch (InterruptedException ioe) {
            InterruptedIOException iioe = new InterruptedIOException();
            iioe.initCause(ioe);
            throw iioe;
        }
    }

    @Override
    public void close() throws IOException {
        IOException ioe = null;
        if (this.frl != null) {
            try {
                this.frl.close();
            } catch (IOException e) {
                ioe = e;
            } catch (Exception e) {
                ioe = new IOException(e);
            }
        }
        if (this.grpcInProcessServer != null) {
            this.grpcInProcessServer.shutdown();
            awaitTermination();
        }
        if (ioe != null) {
            throw ioe;
        }
    }
}
