/*
 * ServerTestUtil.java
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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.test.FDBTestEnvironment;
import io.prometheus.client.CollectorRegistry;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.BindException;

/**
 * Stand up a server for downstream modules to use in test.
 */
@API(API.Status.EXPERIMENTAL)
public final class ServerTestUtil {
    private static final Logger logger = LogManager.getLogger(ServerTestUtil.class.getName());
    private static final int PORT_RETRY_MAX = 100;

    /**
     * No constructors on a utility class.
     */
    private ServerTestUtil() {
    }

    /**
     * Create and start a RelationalServer.
     * Cleanup and retry with different port numbers if we hit a BindException on start.
     * (Let the http port ride along at grpc port + 1).
     * Can happen when lots of concurrent tests running on a single host.
     * Because of the above, do not presume the Server is running at 'preferredPort';
     * be sure to read the port-to-use by calling getGrpcPort/getHttpPort on the returned Server instance.
     * @return Return a started {@link RelationalServer}
     */
    public static RelationalServer createAndStartRelationalServer(int preferredPort) throws IOException {
        return createAndStartRelationalServer(preferredPort, FDBTestEnvironment.randomClusterFile());
    }

    /**
     * Create and start a RelationalServer with a specific cluster file.
     * @param preferredPort The preferred port to start on
     * @param clusterFile The cluster file to use for FDB connection
     * @return Return a started {@link RelationalServer}
     */
    public static RelationalServer createAndStartRelationalServer(int preferredPort, String clusterFile) throws IOException {
        RelationalServer relationalServer = null;
        for (int port = preferredPort; port <= (preferredPort + PORT_RETRY_MAX); port += 2) {
            // Create a CollectorRegistry when a test fixture else "java.lang.IllegalArgumentException:
            // Collector already registered that provides name: grpc_server_started_total" when the second instance
            // of the test fixture runs. Otherwise, just use the default in VServer.
            relationalServer = new RelationalServer(port, port + 1, new CollectorRegistry(true), clusterFile);
            try {
                relationalServer.start();
                // Successful start.
                break;
            } catch (IOException ioe) {
                // GRPC throws an IOE w/ a message that begins with the below when BindException.
                // HTTPServer will throw a BindException. Handle both.
                if (ioe instanceof BindException ||
                        (ioe.getCause() != null && ioe.getCause() instanceof  BindException) ||
                        ioe.getMessage().contains("Failed to bind to address")) {
                    final int portToLog = port;
                    logger.info("BindException on port={}, trying the next port", portToLog, ioe);
                    relationalServer.close();
                    relationalServer = null;
                    continue;
                }
                throw ioe;
            }
        }
        return relationalServer;
    }
}
