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

import java.io.IOException;
import java.util.logging.Logger;

/**
 * Stand up a server for downstream modules to use in test.
 */
public final class ServerTestUtil {
    private static final Logger logger = Logger.getLogger(ServerTestUtil.class.getName());
    private static final int PORT_RETRY_MAX = 100;

    /**
     * No constructors on a utility class.
     */
    private ServerTestUtil() {
    }

    /**
     * Create and start a RelationalServer.
     * Cleanup and retry with a different port number if we hit a BindException on start.
     * Can happen when lots of concurrent tests running on a single host.
     * Because of the above, do not presume the Server is running at 'preferredPort';
     * be sure to read the port-to-use by calling getPort on the returned Server instance.
     * @return Return a started {@link RelationalServer}
     */
    public static RelationalServer createAndStartRelationalServer(int preferredPort) throws IOException {
        RelationalServer relationalServer = null;
        for (int port = preferredPort; port <= (preferredPort + PORT_RETRY_MAX); port++) {
            relationalServer = new RelationalServer(port);
            try {
                relationalServer.start();
                // Successful start.
                break;
            } catch (IOException ioe) {
                // grpc throws an IOE w/ a message that begins with the below when BindException.
                if (ioe.getMessage().contains("Failed to bind to address")) {
                    final int portToLog = port;
                    logger.info(() -> "BindException on port=" + portToLog + ", trying the next port");
                    relationalServer.awaitTermination();
                    relationalServer = null;
                    continue;
                }
                throw ioe;
            }
        }
        return relationalServer;
    }
}
