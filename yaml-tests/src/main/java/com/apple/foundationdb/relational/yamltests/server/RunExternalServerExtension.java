/*
 * RunExternalServerExtension.java
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

package com.apple.foundationdb.relational.yamltests.server;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

/**
 * Extension to run an external server, add as a field, annotated with {@link org.junit.jupiter.api.extension.RegisterExtension}.
 * <p>
 *     For example:
 *     <pre>{@code @RegisterExtension
 * static final RunExternalServerExtension = new RunExternalServerExtension();
 *     }</pre>
 */
public class RunExternalServerExtension implements BeforeAllCallback, AfterAllCallback {
    private static final int SERVER_PORT = 1111;
    private final ExternalServer externalServer;

    /**
     * Create a new extension that will run latest released version of the server, as downloaded by gradle.
     */
    public RunExternalServerExtension() {
        this.externalServer = new ExternalServer(SERVER_PORT, SERVER_PORT + 1);
    }

    /**
     * Get the port to use when connecting.
     * @return the grpc port that the server is listening to
     */
    public int getPort() {
        return externalServer.getPort();
    }

    /**
     * Get the version of the server.
     * @return the version of the server being run.
     */
    public String getVersion() {
        return externalServer.getVersion();
    }


    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        externalServer.start();
    }

    @Override
    public void afterAll(ExtensionContext context) throws Exception {
        externalServer.stop();
    }

}
