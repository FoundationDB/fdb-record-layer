/*
 * ExternalServerTest.java
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

package com.apple.foundationdb.relational.yamltests.server;

import com.apple.foundationdb.test.FDBTestEnvironment;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ExternalServerTest {

    private File currentServerPath;

    @BeforeEach
    void setUp() throws IOException {
        this.currentServerPath = getCurrentServerPath();
    }

    private static File getCurrentServerPath() throws IOException {
        final List<File> availableServers = ExternalServer.getAvailableServers();
        for (File path : availableServers) {
            if (new ExternalServer(path, null).getVersion().equals(SemanticVersion.current())) {
                return path;
            }
        }
        return Assertions.fail("No current version server");
    }

    @Test
    void startMultiple() throws Exception {
        final List<ExternalServer> servers = new ArrayList<>();
        final String clusterFile = FDBTestEnvironment.randomClusterFile();
        for (int i = 0; i < 3; i++) {
            servers.add(new ExternalServer(currentServerPath, clusterFile));
        }
        try {
            for (final ExternalServer server : servers) {
                server.start();
            }
            // we can't assert about the actual values, because one of the ports may be busy,
            // so assert that each server has its own port, and none of them have the same port
            assertEquals(servers.stream().map(ExternalServer::getPort).distinct().collect(Collectors.toList()),
                    servers.stream().map(ExternalServer::getPort).collect(Collectors.toList()));
        } finally {
            for (final ExternalServer server : servers) {
                server.stop();
            }
        }
    }
}
