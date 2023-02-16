/*
 * DatatypeIntegrationTests.java
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

import com.apple.foundationdb.record.query.plan.cascades.debug.Debugger;
import com.apple.foundationdb.record.query.plan.debug.DebuggerWithSymbolTables;
import com.apple.foundationdb.relational.yamltests.YamlRunner;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;

public class DatatypeIntegrationTests {

    private static final Logger LOG = LogManager.getLogger(DatatypeIntegrationTests.class);

    public DatatypeIntegrationTests() {
        if (Debugger.getDebugger() == null && Boolean.getBoolean("useCascadesDebugger")) {
            Debugger.setDebugger(new DebuggerWithSymbolTables());
        }
        Debugger.setup();
    }

    private void doRun(@Nonnull final String fileName) throws Exception {
        try (var yamlRunner = YamlRunner.create(fileName)) {
            try {
                yamlRunner.run();
            } catch (Exception e) {
                if (LOG.isErrorEnabled()) {
                    LOG.error(String.format("‼️ running test file '%s' was not successful", fileName));
                }
                e.printStackTrace();
                throw e;
            }
        }
    }

    @Test
    void booleanTypes() throws Exception {
        doRun("boolean.yaml");
    }
}
