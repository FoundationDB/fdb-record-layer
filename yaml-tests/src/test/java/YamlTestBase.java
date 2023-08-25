/*
 * YamlTestBase.java
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
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.cli.CliCommandFactory;
import com.apple.foundationdb.relational.cli.DbState;
import com.apple.foundationdb.relational.cli.DbStateCommandFactory;
import com.apple.foundationdb.relational.yamltests.YamlRunner;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nonnull;

public abstract class YamlTestBase {

    private static final Logger LOG = LogManager.getLogger(YamlTestBase.class);

    public YamlTestBase() {
        if (Debugger.getDebugger() == null && Boolean.getBoolean("debugBuild")) {
            Debugger.setDebugger(new DebuggerWithSymbolTables());
        }
        Debugger.setup();
    }

    protected void doRun(@Nonnull final String fileName) throws Exception {
        doRun(fileName, false);
        doRun(fileName, true);
    }

    protected void doRun(String fileName, boolean usePreparedStatements) throws Exception {
        try (var yamlRunner = new YamlRunner(fileName, createCliCommandFactory(usePreparedStatements))) {
            try {
                yamlRunner.run();
            } catch (Exception e) {
                LOG.error("‼️ running test file '{}' was not successful", fileName, e);
                throw e;
            }
        }
    }

    CliCommandFactory createCliCommandFactory(boolean usePreparedStatements) throws RelationalException {
        return new DbStateCommandFactory(new DbState(), usePreparedStatements);
    }
}
