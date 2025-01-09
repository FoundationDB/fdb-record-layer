/*
 * FileOptions.java
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

package com.apple.foundationdb.relational.yamltests.block;

import com.apple.foundationdb.relational.yamltests.Matchers;
import com.apple.foundationdb.relational.yamltests.YamlExecutionContext;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Assumptions;

import java.util.Map;
import java.util.Set;

/**
 * Block that configures aspects of the test that do not require a connection yet.
 * <p>
 *     This supports the following sub-commands:
 *     <ul>
 *         <li>{@code supported_version}: if this is set, it will disable the test when running against a server
 *         of a specific version (or newer). The special yaml tag {@code !current_version} can be used to indicate that
 *         it should work against the current code, but is not expected to work with any older versions.
 *         In the future, the release script will update these automatically to the version being released.
 *         <p>
 *             <b>Example:</b>
 *             <pre>{@code
 *                 ---
 *                 options:
 *                     supported_version: !current_version
 *             }</pre>
 *     </ul>
 */
public class FileOptions {
    public static final String OPTIONS = "options";
    private static final String SUPPORTED_VERSION = "supported_version";
    private static final Logger logger = LogManager.getLogger(FileOptions.class);

    public static Block parse(int lineNumber, Object document, YamlExecutionContext executionContext) {
        final Map<?, ?> options = Matchers.map(document, OPTIONS);
        Object supportedVersion = options.get(SUPPORTED_VERSION);
        if (supportedVersion instanceof CurrentVersion) {
            final Set<String> versionsUnderTest = executionContext.getConnectionFactory().getVersionsUnderTest();
            // IntelliJ, at least, doesn't display the reason, so log it
            logger.info(
                    "Skipping test that only works against the current version, when we're running with these versions: {}",
                    versionsUnderTest);
            Assumptions.assumeTrue(versionsUnderTest.isEmpty(),
                    () -> "Test only works against the current version, but we are running with these versions: " +
                    versionsUnderTest);
        } else {
            throw new RuntimeException("Unsupported supported_version: " + supportedVersion);
        }
        return new NoOpBlock(lineNumber);
    }

    public static final class CurrentVersion {
        public static final CurrentVersion INSTANCE = new CurrentVersion();

        private CurrentVersion() {
        }

        @Override
        public String toString() {
            return "!current_version";
        }
    }

    private static class NoOpBlock implements Block {
        private final int lineNumber;

        NoOpBlock(int lineNumber) {
            this.lineNumber = lineNumber;
        }

        @Override
        public int getLineNumber() {
            return this.lineNumber;
        }

        @Override
        public void execute() {

        }
    }
}
