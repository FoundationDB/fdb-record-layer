/*
 * PreambleBlock.java
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

package com.apple.foundationdb.relational.yamltests.block;

import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.yamltests.CustomYamlConstructor;
import com.apple.foundationdb.relational.yamltests.Matchers;
import com.apple.foundationdb.relational.yamltests.YamlExecutionContext;
import com.apple.foundationdb.relational.yamltests.server.SemanticVersion;
import com.apple.foundationdb.relational.yamltests.server.SupportedVersionCheck;
import com.google.common.collect.ImmutableList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Assumptions;

import javax.annotation.Nonnull;
import java.util.Map;

/**
 * A block that collects all file-wide options. It does <li>not</li> have an execution, but merely acts as a placeholder
 * for global options set in the preamble of the test file.
 */
public class PreambleBlock extends SupportBlock {

    public static final String OPTIONS = "options";
    public static final String PREAMBLE_BLOCK_SUPPORTED_VERSION = "supported_version";
    public static final String PREAMBLE_BLOCK_CONNECTION_OPTIONS = "connection_options";
    private static final Logger logger = LogManager.getLogger(PreambleBlock.class);

    private PreambleBlock() {

    }

    @Nonnull
    public static ImmutableList<Block> parse(@Nonnull final Object document, @Nonnull final YamlExecutionContext executionContext) {
        final Map<?, ?> optionsMap = CustomYamlConstructor.LinedObject.unlineKeys(Matchers.map(document, OPTIONS));

        // read the supported version option, and immediately abort the test if the version check fails.
        final SupportedVersionCheck check = SupportedVersionCheck.parseOptions(optionsMap, executionContext.getConnectionFactory().getVersionsUnderTest());
        if (!check.isSupported()) {
            // IntelliJ, at least, doesn't display the reason, so log it
            if (logger.isInfoEnabled()) {
                logger.info(check.getMessage());
            }
            Assumptions.assumeTrue(check.isSupported(), check.getMessage());
        }
        var connectionOptions = Options.none();
        if (optionsMap.containsKey(PREAMBLE_BLOCK_CONNECTION_OPTIONS)) {
            connectionOptions = TestBlock.TestBlockOptions.parseConnectionOptions(Matchers.map(optionsMap.get(PREAMBLE_BLOCK_CONNECTION_OPTIONS)));
        }

        executionContext.setConnectionOptions(connectionOptions);
        return ImmutableList.of();
    }

    @Nonnull
    public static SemanticVersion parseVersion(Object rawVersion) {
        if (rawVersion instanceof CurrentVersion) {
            return SemanticVersion.current();
        } else if (rawVersion instanceof String) {
            return SemanticVersion.parse((String) rawVersion);
        } else {
            throw new IllegalArgumentException("Unable to determine semantic version from object: " + rawVersion);
        }
    }

    public static final class CurrentVersion {
        public static final CurrentVersion INSTANCE = new CurrentVersion();
        public static final String TEXT = SemanticVersion.SemanticVersionType.CURRENT.getText();

        private CurrentVersion() {
        }

        @Override
        public String toString() {
            return TEXT;
        }
    }
}
