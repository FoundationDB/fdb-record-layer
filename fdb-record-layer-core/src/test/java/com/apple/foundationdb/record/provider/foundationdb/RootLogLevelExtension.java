/*
 * RootLogLevelExtension.java
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

package com.apple.foundationdb.record.provider.foundationdb;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

public class RootLogLevelExtension implements BeforeEachCallback, AfterEachCallback {
    private final Level tempLevel;
    private Level original;

    public RootLogLevelExtension(Level tempLevel) {
        this.tempLevel = tempLevel;
    }

    @Override
    public void beforeEach(ExtensionContext context) {
        LoggerContext ctx = (LoggerContext)LogManager.getContext(false);
        Configuration cfg = ctx.getConfiguration();
        original = cfg.getLoggerConfig(LogManager.ROOT_LOGGER_NAME).getLevel();
        Configurator.setRootLevel(tempLevel);
    }

    @Override
    public void afterEach(ExtensionContext context) {
        Configurator.setRootLevel(original);
    }
}
