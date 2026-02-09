/*
 * SkippedCommand.java
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

package com.apple.foundationdb.relational.yamltests.command;

import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.util.Assert;
import com.apple.foundationdb.relational.yamltests.YamlReference;
import com.apple.foundationdb.relational.yamltests.YamlConnection;
import com.apple.foundationdb.relational.yamltests.YamlExecutionContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nonnull;
import java.sql.SQLException;

/**
 * A command that should be skipped by the block.
 * A SkippedCommand gets created when a command is deemed to be inappropriate for execution (because of a condition
 * that makes it so). The SkippedCommand then is a placeholder for a command that is not executed.
 */
public class SkippedCommand extends Command {
    private static final Logger logger = LogManager.getLogger(SkippedCommand.class);
    @Nonnull
    private final String message;
    @Nonnull
    private final String query;

    SkippedCommand(@Nonnull final YamlReference reference, @Nonnull final YamlExecutionContext executionContext,
                   @Nonnull String message, @Nonnull final String query) {
        super(reference, executionContext);
        this.message = message;
        this.query = query;
    }

    @Override
    void executeInternal(@Nonnull final YamlConnection connection) throws SQLException, RelationalException {
        Assert.fail("Skipped commands should not be called");
    }

    public void log() {
        if (logger.isInfoEnabled()) {
            logger.info("Skipped " + getReference() + ": '" + query + "' --  " + message);
        }
    }
}
