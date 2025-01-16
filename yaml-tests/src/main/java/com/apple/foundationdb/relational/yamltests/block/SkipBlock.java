/*
 * NoOpBlock.java
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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nonnull;

/**
 * A block that does nothing.
 */
class SkipBlock implements Block {
    private static final Logger logger = LogManager.getLogger(SkipBlock.class);
    private final int lineNumber;
    @Nonnull
    private final String message;

    SkipBlock(int lineNumber, @Nonnull String message) {
        this.lineNumber = lineNumber;
        this.message = message;
    }

    @Override
    public int getLineNumber() {
        return this.lineNumber;
    }

    @Override
    public void execute() {
        logger.info(message);
    }
}
