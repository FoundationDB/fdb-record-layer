/*
 * Block.java
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

import com.apple.foundationdb.relational.util.Assert;
import com.apple.foundationdb.relational.yamltests.CustomYamlConstructor;
import com.apple.foundationdb.relational.yamltests.Matchers;
import com.apple.foundationdb.relational.yamltests.YamlExecutionContext;

import javax.annotation.Nonnull;

/**
 * Block is a single region in the YAMSQL file that can either be a
 * {@link FileOptions}, {@link SetupBlock} or {@link TestBlock}.
 * <ul>
 *      <li> {@link FileOptions}: Controls whether a file should be run at all, and other configuration that runs before
 *          creating the connection.</li>
 *      <li> {@link SetupBlock}: It can be either a `setup` block or a `destruct` block. The motive of these block
 *          is to "setup" and "clean" the environment needed to run the `test-block`s. A Setup block consist of a list
 *          of commands.</li>
 *      <li> {@link TestBlock}: Defines a scope for a group of tests by setting the knobs that determines how those
 *          tests are run.</li>
 * </ul>
 */
public interface Block {
    /**
     * Looks at the block to determine if its one of the valid blocks. If it is a valid one, parses it to that. This
     * method dispatches the execution to the right block which takes care of reading from the block and initializing
     * the correctly configured {@link ConnectedBlock} for execution.
     *
     * @param document a region in the file
     * @param executionContext information needed to carry out the execution
     */
    static Block parse(@Nonnull Object document, @Nonnull YamlExecutionContext executionContext) {
        final var blockObject = Matchers.map(document, "block");
        Assert.thatUnchecked(blockObject.size() == 1, "Illegal Format: A block is expected to be a map of size 1");
        final var entry = Matchers.firstEntry(blockObject, "block key-value");
        final var linedObject = CustomYamlConstructor.LinedObject.cast(entry.getKey(), () -> "Invalid block key-value pair: " + entry);
        final var lineNumber = linedObject.getLineNumber();
        switch (Matchers.notNull(Matchers.string(linedObject.getObject(), "block key"), "block key")) {
            case SetupBlock.SETUP_BLOCK:
                return SetupBlock.ManualSetupBlock.parse(lineNumber, entry.getValue(), executionContext);
            case TestBlock.TEST_BLOCK:
                return TestBlock.parse(lineNumber, entry.getValue(), executionContext);
            case SetupBlock.SchemaTemplateBlock.SCHEMA_TEMPLATE_BLOCK:
                return SetupBlock.SchemaTemplateBlock.parse(lineNumber, entry.getValue(), executionContext);
            case FileOptions.OPTIONS:
                return FileOptions.parse(lineNumber, entry.getValue(), executionContext);
            default:
                throw new RuntimeException("Cannot recognize the type of block");
        }
    }

    int getLineNumber();

    /**
     * Executes the executables from the parsed block in a single connection.
     */
    void execute();
}
