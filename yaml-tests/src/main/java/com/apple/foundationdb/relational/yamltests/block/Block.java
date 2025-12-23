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
import com.apple.foundationdb.relational.yamltests.Reference;
import com.apple.foundationdb.relational.yamltests.YamlExecutionContext;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;

/**
 * Block is a single region in the YAMSQL file that can either be a
 * {@link PreambleBlock}, {@link SetupBlock} or {@link TestBlock}.
 * <ul>
 *      <li> {@link PreambleBlock}: Controls whether a file should be run at all, and other configuration that runs before
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
     * @param region a region in the file
     * @param executionContext information needed to carry out the execution
     * @return zero of more blocks
     */
    static ImmutableList<Block> parse(@Nonnull Reference.Resource resource, @Nonnull Object region, int blockNumber,
                                      @Nonnull YamlExecutionContext executionContext, boolean isTopLevel) {
        final var blockObject = Matchers.map(region, "block");
        Assert.thatUnchecked(blockObject.size() == 1,
                "Illegal Format: A block is expected to be a map of size 1 ({" + resource + "}) keys: " + blockObject.keySet());
        final var entry = Matchers.onlyEntry(blockObject, "block key-value");
        final var linedObject = CustomYamlConstructor.LinedObject.cast(entry.getKey(), () -> "Invalid block key-value pair: " + entry);
        final var reference = resource.withLineNumber(linedObject.getLineNumber());
        final String blockKey = Matchers.notNull(Matchers.string(linedObject.getObject(), "block key"), "block key");
        try {
            switch (blockKey) {
                case SetupBlock.SETUP_BLOCK:
                    return SetupBlock.ManualSetupBlock.parse(reference, entry.getValue(), executionContext);
                case TransactionSetupsBlock.TRANSACTION_SETUP:
                    return TransactionSetupsBlock.parse(reference, entry.getValue(), executionContext);
                case TestBlock.TEST_BLOCK:
                    return TestBlock.parse(blockNumber, reference, entry.getValue(), executionContext);
                case SetupBlock.SchemaTemplateBlock.SCHEMA_TEMPLATE_BLOCK:
                    return SetupBlock.SchemaTemplateBlock.parse(reference, entry.getValue(), executionContext);
                case PreambleBlock.OPTIONS:
                    Assert.that(isTopLevel && blockNumber == 0, "File-wide options must be the first block, but found one at " + reference);
                    return PreambleBlock.parse(entry.getValue(), executionContext);
                case IncludeBlock.INCLUDE:
                    return IncludeBlock.parse(reference, entry.getValue(), executionContext);
                default:
                    throw new RuntimeException("Cannot recognize the type of block");
            }
        } catch (Exception e) {
            throw YamlExecutionContext.wrapContext(e, () -> "Error parsing block at line " + reference, blockKey, reference);
        }
    }

    /**
     * Returns the {@link Reference} attached with the block. This is usually represented by the first line of the
     * block in YAMSQL file and the name of that file. However, the more important bit that is there in it, is the call
     * stack that has led to the execution of this block.
     * @return the {@link Reference} of the current block.
     */
    @Nonnull
    Reference getReference();

    /**
     * Executes the executables from the parsed block in a single connection.
     */
    void execute();

    /**
     * Returns the list of {@link Block}s that needs to be called to clean up resources that the current block has
     * created. The time at which these blocks should be called depends on the caller of this function that will
     * decide the lifecycle of the resources.
     * @return the list of {@link Block}
     */
    @Nonnull
    default ImmutableList<Block> getFinalizingBlocks() {
        return ImmutableList.of();
    }
}
