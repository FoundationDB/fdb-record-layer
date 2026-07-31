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
import com.apple.foundationdb.relational.yamltests.YamlReference;

import javax.annotation.Nonnull;
import java.util.List;

/**
 * A Block is a single YAML document region in a {@code .yamsql} file. The top-level key determines the block type:
 *
 * <table>
 *   <caption>Block types recognized by {@link #parse}</caption>
 *   <tr><th>YAML key</th><th>Block class</th><th>Purpose</th></tr>
 *   <tr><td>{@code options}</td><td>{@link PreambleBlock}</td>
 *       <td>File-wide configuration ({@code supported_version}, {@code connection_options}). Must be the first
 *       block if present.</td></tr>
 *   <tr><td>{@code schema_template}</td><td>{@link SetupBlock} (SchemaTemplateBlock)</td>
 *       <td>Declarative schema creation from DDL statements. The framework creates a temporary database and schema
 *       automatically, and cleans them up after all blocks execute. Referenced by 1-based index in
 *       {@code connect}. The value can also be a list of any number of variants, each holding a {@code definition}
 *       body and gated by {@code initialVersionAtLeast} or {@code initialVersionLessThan} (or both). In that case the
 *       variant whose version range contains the initial version of the catalog connection is the one whose DDL is
 *       executed. The variants must be mutually exclusive and must comprehensively cover all possible versions.</td></tr>
 *   <tr><td>{@code setup}</td><td>{@link SetupBlock} (ManualSetupBlock)</td>
 *       <td>Arbitrary SQL steps for environment setup/teardown. Supports {@code connect} (integer index, {@code 0}
 *       for catalog, or full JDBC URI) and {@code steps} (list of queries).</td></tr>
 *   <tr><td>{@code test_block}</td><td>{@link TestBlock}</td>
 *       <td>Group of tests with execution options (mode, repetition, presets). See {@link TestBlock} for details.</td></tr>
 *   <tr><td>{@code include}</td><td>{@link IncludeBlock}</td>
 *       <td>Include another {@code .yamsql} file's blocks at this point in execution.</td></tr>
 *   <tr><td>{@code transaction_setups}</td><td>{@link TransactionSetupsBlock}</td>
 *       <td>Named, reusable transaction-scoped setup definitions (e.g., {@code CREATE TEMPORARY FUNCTION}).
 *       Referenced from tests via {@code setupReference}.</td></tr>
 * </table>
 *
 * @see TestBlock
 * @see SetupBlock
 * @see PreambleBlock
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
    static List<Block> parse(@Nonnull YamlReference.YamlResource resource, @Nonnull Object region, int blockNumber,
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
                    return TransactionSetupsBlock.parse(entry.getValue(), executionContext);
                case TestBlock.TEST_BLOCK:
                    return TestBlock.parse(blockNumber, reference, entry.getValue(), executionContext);
                case SetupBlock.SchemaTemplateBlock.SCHEMA_TEMPLATE_BLOCK:
                    return SetupBlock.SchemaTemplateBlock.parse(reference, entry.getValue(), executionContext);
                case PreambleBlock.OPTIONS:
                    Assert.that(isTopLevel && blockNumber == 0, "File-wide options must be the first block, but found one at " + reference);
                    return PreambleBlock.parse(entry.getValue(), executionContext);
                case IncludeBlock.INCLUDE:
                    return IncludeBlock.parse(reference, entry.getValue(), executionContext);
                case CopyBlock.COPY_BLOCK:
                    return CopyBlock.parse(reference, entry.getValue(), executionContext);
                default:
                    throw new RuntimeException("Cannot recognize the type of block");
            }
        } catch (Exception e) {
            throw YamlExecutionContext.wrapContext(e, () -> "Error parsing block at " + reference, blockKey, reference);
        }
    }

    /**
     * Returns the {@link YamlReference} attached with the block. This is usually represented by the first line of the
     * block in YAMSQL file and the name of that file. However, the more important bit that is there in it, is the call
     * stack that has led to the execution of this block.
     * @return the {@link YamlReference} of the current block.
     */
    @Nonnull
    YamlReference getReference();

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
    default List<Block> getAndClearFinalizingBlocks() {
        return List.of();
    }
}
