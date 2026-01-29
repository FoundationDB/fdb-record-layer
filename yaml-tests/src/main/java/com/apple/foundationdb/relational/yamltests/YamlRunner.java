/*
 * YamlRunner.java
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

package com.apple.foundationdb.relational.yamltests;

import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.util.Assert;
import com.apple.foundationdb.relational.yamltests.block.IncludeBlock;
import com.apple.foundationdb.relational.yamltests.block.TestBlock;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@SuppressWarnings({"PMD.GuardLogStatement", "PMD.AvoidCatchingThrowable"}) // It already is, but PMD is confused and reporting error in unrelated locations.
public final class YamlRunner {

    private static final Logger logger = LogManager.getLogger(YamlRunner.class);

    static final String TEST_NIGHTLY = "tests.nightly";
    static final String TEST_SEED = "tests.yaml.seed";
    static final String TEST_NIGHTLY_REPETITION = "tests.yaml.iterations";

    @Nonnull
    private final YamlReference.YamlResource baseResource;

    @Nonnull
    private final YamlExecutionContext executionContext;

    public YamlRunner(@Nonnull String resourcePath, @Nonnull YamlConnectionFactory factory,
                      @Nonnull final YamlExecutionContext.ContextOptions additionalOptions) throws RelationalException {
        this.baseResource = YamlReference.YamlResource.base(resourcePath);
        this.executionContext = new YamlExecutionContext(baseResource, factory, additionalOptions);
    }

    public void run() throws Exception {
        try {
            final var allBlocks = IncludeBlock.parse(baseResource, executionContext);
            final var testBlocks = new ArrayList<TestBlock>();
            for (final var block: allBlocks) {
                if (block instanceof TestBlock) {
                    testBlocks.add((TestBlock)block);
                }
            }
            Assert.thatUnchecked(!testBlocks.isEmpty(), "No test blocks found!");
            for (final var block: allBlocks) {
                logger.debug("⚪️ Executing {} at {}", block.getClass().getSimpleName(), block.getReference());
                block.execute();
            }
            evaluateTestBlockResults(testBlocks);
            executionContext.replaceFilesIfRequired();
        } catch (Throwable e) {
            throw YamlExecutionContext.wrapContext(e, () -> "‼️ running test file '" + baseResource.getPath() + "' was not successful", "<root>", baseResource.withLineNumber(0));
        }
    }

    private void evaluateTestBlockResults(List<TestBlock> testBlocks) {
        logger.info("");
        logger.info("");
        logger.info("--------------------------------------------------------------------------------------------------------------");
        logger.info("TEST RESULTS");
        logger.info("--------------------------------------------------------------------------------------------------------------");

        RuntimeException failure = null;
        for (int i = 0; i < testBlocks.size(); i++) {
            final var block = testBlocks.get(i);
            Optional<RuntimeException> maybeFailure = block.getFailureExceptionIfPresent();
            if (maybeFailure.isEmpty()) {
                logger.info("🟢 TestBlock {}/{} runs successfully", i + 1, testBlocks.size());
            } else {
                RuntimeException failureInBlock = maybeFailure.get();
                logger.error("🔴 TestBlock {}/{} ({}) fails", i + 1, testBlocks.size(), block.getReference());
                logger.error("--------------------------------------------------------------------------------------------------------------");
                logger.error("Error:", failureInBlock);
                logger.error("--------------------------------------------------------------------------------------------------------------");
                failure = failure == null ? failureInBlock : failure;
            }
        }
        if (failure != null) {
            logger.error("⚠️ Some TestBlocks in {} do not pass. ", baseResource.getPath());
            throw failure;
        } else {
            logger.info("🟢 All tests in {} pass successfully.", baseResource.getPath());
        }
    }
}
