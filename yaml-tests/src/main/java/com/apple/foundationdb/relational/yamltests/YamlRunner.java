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
import com.apple.foundationdb.relational.util.SpotBugsSuppressWarnings;
import com.apple.foundationdb.relational.yamltests.block.Block;
import com.apple.foundationdb.relational.yamltests.block.TestBlock;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Assertions;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.representer.Representer;
import org.yaml.snakeyaml.resolver.Resolver;

import javax.annotation.Nonnull;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

@SuppressWarnings({"PMD.GuardLogStatement"}) // It already is, but PMD is confused and reporting error in unrelated locations.
public final class YamlRunner {

    private static final Logger logger = LogManager.getLogger(YamlRunner.class);

    static final String TEST_CI = "tests.ci";
    static final String TEST_NIGHTLY = "tests.nightly";
    static final String TEST_SEED = "tests.yaml.seed";
    static final String TEST_NIGHTLY_REPETITION = "tests.yaml.iterations";

    @Nonnull
    private final String resourcePath;

    @Nonnull
    private final YamlExecutionContext executionContext;

    public YamlRunner(@Nonnull String resourcePath, @Nonnull YamlConnectionFactory factory,
                      @Nonnull final YamlExecutionContext.ContextOptions additionalOptions) throws RelationalException {
        this.resourcePath = resourcePath;
        this.executionContext = new YamlExecutionContext(resourcePath, factory, additionalOptions);
    }

    public void run() throws Exception {
        try {
            LoaderOptions loaderOptions = new LoaderOptions();
            loaderOptions.setAllowDuplicateKeys(true);
            DumperOptions dumperOptions = new DumperOptions();
            final var yaml = new Yaml(new CustomYamlConstructor(loaderOptions), new Representer(dumperOptions),
                    new DumperOptions(), loaderOptions, new Resolver());

            final var testBlocks = new ArrayList<TestBlock>();
            int blockNumber = 0;
            try (var inputStream = getInputStream(resourcePath)) {
                for (var doc : yaml.loadAll(inputStream)) {
                    final var block = Block.parse(doc, blockNumber, executionContext);
                    logger.debug("⚪️ Executing block at line {} in {}", block.getLineNumber(), resourcePath);
                    block.execute();
                    if (block instanceof TestBlock) {
                        testBlocks.add((TestBlock)block);
                    }
                    blockNumber++;
                }
            }
            for (var block : executionContext.getFinalizeBlocks()) {
                logger.debug("⚪️ Executing finalizing block for block at line {} in {}", block.getLineNumber(), resourcePath);
                block.execute();
            }
            evaluateTestBlockResults(testBlocks);
            replaceTestFileIfRequired();
            replaceMetricsFileIfRequired();
        } catch (RelationalException | IOException e) {
            logger.error("‼️ running test file '{}' was not successful", resourcePath, e);
            throw e;
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
                logger.error("🔴 TestBlock {}/{} (starting at line {}) fails", i + 1, testBlocks.size(), block.getLineNumber());
                logger.error("--------------------------------------------------------------------------------------------------------------");
                logger.error("Error:", failureInBlock);
                logger.error("--------------------------------------------------------------------------------------------------------------");
                failure = failure == null ? failureInBlock : failure;
            }
        }
        if (failure != null) {
            logger.error("⚠️ Some TestBlocks in {} do not pass. ", resourcePath);
            throw failure;
        } else {
            logger.info("🟢 All tests in {} pass successfully.", resourcePath);
        }
    }

    @SpotBugsSuppressWarnings(value = "NP_NONNULL_RETURN_VIOLATION", justification = "should never happen, fail throws")
    @Nonnull
    private static InputStream getInputStream(@Nonnull final String resourcePath) throws RelationalException {
        final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        InputStream inputStream = classLoader.getResourceAsStream(resourcePath);
        Assert.notNull(inputStream, String.format(Locale.ROOT, "could not find '%s' in resources bundle", resourcePath));
        return inputStream;
    }

    private void replaceTestFileIfRequired() {
        if (executionContext.getEditedFileStream() == null || !executionContext.isDirty()) {
            return;
        }
        try {
            try (var writer = new PrintWriter(new FileWriter(Path.of(System.getProperty("user.dir")).resolve(Path.of("src", "test", "resources", resourcePath)).toAbsolutePath().toString(), StandardCharsets.UTF_8))) {
                for (var line : executionContext.getEditedFileStream()) {
                    writer.println(line);
                }
            }
            logger.info("🟢 Source file {} replaced.", resourcePath);
        } catch (IOException e) {
            logger.error("⚠️ Source file {} could not be replaced with corrected file.", resourcePath);
            Assertions.fail(e);
        }
    }

    private void replaceMetricsFileIfRequired() throws RelationalException {
        if (!executionContext.isDirtyMetrics()) {
            return;
        }
        executionContext.saveMetricsAsBinaryProto();
        executionContext.saveMetricsAsYaml();
    }
}
