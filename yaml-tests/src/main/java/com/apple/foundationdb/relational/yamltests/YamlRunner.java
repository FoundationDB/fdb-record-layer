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

import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.util.Assert;
import com.apple.foundationdb.relational.util.SpotBugsSuppressWarnings;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Assertions;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.representer.Representer;
import org.yaml.snakeyaml.resolver.Resolver;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@SuppressWarnings({"PMD.GuardLogStatement"}) // It already is, but PMD is confused and reporting error in unrelated locations.
public final class YamlRunner {

    private static final Logger logger = LogManager.getLogger(YamlRunner.class);

    private static final String TEST_NIGHTLY = "yaml_testing_nightly";
    private static final String TEST_SEED = "yaml_testing_seed";
    private static final String TEST_NIGHTLY_REPETITION = "yaml_testing_nightly_repetition";
    private static final String TEST_MAX_THREADS = "yaml_testing_max_threads";

    @Nonnull
    private final String resourcePath;

    @Nonnull
    private final YamlExecutionContext executionContext;

    public interface YamlConnectionFactory {
        RelationalConnection getNewConnection(@Nonnull URI connectPath) throws SQLException;
    }

    public static final class YamlExecutionContext {

        @Nullable
        private final List<String> editedFileStream;
        private boolean isDirty;
        @Nonnull
        private final YamlConnectionFactory connectionFactory;

        private YamlExecutionContext(@Nonnull String resourcePath, @Nonnull YamlConnectionFactory factory, boolean correctExplain) throws RelationalException {
            this.connectionFactory = factory;
            this.editedFileStream = correctExplain ? loadFileToMemory(resourcePath) : null;
            if (isNightly()) {
                logger.info("ℹ️ Running in the NIGHTLY context.");
                logger.info("ℹ️ Number of threads to be used for parallel execution " + getNumThreads());
                getNightlyRepetition().ifPresent(rep -> logger.info("ℹ️ Running with high repetition value set to " + rep));
            }
        }

        @Nonnull
        YamlConnectionFactory getConnectionFactory() {
            return connectionFactory;
        }

        boolean shouldCorrectExplains() {
            return editedFileStream != null;
        }

        boolean correctExplain(int lineNumber, @Nonnull String actual) {
            if (!shouldCorrectExplains()) {
                return false;
            }
            try {
                editedFileStream.set(lineNumber, "      - explain: \"" + actual + "\"");
                isDirty = true;
                return true;
            } catch (Exception e) {
                return false;
            }
        }

        boolean isNightly() {
            return System.getProperties().stringPropertyNames().contains(TEST_NIGHTLY);
        }

        Optional<String> getSeed() {
            return Optional.ofNullable(System.getProperty(TEST_SEED, null));
        }

        Optional<String> getNightlyRepetition() {
            return Optional.ofNullable(System.getProperty(TEST_NIGHTLY_REPETITION, null));
        }

        int getNumThreads() {
            var numThreads = 1;
            if (System.getProperties().stringPropertyNames().contains(TEST_MAX_THREADS)) {
                numThreads = Integer.parseInt(System.getProperty(TEST_MAX_THREADS));
                Assert.thatUnchecked(numThreads > 0, "Invalid number of threads provided in the YamlExecutionContext");
            }
            return numThreads;
        }
    }

    public YamlRunner(@Nonnull String resourcePath, @Nonnull YamlConnectionFactory factory, boolean correctExplain) throws RelationalException {
        this.resourcePath = resourcePath;
        this.executionContext = new YamlExecutionContext(resourcePath, factory, correctExplain);
    }

    public void run() throws Exception {
        LoaderOptions loaderOptions = new LoaderOptions();
        loaderOptions.setAllowDuplicateKeys(true);
        DumperOptions dumperOptions = new DumperOptions();
        final var yaml = new Yaml(new CustomYamlConstructor(loaderOptions), new Representer(dumperOptions), new DumperOptions(), loaderOptions, new Resolver());

        final var documents = new ArrayList<>();
        try (var inputStream = getInputStream(resourcePath)) {
            yaml.loadAll(inputStream).forEach(documents::add);
        }
        Assert.thatUnchecked(documents.size() >= 2, "Illegal Format: File has less than minimum 2 required documents.");

        // setup block
        Assert.thatUnchecked(Block.isConfigBlock(documents.get(0)), "Illegal Format: The first document in the file is required to be a Setup block.");
        executeConfigBlock(documents.get(0));

        final var testBlockResults = new ArrayList<Pair<Integer, Optional<Throwable>>>();
        for (int i = 1; i < documents.size() - 1; i++) {
            final var document = documents.get(i);
            if (Block.isConfigBlock(document)) {
                executeConfigBlock(document);
            } else {
                executeTestBlock(document, testBlockResults);
            }
        }

        // destruct block
        Assert.thatUnchecked(Block.isConfigBlock(documents.get(documents.size() - 1)), "Illegal Format: The last document in the file is required to be a destruct block.");
        executeConfigBlock(documents.get(documents.size() - 1));

        evaluateTestBlockResults(testBlockResults);
        replaceTestFileIfRequired();
    }

    private void executeConfigBlock(@Nonnull Object document) {
        final var block = Block.parse(document, executionContext);
        logger.debug("⚪️ Executing `config` block at line {} in {}", block.getLineNumber(), resourcePath);
        block.execute();
    }

    private void executeTestBlock(@Nonnull Object document, List<Pair<Integer, Optional<Throwable>>> testBlockResults) {
        final var block = Block.parse(document, executionContext);
        Assert.thatUnchecked(block instanceof Block.TestBlock, "Expect the block to be a test_block at line " + block.getLineNumber());
        logger.debug("⚪️ Executing `test` block at line {} in {}", block.getLineNumber(), resourcePath);
        block.execute();
        testBlockResults.add(Pair.of(block.getLineNumber(), block.getThrowableIfExists()));
    }

    private void evaluateTestBlockResults(List<Pair<Integer, Optional<Throwable>>> testBlockResults) {
        int failures = 0;
        logger.info("");
        logger.info("");
        logger.info("--------------------------------------------------------------------------------------------------------------");
        logger.info("TEST RESULTS");
        logger.info("--------------------------------------------------------------------------------------------------------------");

        for (int i = 0; i < testBlockResults.size(); i++) {
            final var result = testBlockResults.get(i);
            if (result.getRight().isEmpty()) {
                logger.info("🟢 TestBlock {}/{} runs successfully", i + 1, testBlockResults.size());
            } else {
                logger.error("🔴 TestBlock {}/{} (at line {}) fails", i + 1, testBlockResults.size(), result.getLeft());
                logger.error("--------------------------------------------------------------------------------------------------------------");
                logger.error("Error:", result.getRight().get());
                logger.error("--------------------------------------------------------------------------------------------------------------");
                failures++;
            }
        }
        if (failures > 0) {
            logger.error("⚠️ Some TestBlocks in {} do not pass.", resourcePath);
            Assertions.fail();
        } else {
            logger.info("🟢 All tests in {} pass successfully.", resourcePath);
        }
    }

    @SpotBugsSuppressWarnings(value = "NP_NONNULL_RETURN_VIOLATION", justification = "should never happen, fail throws")
    @Nonnull
    private static InputStream getInputStream(@Nonnull final String resourcePath) throws RelationalException {
        final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        InputStream inputStream = classLoader.getResourceAsStream(resourcePath);
        Assert.notNull(inputStream, String.format("could not find '%s' in resources bundle", resourcePath));
        return inputStream;
    }

    private void replaceTestFileIfRequired() {
        if (executionContext.editedFileStream == null || !executionContext.isDirty) {
            return;
        }
        try {
            try (var writer = new PrintWriter(new FileWriter(Path.of(System.getProperty("user.dir")).resolve(Path.of("src", "test", "resources", resourcePath)).toAbsolutePath().toString(), StandardCharsets.UTF_8))) {
                for (var line : executionContext.editedFileStream) {
                    writer.println(line);
                }
            }
            logger.info("🟢 Source file {} replaced.", resourcePath);
        } catch (IOException e) {
            logger.error("⚠️ Source file {} could not be replaced with corrected file.", resourcePath);
            Assertions.fail(e);
        }
    }

    @Nonnull
    private static List<String> loadFileToMemory(@Nonnull final String resourcePath) throws RelationalException {
        final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        final List<String> inMemoryFile = new ArrayList<>();
        try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(classLoader.getResourceAsStream(resourcePath), StandardCharsets.UTF_8))) {
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                inMemoryFile.add(line);
            }
        } catch (IOException e) {
            throw new RelationalException(ErrorCode.INTERNAL_ERROR, e);
        }
        return inMemoryFile;
    }
}
