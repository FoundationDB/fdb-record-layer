/*
 * YamlExecutionContext.java
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

import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.util.Assert;
import com.apple.foundationdb.relational.yamltests.block.Block;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@SuppressWarnings({"PMD.GuardLogStatement"}) // It already is, but PMD is confused and reporting error in unrelated locations.
public final class YamlExecutionContext {

    private static final Logger logger = LogManager.getLogger(YamlRunner.class);

    @Nullable
    private final List<String> editedFileStream;
    private boolean isDirty;
    @Nonnull
    private final YamlRunner.YamlConnectionFactory connectionFactory;
    @Nonnull
    private final List<Block> blocks = new ArrayList<>();
    private final List<Block> finalizeBlocks = new ArrayList<>();
    private final List<String> connectionPaths = new ArrayList<>();

    YamlExecutionContext(@Nonnull String resourcePath, @Nonnull YamlRunner.YamlConnectionFactory factory, boolean correctExplain) throws RelationalException {
        this.connectionFactory = factory;
        this.editedFileStream = correctExplain ? loadFileToMemory(resourcePath) : null;
        if (isNightly()) {
            logger.info("ℹ️ Running in the NIGHTLY context.");
            logger.info("ℹ️ Number of threads to be used for parallel execution " + getNumThreads());
            getNightlyRepetition().ifPresent(rep -> logger.info("ℹ️ Running with high repetition value set to " + rep));
        }
    }

    @Nonnull
    public YamlRunner.YamlConnectionFactory getConnectionFactory() {
        return connectionFactory;
    }

    public boolean shouldCorrectExplains() {
        return editedFileStream != null;
    }

    public boolean correctExplain(int lineNumber, @Nonnull String actual) {
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

    public boolean isNightly() {
        return Boolean.parseBoolean(System.getProperty(YamlRunner.TEST_NIGHTLY, "false"));
    }

    public Optional<Long> getSeed() {
        final var maybeValue = System.getProperty(YamlRunner.TEST_SEED, null);
        if (maybeValue != null) {
            return Optional.of(Long.parseLong(maybeValue));
        }
        return Optional.empty();
    }

    public Optional<Integer> getNightlyRepetition() {
        final var maybeValue = System.getProperty(YamlRunner.TEST_NIGHTLY_REPETITION, null);
        if (maybeValue != null) {
            return Optional.of(Integer.parseInt(maybeValue));
        }
        return Optional.empty();
    }

    public int getNumThreads() {
        var numThreads = 1;
        if (System.getProperties().stringPropertyNames().contains(YamlRunner.TEST_MAX_THREADS)) {
            numThreads = Integer.parseInt(System.getProperty(YamlRunner.TEST_MAX_THREADS));
            Assert.thatUnchecked(numThreads > 0, "Invalid number of threads provided in the YamlExecutionContext");
        }
        return numThreads;
    }

    boolean isDirty() {
        return isDirty;
    }

    @Nullable
    List<String> getEditedFileStream() {
        return editedFileStream;
    }

    public void registerBlock(@Nonnull Block block) {
        this.blocks.add(block);
    }

    public void registerFinalizeBlock(@Nonnull Block block) {
        this.finalizeBlocks.add(block);
    }

    public void registerConnectionPath(@Nonnull String path) {
        this.connectionPaths.add(path);
    }

    @Nonnull
    public String getOnlyConnectionPath() {
        Assert.thatUnchecked(!connectionPaths.isEmpty(), ErrorCode.INTERNAL_ERROR, () -> "Requested single connection path, but none present");
        Assert.thatUnchecked(connectionPaths.size() == 1, ErrorCode.INTERNAL_ERROR,
                () -> "Requested single connection path, but multiple paths are available to choose from: " + String.join(", ", connectionPaths));
        return connectionPaths.get(0);
    }

    @Nonnull
    public String getConnectionPath(int idx) {
        if (idx == 0) {
            return "jdbc:embed:/__SYS?schema=CATALOG";
        }
        Assert.thatUnchecked(idx <= connectionPaths.size(), ErrorCode.INTERNAL_ERROR,
                () -> String.format("Requested connection path at index %d, but only have %d available connection paths.", idx, connectionPaths.size()));
        return connectionPaths.get(idx - 1);
    }

    @Nonnull
    public List<Block> getBlocks() {
        return blocks;
    }

    @Nonnull
    public List<Block> getFinalizeBlocks() {
        return finalizeBlocks;
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
