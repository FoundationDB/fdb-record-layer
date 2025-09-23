/*
 * GitMetricsFileFinder.java
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

package com.apple.foundationdb.relational.yamltests;

import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Utility class for finding changed metrics files using git integration.
 * This class provides methods to identify metrics files that have changed between different git references.
 */
public final class GitMetricsFileFinder {
    private static final Logger logger = LoggerFactory.getLogger(GitMetricsFileFinder.class);

    private GitMetricsFileFinder() {
        // Utility class
    }

    /**
     * Finds all metrics YAML files that have changed between two git references.
     *
     * @param baseRef the base git reference (e.g., "main", "HEAD~1", commit SHA)
     * @param headRef the target git reference (e.g., "main", "HEAD~1", commit SHA)
     * @param repositoryRoot the root directory of the git repository
     * @return set of paths to changed metrics YAML files
     * @throws RelationalException if git command fails or repository is not valid
     */
    @Nonnull
    public static Set<Path> findChangedMetricsYamlFiles(@Nonnull final String baseRef,
                                                        @Nonnull final String headRef,
                                                        @Nonnull final Path repositoryRoot) throws RelationalException {
        final List<String> changedFiles = getChangedFiles(baseRef, headRef, repositoryRoot);
        final ImmutableSet.Builder<Path> metricsFiles = ImmutableSet.builder();

        for (final var filePath : changedFiles) {
            if (isMetricsYamlFile(filePath)) {
                metricsFiles.add(repositoryRoot.resolve(filePath));
            }
        }

        return metricsFiles.build();
    }

    /**
     * Gets a list of all files changed between two git references.
     *
     * @param baseRef the base reference
     * @param headRef the target reference
     * @param repositoryRoot the repository root directory
     * @return list of relative file paths that have changed
     * @throws RelationalException if git command fails
     */
    @Nonnull
    private static List<String> getChangedFiles(@Nonnull final String baseRef,
                                                @Nonnull final String headRef,
                                                @Nonnull final Path repositoryRoot) throws RelationalException {
        final var command = List.of("git", "diff", "--name-only", baseRef + "..." + headRef);
        try {
            if (logger.isDebugEnabled()) {
                logger.debug(KeyValueLogMessage.of("Executing git command",
                        "root", repositoryRoot,
                        "command", String.join(" ", command)));
            }
            final var processBuilder = new ProcessBuilder(command)
                    .directory(repositoryRoot.toFile())
                    .redirectErrorStream(true);

            final var process = processBuilder.start();
            final List<String> result = new ArrayList<>();

            try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
                for (String line = reader.readLine(); line != null; line = reader.readLine()) {
                    final String stripped = line.strip();
                    if (!stripped.isEmpty()) {
                        result.add(stripped);
                    }
                }
            }

            final var exitCode = process.waitFor();
            if (exitCode != 0) {
                throw new RelationalException(
                        "Git command failed with exit code " + exitCode + ": " + String.join(" ", command),
                        ErrorCode.INTERNAL_ERROR);
            }

            return result;
        } catch (final IOException | InterruptedException e) {
            throw new RelationalException("Failed to execute git command: " + String.join(" ", command), ErrorCode.INTERNAL_ERROR, e);
        }
    }

    /**
     * Checks if a file path represents a metrics YAML file.
     *
     * @param filePath the file path to check
     * @return true if the file is a metrics file
     */
    private static boolean isMetricsYamlFile(@Nonnull final String filePath) {
        return filePath.endsWith(".metrics.yaml");
    }

    /**
     * Gets the file contents for a specific git reference (commit).
     * This method checks out the file content at the specified reference and
     * saves it into a temporary file located at the returned {@link Path}.
     * If the file does not exist at the given reference, it will return a
     * {@code null} path.
     *
     * @param filePath the relative file path
     * @param gitRef the git reference
     * @param repositoryRoot the repository root
     * @return path to a temporary file containing the content at the specified reference
     *    or {@code null} if it didn't exist at the given ref
     * @throws RelationalException if git command fails
     */
    @Nullable
    public static Path getFileAtReference(@Nonnull final String filePath,
                                          @Nonnull final String gitRef,
                                          @Nonnull final Path repositoryRoot) throws RelationalException {
        try {
            final var tempFile = Files.createTempFile("metrics-diff-", gitRef + ".metrics.yaml");
            final var command = List.of("git", "show", gitRef + ":" + filePath);
            final var processBuilder = new ProcessBuilder(command)
                    .directory(repositoryRoot.toFile())
                    .redirectOutput(tempFile.toFile())
                    .redirectErrorStream(false);

            final var process = processBuilder.start();
            final var exitCode = process.waitFor();

            if (exitCode != 0) {
                // File could not be loaded. It was probably not present at that version, so return null
                if (logger.isDebugEnabled()) {
                    logger.debug(KeyValueLogMessage.of("Unable to get file contents at reference",
                            "path", filePath,
                            "ref", gitRef));
                }
                Files.deleteIfExists(tempFile);
                return null;
            }

            return tempFile;
        } catch (final IOException | InterruptedException e) {
            throw new RelationalException(
                    "Failed to get file content at reference " + gitRef + ":" + filePath, ErrorCode.INTERNAL_ERROR, e);
        }
    }

    /**
     * Get the commit hash for a given reference. This is to construct the commit values for use in things
     * like permanent URLs.
     *
     * @param repositoryRoot the directory to run the git process from
     * @param refName a git reference (e.g., branch name, "HEAD", etc.)
     * @return the current commit hash
     * @throws RelationalException if git commit fails
     */
    @Nonnull
    public static String getCommitHash(@Nonnull Path repositoryRoot, @Nonnull String refName) throws RelationalException {
        try {
            final var command = List.of("git", "rev-parse", refName);
            final var processBuilder = new ProcessBuilder(command)
                    .directory(repositoryRoot.toFile())
                    .redirectErrorStream(false);

            final var process = processBuilder.start();
            final var exitCode = process.waitFor();
            final String ref = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8).strip();

            if (exitCode != 0) {
                throw new RelationalException(
                        "Git rev-parse command failed with exit code " + exitCode,
                        ErrorCode.INTERNAL_ERROR);
            }

            return ref;
        } catch (final IOException | InterruptedException e) {
            throw new RelationalException(
                    "Failed to get current head reference", ErrorCode.INTERNAL_ERROR, e);
        }
    }
}
