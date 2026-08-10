/*
 * YamlFilesMaintainer.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.relational.yamltests.command.queryconfigs.CheckResultMetadataConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Assertions;

import javax.annotation.Nonnull;
import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Manages in-place corrections to {@code .yamsql} source files during a maintenance test run.
 *
 * <p>When a maintenance config (e.g. {@code CorrectExplains}, {@code AddExplains}) is active,
 * this class loads each {@code .yamsql} file into an in-memory line buffer at test startup.
 * As tests execute, correction requests ({@link #correctExplain}, {@link #addExplain},
 * {@link #correctResultMetadata}, {@link #addResultMetadata}) queue {@link YamlCorrection}
 * objects against that buffer. At teardown, {@link #saveIfNeeded()} sorts the pending
 * corrections in descending line-number order — so later edits do not shift the offsets of
 * earlier ones — applies them to the buffer, and writes the result back to the source tree.
 */
@SuppressWarnings({"PMD.GuardLogStatement"})
public class YamlFilesMaintainer {
    private static final Logger logger = LogManager.getLogger(YamlFilesMaintainer.class);

    @Nonnull
    private final Map<YamlReference.YamlResource, List<String>> editedFileStream = new HashMap<>();
    @Nonnull
    private final Map<YamlReference.YamlResource, Boolean> isDirty = new HashMap<>();
    /**
     * Pending corrections (explain and result-metadata), buffered so they can be applied in descending
     * line-number order to avoid stale-offset corruption when multiple corrections target the same file.
     */
    @Nonnull
    private final Map<YamlReference.YamlResource, List<YamlCorrection>> pendingCorrections = new HashMap<>();

    public void loadFile(@Nonnull YamlReference.YamlResource resource) throws RelationalException {
        this.editedFileStream.put(resource, loadYamlResource(resource));
    }

    private void verifyFileLoaded(@Nonnull final YamlReference reference) {
        if (editedFileStream.get(reference.getResource()) == null) {
            throw new IllegalStateException("‼️ YAMSQL file not loaded for resource: " + reference.getResource());
        }
    }

    public void correctResultMetadata(@Nonnull final YamlReference reference,
                                      @Nonnull final List<CheckResultMetadataConfig.ColumnDescriptor> actualColumns) {
        verifyFileLoaded(reference);
        synchronized (this) {
            pendingCorrections
                    .computeIfAbsent(reference.getResource(), k -> new ArrayList<>())
                    .add(new MetadataCorrection(reference, new ArrayList<>(actualColumns)));
            isDirty.put(reference.getResource(), true);
        }
    }

    public void addResultMetadata(@Nonnull final YamlReference queryReference,
                                  @Nonnull final List<CheckResultMetadataConfig.ColumnDescriptor> actualColumns) {
        verifyFileLoaded(queryReference);
        synchronized (this) {
            final List<YamlCorrection> corrections = pendingCorrections
                    .computeIfAbsent(queryReference.getResource(), k -> new ArrayList<>());
            // Deduplicate: if a correction for this query line is already pending, skip.
            // The query could run multiple times depending on repetition, and each of the run calls addResultMetadata independently.
            // Since the YAML file is parsed only once at the start, all runs see the same parsed config with no resultMetadata yet,
            // and all queue a correction. The deduplication check prevents all but the first correct from being added.
            final int lineNumber = queryReference.getLineNumber();
            final boolean alreadyPending = corrections.stream()
                    .anyMatch(c -> c instanceof YamlFilesMaintainer.AddMetadataCorrection && c.getLineNumber() == lineNumber);
            if (!alreadyPending) {
                corrections.add(new YamlFilesMaintainer.AddMetadataCorrection(queryReference, new ArrayList<>(actualColumns)));
                isDirty.put(queryReference.getResource(), true);
            }
        }
    }

    public void correctExplain(@Nonnull final YamlReference reference, @Nonnull String actual) {
        verifyFileLoaded(reference);
        synchronized (this) {
            pendingCorrections
                    .computeIfAbsent(reference.getResource(), k -> new ArrayList<>())
                    .add(new ExplainCorrection(reference, actual));
            isDirty.put(reference.getResource(), true);
        }
    }

    public void addExplain(@Nonnull final YamlReference queryReference, @Nonnull String actual) {
        verifyFileLoaded(queryReference);
        synchronized (this) {
            final List<YamlCorrection> corrections = pendingCorrections
                    .computeIfAbsent(queryReference.getResource(), k -> new ArrayList<>());
            final int lineNumber = queryReference.getLineNumber();
            final boolean alreadyPending = corrections.stream()
                    .anyMatch(c -> c instanceof YamlFilesMaintainer.AddExplainCorrection && c.getLineNumber() == lineNumber);
            if (!alreadyPending) {
                corrections.add(new YamlFilesMaintainer.AddExplainCorrection(queryReference, actual));
                isDirty.put(queryReference.getResource(), true);
            }
        }
    }

    private void applyPendingCorrections(@Nonnull final YamlReference.YamlResource resource) {
        final List<YamlCorrection> corrections = pendingCorrections.get(resource);
        if (corrections == null || corrections.isEmpty()) {
            return;
        }
        final List<String> lines = editedFileStream.get(resource);
        if (lines == null) {
            return;
        }
        // Sort descending by line number so each edit only shifts lines that have already been processed.
        corrections.sort(Comparator.comparingInt(c -> -c.getLineNumber()));
        for (final YamlCorrection correction : corrections) {
            correction.apply(lines);
        }
    }

    public void saveIfNeeded() {
        final var filePathsWithResourceCount = editedFileStream.keySet().stream()
                .map(YamlReference.YamlResource::getPath)
                .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
        for (final var resource: editedFileStream.keySet()) {
            if (!isDirty.getOrDefault(resource, false)) {
                continue;
            }
            // There could arise a common scenario where a YAMSQL file is "opened" as 2 separate resource, coming from
            // different call stacks. If this file has an EXPLAIN, and warrants correction, that will be a problem if,
            // for the 2 resources pointing to same file, there is some disagreement on the values. Ideally this should
            // not happen however, I believe it's still a possibility, mainly with metrics, to be highly sensitive to
            // the environment in which the query is running. Because of this reason, just fail if we found resources
            // that are marked as dirty and pointing to the same file as any other (dirty or non-dirty) resource.
            if (filePathsWithResourceCount.getOrDefault(resource.getPath(), 0L) > 1) {
                Assertions.fail("Found duplicate entries for writing to file: " + resource.getPath());
            }
            // Apply buffered corrections (explain + result-metadata) in descending line-number order before saving.
            applyPendingCorrections(resource);
            saveYamlFile(resource);
        }
    }

    private void saveYamlFile(@Nonnull final YamlReference.YamlResource resource) {
        try {
            try (var writer = new PrintWriter(new FileWriter(Path.of(System.getProperty("user.dir")).resolve(Path.of("src", "test", "resources", resource.getPath())).toAbsolutePath().toString(), StandardCharsets.UTF_8))) {
                for (var line : editedFileStream.get(resource)) {
                    writer.println(line);
                }
            }
            logger.info("🟢 Source file {} replaced.", resource.getPath());
        } catch (IOException e) {
            logger.error("⚠️ Source file {} could not be replaced with corrected file.", resource.getPath());
            Assertions.fail(e);
        }
    }

    @Nonnull
    private static List<String> loadYamlResource(@Nonnull final YamlReference.YamlResource resource) throws RelationalException {
        final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        final List<String> inMemoryFile = new ArrayList<>();
        try (BufferedReader bufferedReader =
                     new BufferedReader(
                             new InputStreamReader(Objects.requireNonNull(classLoader.getResourceAsStream(resource.getPath())),
                                     StandardCharsets.UTF_8))) {
            for (String line = bufferedReader.readLine(); line != null; line = bufferedReader.readLine()) {
                inMemoryFile.add(line);
            }
        } catch (IOException e) {
            throw new RelationalException(ErrorCode.INTERNAL_ERROR, e);
        }
        return inMemoryFile;
    }

    static final class ExplainCorrection implements YamlCorrection {
        @Nonnull
        private final YamlReference reference;
        @Nonnull
        private final String actual;

        ExplainCorrection(@Nonnull final YamlReference reference, @Nonnull final String actual) {
            this.reference = reference;
            this.actual = actual;
        }

        @Override
        public int getLineNumber() {
            return reference.getLineNumber();
        }

        @Override
        public void apply(@Nonnull final List<String> lines) {
            final int idx = reference.getLineNumber() - 1;
            if (idx >= 0 && idx < lines.size()) {
                final String itemPrefix = " ".repeat(indentOf(lines.get(idx)));
                lines.set(idx, itemPrefix + "- explain: \"" + actual + "\"");
            }
        }
    }

    static final class AddExplainCorrection implements YamlCorrection {
        @Nonnull
        private final YamlReference queryReference;
        @Nonnull
        private final String actual;

        public AddExplainCorrection(@Nonnull final YamlReference queryReference, @Nonnull final String actual) {
            this.queryReference = queryReference;
            this.actual = actual;
        }

        @Override
        public int getLineNumber() {
            return queryReference.getLineNumber();
        }

        @Override
        public void apply(@Nonnull final List<String> lines) {
            final int queryLineIdx = queryReference.getLineNumber() - 1; // 1-based → 0-based
            if (queryLineIdx < 0 || queryLineIdx >= lines.size()) {
                return;
            }
            final String itemPrefix = " ".repeat(indentOf(lines.get(queryLineIdx)));
            // Scan forward past any query-string continuation lines to find the first config entry
            // at the same indentation level, and insert the explain line before it.
            final int insertIdx = findInsertionPoint(lines, queryLineIdx + 1, itemPrefix,
                    line -> line.startsWith(itemPrefix + "- "));
            lines.add(insertIdx, itemPrefix + "- explain: \"" + actual + "\"");
        }
    }

    static final class MetadataCorrection implements YamlCorrection {
        @Nonnull
        private final YamlReference reference;
        @Nonnull
        private final List<CheckResultMetadataConfig.ColumnDescriptor> actualColumns;

        MetadataCorrection(@Nonnull final YamlReference reference,
                           @Nonnull final List<CheckResultMetadataConfig.ColumnDescriptor> actualColumns) {
            this.reference = reference;
            this.actualColumns = actualColumns;
        }

        @Override
        public int getLineNumber() {
            return reference.getLineNumber();
        }

        @Override
        public void apply(@Nonnull final List<String> lines) {
            final int startIdx = reference.getLineNumber() - 1; // 1-based → 0-based
            if (startIdx < 0 || startIdx >= lines.size()) {
                return;
            }
            final String startLine = lines.get(startIdx);
            final int indent = indentOf(startLine);

            // Build replacement lines
            final List<String> newLines = new ArrayList<>();
            final String itemPrefix = " ".repeat(indent);
            final String cols = actualColumns.stream().map(YamlFilesMaintainer::buildInlineDescriptor)
                    .collect(Collectors.joining(", "));
            newLines.add(itemPrefix + "- resultMetadata: [" + cols + "]");

            // Find the end of the existing resultMetadata block
            int endIdx = startIdx + 1;
            while (endIdx < lines.size()) {
                final String line = lines.get(endIdx);
                if (line.isBlank()) {
                    endIdx++;
                    continue;
                }
                if (indentOf(line) > indent) {
                    endIdx++;
                } else {
                    break;
                }
            }

            lines.subList(startIdx, endIdx).clear();
            lines.addAll(startIdx, newLines);
        }
    }

    static final class AddMetadataCorrection implements YamlCorrection {
        @Nonnull
        private final YamlReference queryReference;
        @Nonnull
        private final List<CheckResultMetadataConfig.ColumnDescriptor> actualColumns;

        AddMetadataCorrection(@Nonnull final YamlReference queryReference,
                              @Nonnull final List<CheckResultMetadataConfig.ColumnDescriptor> actualColumns) {
            this.queryReference = queryReference;
            this.actualColumns = actualColumns;
        }

        @Override
        public int getLineNumber() {
            return queryReference.getLineNumber();
        }

        @Override
        public void apply(@Nonnull final List<String> lines) {
            final int queryLineIdx = queryReference.getLineNumber() - 1; // 1-based → 0-based
            if (queryLineIdx < 0 || queryLineIdx >= lines.size()) {
                return;
            }
            final String itemPrefix = " ".repeat(indentOf(lines.get(queryLineIdx)));
            final String cols = actualColumns.stream().map(YamlFilesMaintainer::buildInlineDescriptor)
                    .collect(Collectors.joining(", "));
            // Insert just before the first result:/unorderedResult: config, so that explain:/planHash: lines
            // that follow the query line are not displaced. Falls back to right after the query line.
            final int insertIdx = findInsertionPoint(lines, queryLineIdx + 1, itemPrefix,
                    line -> line.startsWith(itemPrefix + "- result:") || line.startsWith(itemPrefix + "- unorderedResult:"));
            lines.add(insertIdx, itemPrefix + "- resultMetadata: [" + cols + "]");
        }
    }

    private static int indentOf(@Nonnull final String line) {
        int indent = 0;
        while (indent < line.length() && line.charAt(indent) == ' ') {
            indent++;
        }
        return indent;
    }

    private static int findInsertionPoint(@Nonnull final List<String> lines, final int startIdx,
                                  @Nonnull final String itemPrefix,
                                  @Nonnull final Predicate<String> stopAt) {
        for (int i = startIdx; i < lines.size(); i++) {
            final String line = lines.get(i);
            if (stopAt.test(line)) {
                return i;
            }
            if (!itemPrefix.isEmpty() && line.length() >= itemPrefix.length() && !line.startsWith(itemPrefix)) {
                break;
            }
        }
        return startIdx;
    }

    /**
     * Builds a single-line inline YAML representation for a {@link CheckResultMetadataConfig.ColumnDescriptor}.
     * <ul>
     *   <li>Scalar column: {@code {NAME: TYPE}}</li>
     *   <li>Struct column (no type name): {@code {NAME: [{FIELD: TYPE}, ...]}}</li>
     *   <li>Struct column (with type name): {@code {NAME: [structTypeName, {FIELD: TYPE}, ...]}}</li>
     *   <li>Array-of-scalar column: {@code {NAME: {array: TYPE}}}</li>
     *   <li>Array-of-array column: {@code {NAME: {array: {array: TYPE}}}}</li>
     *   <li>Array-of-struct column (no type name): {@code {NAME: {array: [{FIELD: TYPE}, ...]}}}</li>
     *   <li>Array-of-struct column (with type name): {@code {NAME: {array: [structTypeName, {FIELD: TYPE}, ...]}}}</li>
     * </ul>
     */
    private static String buildInlineDescriptor(@Nonnull final CheckResultMetadataConfig.ColumnDescriptor col) {
        if (col.isArray && col.fields != null) {
            final String typePrefix = col.structTypeName != null ? col.structTypeName + ", " : "";
            final String fields = col.fields.stream().map(YamlFilesMaintainer::buildInlineDescriptor)
                    .collect(Collectors.joining(", "));
            return "{" + col.name + ": {array: [" + typePrefix + fields + "]}}";
        } else if (col.fields != null) {
            final String typePrefix = col.structTypeName != null ? col.structTypeName + ", " : "";
            final String fields = col.fields.stream().map(YamlFilesMaintainer::buildInlineDescriptor)
                    .collect(Collectors.joining(", "));
            return "{" + col.name + ": [" + typePrefix + fields + "]}";
        } else {
            return "{" + col.name + ": " + typeNameToInlineValue(col.typeName) + "}";
        }
    }

    /**
     * Converts an SQL array type name (e.g., {@code "ARRAY(INTEGER)"}) to its {@code {array: ...}} inline YAML
     * representation. Non-array type names are returned unchanged.
     * <ul>
     *   <li>{@code "ARRAY(INTEGER)"} → {@code "{array: INTEGER}"}</li>
     *   <li>{@code "ARRAY(ARRAY(INTEGER))"} → {@code "{array: {array: INTEGER}}"}</li>
     *   <li>{@code "BIGINT"} → {@code "BIGINT"}</li>
     * </ul>
     */
    private static String typeNameToInlineValue(@Nonnull final String typeName) {
        if (typeName.startsWith("ARRAY(") && typeName.endsWith(")")) {
            final String inner = typeName.substring(6, typeName.length() - 1);
            return "{array: " + typeNameToInlineValue(inner) + "}";
        }
        return typeName;
    }
}
