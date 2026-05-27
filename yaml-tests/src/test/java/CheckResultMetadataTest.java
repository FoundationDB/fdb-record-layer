/*
 * CheckResultMetadataTest.java
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

import com.apple.foundationdb.relational.yamltests.YamlExecutionContext;
import com.apple.foundationdb.relational.yamltests.YamlRunner;
import com.apple.foundationdb.relational.yamltests.configs.EmbeddedConfig;
import com.apple.foundationdb.relational.yamltests.configs.YamlTestConfig;
import com.apple.foundationdb.test.FDBTestEnvironment;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests that {@code resultMetadata} checks fail correctly when the expected metadata does not match
 * the actual result-set metadata, and pass when it does.
 * <p>
 *     Negative (shouldFail) tests are the most important: if a mismatch silently passes we would
 *     have false confidence in the coverage.
 * </p>
 */
public class CheckResultMetadataTest {

    private static final YamlTestConfig config = new EmbeddedConfig(FDBTestEnvironment.allClusterFiles());

    @BeforeAll
    static void beforeAll() throws Exception {
        config.beforeAll();
    }

    @AfterAll
    static void afterAll() throws Exception {
        config.afterAll();
    }

    private void doRun(String fileName) throws Exception {
        new YamlRunner(fileName, config.createConnectionFactory(), YamlExecutionContext.ContextOptions.EMPTY_OPTIONS).run();
    }

    // ── negative tests ────────────────────────────────────────────────────────

    /**
     * Files that must raise {@link YamlExecutionContext.YamlExecutionError} because the
     * {@code resultMetadata} block does not match what the query actually returns.
     */
    static Stream<String> shouldFail() {
        return Stream.of(
                "wrong-column-name",                      // expected "WRONG", actual "ID"
                "wrong-column-type",                      // expected INTEGER, actual BIGINT
                "missing-column",                         // expected 1 column, actual 2
                "extra-column",                           // expected 2 columns, actual 1
                "wrong-column-order",                     // columns listed in reversed order
                "wrong-nested-struct-name",               // struct column name wrong
                "wrong-nested-struct-type",               // struct column typed as BIGINT
                "wrong-metadata-on-continuation-page",   // inline check on continuation page fails
                "wrong-array-element-type",               // expected ARRAY(BIGINT), actual ARRAY(INTEGER)
                "wrong-struct-field-name",                // nested struct field name wrong
                "wrong-struct-field-type",                // nested struct field type wrong
                "wrong-array-of-struct-field-name",       // array-of-struct element field name wrong
                "wrong-array-of-struct-field-type",        // array-of-struct element field type wrong
                "wrong-struct-type-name",                  // struct type name does not match declared type
                "result-metadata-without-result"          // resultMetadata present but no consuming config — parse-time failure
        );
    }

    @ParameterizedTest
    @MethodSource("shouldFail")
    void shouldFail(String filename) {
        assertThrows(YamlExecutionContext.YamlExecutionError.class, () ->
                doRun("check-result-metadata/shouldFail/" + filename + ".yamsql"));
    }

    // ── positive tests ────────────────────────────────────────────────────────

    /**
     * Files that must complete without error because the {@code resultMetadata} block
     * correctly describes the query's result-set columns.
     */
    static Stream<String> shouldPass() {
        return Stream.of(
                "single-column",                   // one BIGINT column
                "multiple-columns",                // two columns, also exercises case-insensitive name match
                "nested-struct-column",            // struct column with nested field descriptors
                "metadata-on-continuation-page",  // single metadata block covers all continuation pages
                "empty-result-set",                // metadata check passes even with zero rows
                "array-column",                    // integer array column reported as {array: INTEGER}
                "array-of-struct-column",          // array-of-struct with nested element field descriptors
                "struct-type-name",                // struct type name as optional prefix in field list
                "field-named-array",               // struct field named "array" — no clash with {array: ...} map syntax
                "type-named-array"                 // struct type named "array" — no clash with {array: ...} map syntax
        );
    }

    @ParameterizedTest
    @MethodSource("shouldPass")
    void shouldPass(String filename) throws Exception {
        doRun("check-result-metadata/shouldPass/" + filename + ".yamsql");
    }

    // ── add-result-metadata test ──────────────────────────────────────────────

    /**
     * Runs a YAMSQL file that has no {@code resultMetadata:} block with
     * {@code OPTION_ADD_RESULT_METADATA}, then verifies that the runner wrote a
     * {@code resultMetadata:} line back into the file.  Any existing {@code resultMetadata:} line
     * is stripped at the start so the test is self-resetting.  Skipped in CI because correction
     * mode is not permitted there.
     */
    @Test
    void addResultMetadataInsertsMetadataBlockIntoFile() throws Exception {
        Assumptions.assumeFalse(YamlExecutionContext.isInCI(), "Skipped in CI: cannot modify YAMSQL files");

        final String resourcePath = "check-result-metadata/addResultMetadata/add-result-metadata.yamsql";
        final Path filePath = Path.of(System.getProperty("user.dir"), "src", "test", "resources", resourcePath);

        // Strip any resultMetadata: line left by a previous run so we always start without one.
        final List<String> original = Files.readAllLines(filePath, StandardCharsets.UTF_8);
        final List<String> stripped = original.stream()
                .filter(line -> !line.stripLeading().startsWith("- resultMetadata:"))
                .collect(Collectors.toList());
        Files.write(filePath, stripped, StandardCharsets.UTF_8);

        // Run with OPTION_ADD_RESULT_METADATA: the missing block is detected and written back.
        new YamlRunner(resourcePath, config.createConnectionFactory(),
                YamlExecutionContext.ContextOptions.of(YamlExecutionContext.OPTION_ADD_RESULT_METADATA, true)).run();

        // Verify the resultMetadata line is now present in the file.
        final List<String> updated = Files.readAllLines(filePath, StandardCharsets.UTF_8);
        assertTrue(updated.stream().anyMatch(line -> line.stripLeading().startsWith("- resultMetadata:")));
    }
}
