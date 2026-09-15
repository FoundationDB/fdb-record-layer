/*
 * JsonExtensionMergerTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2026 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.yamltests.command;

import com.apple.foundationdb.relational.api.metadata.Column;
import com.apple.foundationdb.relational.api.metadata.DataType;
import com.apple.foundationdb.relational.api.metadata.SchemaTemplate;
import com.apple.foundationdb.relational.api.metadata.Table;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the proto2 extensions that {@link JsonExtensionMerger} re-attaches to metadata loaded from JSON. The
 * extension used here carries the precision and dimensions of a vector column, which is the one the metadata cannot do
 * without: lose it and the column is a plain {@code bytes} field.
 * <p>
 * The keys below are spelled in snake case, the form a hand-written file tends to use, where
 * {@code vector-metadata.yamsql}'s fixture uses the camel case that {@code JsonFormat}'s printer emits. Both spellings
 * have to resolve, so the divergence is deliberate.
 * </p>
 */
class JsonExtensionMergerTest {
    /** Metadata for two record types, each with one {@code bytes} column whose {@code options} the caller supplies. */
    private static final String METADATA = """
            {
              "records": {
                "dependency": ["record_metadata_options.proto"],
                "message_type": [
                  {
                    "name": "RecordTypeUnion",
                    "field": [
                      {"name": "_FIRST", "number": 1, "type": "TYPE_MESSAGE", "type_name": "FIRST"},
                      {"name": "_SECOND", "number": 2, "type": "TYPE_MESSAGE", "type_name": "SECOND"}
                    ]
                  },
                  {
                    "name": "FIRST",
                    "field": [
                      {"name": "ID", "number": 1, "type": "TYPE_INT64"},
                      {"name": "EMBEDDING", "number": 2, "type": "TYPE_BYTES", "options": {%s}}
                    ]
                  },
                  {
                    "name": "SECOND",
                    "field": [
                      {"name": "ID", "number": 1, "type": "TYPE_INT64"},
                      {"name": "EMBEDDING", "number": 2, "type": "TYPE_BYTES", "options": {%s}}
                    ]
                  }
                ]
              },
              "record_types": [
                {"name": "FIRST", "primary_key": {"field": {"field_name": "ID", "fan_type": "SCALAR"}}},
                {"name": "SECOND", "primary_key": {"field": {"field_name": "ID", "fan_type": "SCALAR"}}}
              ]
            }
            """;

    @TempDir
    Path tempDir;

    /**
     * The two record types carry different dimensions, so this also pins that each element of a repeated message field
     * is walked against the element of the JSON array it was parsed from.
     */
    @Test
    void vectorOptionsSurviveTheParse() throws Exception {
        final SchemaTemplate template = loadTemplate(metadata(vectorOptions(3), vectorOptions(5)));

        assertThat(vectorColumn(template, "FIRST").getDimensions()).isEqualTo(3);
        assertThat(vectorColumn(template, "SECOND").getDimensions()).isEqualTo(5);
        assertThat(vectorColumn(template, "FIRST").getPrecision()).isEqualTo(32);
    }

    /**
     * A key naming neither a field nor an extension is left as the ordinary parse left it, so that metadata written by
     * a newer version still loads. Without the extension the column is the {@code bytes} field it is declared as, which
     * is what makes the assertions above evidence about this class rather than about the stack under it.
     */
    @Test
    void unknownKeyIsLeftAlone() throws Exception {
        final SchemaTemplate template = loadTemplate(metadata("\"notAnExtension\": {}", vectorOptions(5)));

        assertThat(columnType(template, "FIRST").getCode()).isEqualTo(DataType.Code.BYTES);
        assertThat(vectorColumn(template, "SECOND").getDimensions()).isEqualTo(5);
    }

    /**
     * A key that names an extension no resolved dependency declares cannot be told from one this version does not know,
     * except that a field name never contains a dot. Dropping it would be the silent loss this class exists to prevent.
     */
    @Test
    void unregisteredExtensionIsRejected() {
        assertThatThrownBy(() -> loadTemplate(metadata("\"com.example.nosuch.field\": {}", vectorOptions(5))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("com.example.nosuch.field");
    }

    @Nonnull
    private static String metadata(@Nonnull String firstOptions, @Nonnull String secondOptions) {
        return String.format(METADATA, firstOptions, secondOptions);
    }

    @Nonnull
    private static String vectorOptions(int dimensions) {
        return String.format("\"com.apple.foundationdb.record.field\": "
                + "{\"vectorOptions\": {\"precision\": 32, \"dimensions\": %d}}", dimensions);
    }

    @Nonnull
    private SchemaTemplate loadTemplate(@Nonnull String metadataJson) throws IOException {
        final Path jsonFile = Files.writeString(tempDir.resolve("metadata.json"), metadataJson,
                StandardCharsets.UTF_8);
        return CommandUtil.fromProto("EMBEDDINGS_TEMPLATE from " + jsonFile);
    }

    @Nonnull
    private static DataType.VectorType vectorColumn(@Nonnull SchemaTemplate template,
                                                    @Nonnull String tableName) throws Exception {
        final DataType type = columnType(template, tableName);
        assertThat(type).isInstanceOf(DataType.VectorType.class);
        return (DataType.VectorType) type;
    }

    @Nonnull
    private static DataType columnType(@Nonnull SchemaTemplate template, @Nonnull String tableName) throws Exception {
        final Table table = template.getTables().stream()
                .filter(candidate -> tableName.equals(candidate.getName()))
                .findFirst()
                .orElseThrow(() -> new AssertionError("no table named " + tableName));
        final Column column = table.getColumns().stream()
                .filter(candidate -> "EMBEDDING".equals(candidate.getName()))
                .findFirst()
                .orElseThrow(() -> new AssertionError("no EMBEDDING column on " + tableName));
        return column.getDataType();
    }
}
