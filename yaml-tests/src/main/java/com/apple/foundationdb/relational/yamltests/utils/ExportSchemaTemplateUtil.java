/*
 * ExportSchemaTemplate.java
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

package com.apple.foundationdb.relational.yamltests.utils;

import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataProto;
import com.google.protobuf.util.JsonFormat;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * Utility to generate a JSON export of a schema template. The output of this file can then be commited
 * to the repository and then the referenced by a test using the {@code lode schema template} directive.
 * This allows the user to test a schema template that either wouldn't be generated from the DDL or to
 * verify that a certain serialized meta-data will be interpreted as expected at runtime.
 */
public class ExportSchemaTemplateUtil {
    private ExportSchemaTemplateUtil() {
    }

    /**
     * Export a {@link RecordMetaData} object to a file. This will overwrite an existing file with
     * the new meta-data if set.
     *
     * @param metaData meta-data to export
     * @param exportLocation path to export location
     * @throws IOException any problem encountered writing the file
     */
    public static void export(@Nonnull RecordMetaData metaData, @Nonnull Path exportLocation) throws IOException {
        final RecordMetaDataProto.MetaData metaDataProto = metaData.toProto();

        // Ensure parent directory exists
        Path parentDir = exportLocation.getParent();
        if (parentDir != null) {
            Files.createDirectories(parentDir);
        }

        // Convert protobuf to human-readable JSON
        String json = JsonFormat.printer()
                .print(metaDataProto);

        // Write to file, overwriting if exists
        Files.writeString(exportLocation, json,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING);
    }
}
