/*
 * TableMetadataVersionTest.java
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

package com.apple.foundationdb.relational.recordlayer;

import com.apple.foundationdb.relational.api.KeySet;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.TableScan;
import com.apple.foundationdb.relational.api.RelationalStatement;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.utils.SimpleDatabaseRule;
import com.apple.foundationdb.relational.utils.TestSchemas;
import com.apple.foundationdb.relational.utils.RelationalAssertions;

import com.google.protobuf.Message;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.net.URI;
import java.util.Collections;

public class TableMetadataVersionTest {

    @RegisterExtension
    @Order(0)
    public final EmbeddedRelationalExtension relationalExtension = new EmbeddedRelationalExtension();

    @RegisterExtension
    @Order(1)
    public final SimpleDatabaseRule database = new SimpleDatabaseRule(relationalExtension,
            URI.create("/metadata_version_test"), TestSchemas.restaurant());

    @RegisterExtension
    @Order(2)
    public final RelationalConnectionRule dbConn = new RelationalConnectionRule(database::getConnectionUri);

    @Test
    void missingMetadataVersionFailsToLoadDuringGet() throws Exception {
        dbConn.setSchema(database.getSchemaName());
        try (RelationalStatement vs = dbConn.createStatement()) {
            Options opts = Options.builder().withOption(Options.Name.REQUIRED_METADATA_TABLE_VERSION, -1).build();

            RelationalAssertions.assertThrows(() -> vs.executeGet("RESTAURANT", new KeySet().setKeyColumn("REST_NO", 1L), opts))
                    .hasErrorCode(ErrorCode.INCORRECT_METADATA_TABLE_VERSION);
        }
    }

    @Test
    void missingMetadataVersionFailsToLoadDuringScan() throws Exception {
        dbConn.setSchema(database.getSchemaName());
        try (RelationalStatement vs = dbConn.createStatement()) {
            Options opts = Options.builder().withOption(Options.Name.REQUIRED_METADATA_TABLE_VERSION, -1).build();

            TableScan ts = new TableScan("RESTAURANT", new KeySet(), new KeySet());
            RelationalAssertions.assertThrows(() -> vs.executeScan(ts, opts))
                    .hasErrorCode(ErrorCode.INCORRECT_METADATA_TABLE_VERSION);
        }
    }

    @Test
    void missingMetadataVersionFailsToLoadDuringInsert() throws Exception {
        dbConn.setSchema(database.getSchemaName());
        try (RelationalStatement vs = dbConn.createStatement()) {
            Options opts = Options.builder().withOption(Options.Name.REQUIRED_METADATA_TABLE_VERSION, -1).build();
            RelationalAssertions.assertThrows(() -> {
                Message message = vs.getDataBuilder("RESTAURANT").setField("REST_NO", 1L).build();
                vs.executeInsert("RESTAURANT", message, opts);
            }).hasErrorCode(ErrorCode.INCORRECT_METADATA_TABLE_VERSION);
        }
    }

    @Test
    void missingMetadataVersionFailsToLoadDuringDelete() throws Exception {
        dbConn.setSchema(database.getSchemaName());
        try (RelationalStatement vs = dbConn.createStatement()) {
            Options opts = Options.builder().withOption(Options.Name.REQUIRED_METADATA_TABLE_VERSION, -1).build();
            RelationalAssertions.assertThrows(() ->
                    vs.executeDelete("RESTAURANT", Collections.singleton(new KeySet().setKeyColumn("REST_NO", 1L)), opts))
                    .hasErrorCode(ErrorCode.INCORRECT_METADATA_TABLE_VERSION);
        }
    }
}
