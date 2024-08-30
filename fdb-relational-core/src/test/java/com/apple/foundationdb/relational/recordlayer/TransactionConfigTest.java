/*
 * TransactionConfigTest.java
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

import com.apple.foundationdb.relational.api.EmbeddedRelationalStruct;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.Relational;
import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.api.RelationalStatement;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.utils.SimpleDatabaseRule;
import com.apple.foundationdb.relational.utils.TestSchemas;
import com.apple.foundationdb.relational.utils.RelationalAssertions;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.sql.SQLException;

public class TransactionConfigTest {
    @RegisterExtension
    @Order(0)
    public static final EmbeddedRelationalExtension relational = new EmbeddedRelationalExtension();

    @RegisterExtension
    @Order(1)
    public final SimpleDatabaseRule database = new SimpleDatabaseRule(relational, TransactionConfigTest.class, TestSchemas.restaurant());

    @Disabled // TODO (Bug: sporadic failure in `testRecordInsertionWithTimeOutInConfig`)
    void testRecordInsertionWithTimeOutInConfig() throws RelationalException, SQLException {
        try (RelationalConnection conn = Relational.connect(database.getConnectionUri(), Options.NONE)) {
            conn.setSchema("TEST_SCHEMA");
            conn.setOption(Options.Name.TRANSACTION_TIMEOUT, 1L);
            try (RelationalStatement s = conn.createStatement()) {
                long id = System.currentTimeMillis();
                RelationalAssertions.assertThrowsSqlException(
                        () -> s.executeInsert("RESTAURANTS", EmbeddedRelationalStruct.newBuilder().addLong("REST_NO", id).addString("NAME", "testRest").build()))
                        .hasErrorCode(ErrorCode.TRANSACTION_TIMEOUT);
            }
        }
    }
}
