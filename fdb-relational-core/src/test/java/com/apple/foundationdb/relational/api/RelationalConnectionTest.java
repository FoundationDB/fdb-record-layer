/*
 * RelationalConnectionTest.java
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

package com.apple.foundationdb.relational.api;

import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalExtension;
import com.apple.foundationdb.relational.utils.RelationalAssertions;

import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.net.URI;

class RelationalConnectionTest {

    @RegisterExtension
    @Order(0)
    public final EmbeddedRelationalExtension relationalExtension = new EmbeddedRelationalExtension();

    @Test
    void wrongScheme() {
        RelationalAssertions.assertThrows(() -> Relational.connect(URI.create("foo"), Options.NONE))
                .hasErrorCode(ErrorCode.INVALID_PATH);

        RelationalAssertions.assertThrows(() -> Relational.connect(URI.create("foo:foo"), Options.NONE))
                .hasErrorCode(ErrorCode.INVALID_PATH);

        RelationalAssertions.assertThrows(() -> Relational.connect(URI.create("jdbc:foo"), Options.NONE))
                .hasErrorCode(ErrorCode.UNABLE_TO_ESTABLISH_SQL_CONNECTION);

        RelationalAssertions.assertThrows(() -> Relational.connect(URI.create("jdbc:embed"), Options.NONE))
                .hasErrorCode(ErrorCode.UNABLE_TO_ESTABLISH_SQL_CONNECTION);

        RelationalAssertions.assertThrows(() -> Relational.connect(URI.create("jdbc:embed:/i_am_not_a_database"), Options.NONE))
                .hasErrorCode(ErrorCode.DATABASE_NOT_FOUND);
    }

    @Test
    void missingLeadingSlash() {
        RelationalAssertions.assertThrows(() -> Relational.connect(URI.create("jdbc:embed:i_am_not_a_database"), Options.NONE))
                .hasErrorCode(ErrorCode.DATABASE_NOT_FOUND)
                .containsInMessage("<i_am_not_a_database>")
                .doesNotContainInMessage("<null>")
        ;

    }
}
