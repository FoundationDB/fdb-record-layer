/*
 * MessageTupleTest.java
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

import com.apple.foundationdb.record.Restaurant;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.InvalidColumnReferenceException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

class MessageTupleTest {
    Restaurant.RestaurantRecord record = Restaurant.RestaurantRecord.newBuilder().build();
    MessageTuple tuple = new MessageTuple(record);

    @Test
    void getNumFields() {
        assertThat(tuple.getNumFields()).isEqualTo(6);
    }

    @Test
    void getObject() throws InvalidColumnReferenceException {
        assertThat(tuple.getObject(0)).isEqualTo(0L);
        assertThat(tuple.getObject(1)).isEqualTo("");
        assertThat(tuple.getObject(2)).isEqualTo(Restaurant.Location.getDefaultInstance());
        assertThat(tuple.getObject(3)).isEqualTo(Collections.emptyList());
        assertThat(tuple.getObject(4)).isEqualTo(Collections.emptyList());
        assertThat(tuple.getObject(5)).isEqualTo(Collections.emptyList());

        RelationalAssertions.assertThrowsRelationalException(
                () -> tuple.getObject(-1),
                ErrorCode.INVALID_COLUMN_REFERENCE
        );
        RelationalAssertions.assertThrowsRelationalException(
                () -> tuple.getObject(10),
                ErrorCode.INVALID_COLUMN_REFERENCE
        );
    }

    @Test
    void parseMessage() {
        Assertions.assertEquals(record, tuple.parseMessage());
    }
}
