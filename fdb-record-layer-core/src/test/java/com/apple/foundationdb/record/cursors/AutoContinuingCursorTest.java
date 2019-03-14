/*
 * AutoContinuingCursorTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2019 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.cursors;

import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseFactory;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseRunner;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBTestBase;
import com.apple.test.Tags;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.function.BiFunction;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test for {@link AutoContinuingCursor}.
 */
@Tag(Tags.RequiresFDB)
public class AutoContinuingCursorTest extends FDBTestBase {
    private FDBDatabase database;

    @BeforeEach
    public void getDatabase() {
        database = FDBDatabaseFactory.instance().getDatabase();
    }

    private static final List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

    private void testAutoContinuingCursorGivenCursorGenerator(
            BiFunction<FDBRecordContext, byte[], RecordCursor<Integer>> nextCursorGenerator,
            List<Integer> expectedList) {
        try (FDBDatabaseRunner runner = database.newRunner()) {
            RecordCursor<Integer> cursor = new AutoContinuingCursor<>(runner, nextCursorGenerator);

            List<Integer> returnedList = cursor.asList().join();
            assertEquals(expectedList, returnedList);
        }
    }

    @Test
    public void testAutoContinuingCursorSimple() {
        testAutoContinuingCursorGivenCursorGenerator((context, continuation) ->
                        new ListCursor<>(list, continuation).limitRowsTo(3),
                list);
    }

    @Test
    public void testAutoContinuingCursorWhenSomeGeneratedCursorsNeverHaveNext() {
        // This underlying cursor may not produce any item in one transaction. AutoContinuingCursor is expected to make
        // progress until it is truly exhausted.
        testAutoContinuingCursorGivenCursorGenerator((context, continuation) ->
                        new ListCursor<>(list, continuation).limitRowsTo(2).filter(item -> item % 3 == 0),
                Arrays.asList(3, 6, 9)
        );
    }
}
