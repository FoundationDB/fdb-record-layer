/*
 * IteratorCursorTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2021 Apple Inc. and the FoundationDB project authors
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

import com.google.common.collect.Iterators;
import org.junit.jupiter.api.Test;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 *
 * Test to validate IteratorCursor.
 *
 */
public class IteratorCursorTest {

    @Test
    public void testCloseableCloses() throws Exception {
        TestCloseable closeable = new TestCloseable();
        IteratorCursor<String> cursor = new IteratorCursor<>(Executors.newSingleThreadExecutor(), Iterators.singletonIterator("John"), closeable);
        assertEquals(1, cursor.getCount().get());
        cursor.close();
        assertTrue(closeable.closed);
    }

    @Test
    public void testCloseableSwallowsException() throws Exception {
        IteratorCursor<String> cursor = new IteratorCursor<>(Executors.newSingleThreadExecutor(), Iterators.singletonIterator("John"), () -> { throw new RuntimeException("fired"); } );
        assertEquals(1, cursor.getCount().get());
        cursor.close();
    }

    private static class TestCloseable implements Closeable {
        private boolean closed = false;

        @Override
        public void close() throws IOException {
            closed = true;
        }
    }


}
