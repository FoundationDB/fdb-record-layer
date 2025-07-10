/*
 * CloseableUtilsTest.java
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

package com.apple.foundationdb.util;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests for the {@link CloseableUtils} class.
 */
public class CloseableUtilsTest {

    @Test
    void closeAllNoIssue() throws Exception {
        SimpleCloseable c1 = new SimpleCloseable(false, null);
        SimpleCloseable c2 = new SimpleCloseable(false, null);
        SimpleCloseable c3 = new SimpleCloseable(false, null);

        CloseableUtils.closeAll(c1, c2, c3);

        Assertions.assertTrue(c1.isClosed());
        Assertions.assertTrue(c2.isClosed());
        Assertions.assertTrue(c3.isClosed());
    }

    @Test
    void closeAllFailed() throws Exception {
        SimpleCloseable c1 = new SimpleCloseable(true, "c1");
        SimpleCloseable c2 = new SimpleCloseable(true, "c2");
        SimpleCloseable c3 = new SimpleCloseable(true, "c3");

        final CloseException exception = assertThrows(CloseException.class, () -> CloseableUtils.closeAll(c1, c2, c3));

        Assertions.assertEquals("c1", exception.getCause().getMessage());
        final Throwable[] suppressed = exception.getSuppressed();
        Assertions.assertEquals(2, suppressed.length);
        Assertions.assertEquals("c2", suppressed[0].getMessage());
        Assertions.assertEquals("c3", suppressed[1].getMessage());

        Assertions.assertTrue(c1.isClosed());
        Assertions.assertTrue(c2.isClosed());
        Assertions.assertTrue(c3.isClosed());
    }

    @Test
    void closeSomeFailed() throws Exception {
        SimpleCloseable c1 = new SimpleCloseable(true, "c1");
        SimpleCloseable c2 = new SimpleCloseable(false, null);
        SimpleCloseable c3 = new SimpleCloseable(true, "c3");

        final CloseException exception = assertThrows(CloseException.class, () -> CloseableUtils.closeAll(c1, c2, c3));

        Assertions.assertEquals("c1", exception.getCause().getMessage());
        final Throwable[] suppressed = exception.getSuppressed();
        Assertions.assertEquals(1, suppressed.length);
        Assertions.assertEquals("c3", suppressed[0].getMessage());

        Assertions.assertTrue(c1.isClosed());
        Assertions.assertTrue(c2.isClosed());
        Assertions.assertTrue(c3.isClosed());
    }

    private class SimpleCloseable implements AutoCloseable {
        private final boolean fail;
        private final String message;
        private boolean closed = false;

        public SimpleCloseable(boolean fail, String message) {
            this.fail = fail;
            this.message = message;
        }

        @Override
        public void close() {
            closed = true;
            if (fail) {
                throw new RuntimeException(message);
            }
        }

        public boolean isClosed() {
            return closed;
        }
    }
}
