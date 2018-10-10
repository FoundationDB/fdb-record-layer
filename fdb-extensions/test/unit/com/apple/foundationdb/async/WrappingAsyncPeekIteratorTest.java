/*
 * WrappingAsyncPeekIteratorTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.async;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link AsyncPeekIterator}.
 */
public class WrappingAsyncPeekIteratorTest {
    @Test
    public void basicTest() {
        List<Integer> data = Arrays.asList(1, 10, 100, 1000);
        AsyncPeekIterator<Integer> iterator = AsyncPeekIterator.wrap(new NonAsyncIterator<>(data.iterator()));
        assertTrue(iterator.hasNext());
        assertTrue(iterator.hasNext());
        assertEquals(1, iterator.peek().intValue());
        assertEquals(1, iterator.peek().intValue());
        assertEquals(1, iterator.next().intValue());
        assertEquals(10, iterator.peek().intValue());
        assertEquals(10, iterator.next().intValue());
        assertEquals(100, iterator.next().intValue());
        assertEquals(1000, iterator.peek().intValue());
        assertTrue(iterator.hasNext());
        assertEquals(1000, iterator.next().intValue());
        assertFalse(iterator.hasNext());
        assertThrows(NoSuchElementException.class, iterator::peek);
        assertThrows(NoSuchElementException.class, iterator::next);
        assertThrows(NoSuchElementException.class, iterator::peek);
        assertFalse(iterator.hasNext());
    }

    @Test
    public void wrappingEmptyList() {
        AsyncPeekIterator<Integer> iterator = AsyncPeekIterator.wrap(new NonAsyncIterator<>(Collections.emptyIterator()));
        assertFalse(iterator.hasNext());
        assertThrows(NoSuchElementException.class, iterator::peek);
        assertThrows(NoSuchElementException.class, iterator::next);
        assertThrows(NoSuchElementException.class, iterator::peek);
        assertFalse(iterator.hasNext());
    }
}
