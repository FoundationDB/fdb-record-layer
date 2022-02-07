/*
 * ResumableIteratorTests.java
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

import com.apple.foundationdb.relational.api.Continuation;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class ResumableIteratorTests {

    @Test
    public void testContinuation() {
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        ResumableIteratorImpl<Integer> testIt = new ResumableIteratorImpl<>(list.listIterator(), null);
        advanceIterator(testIt, 3);
        Assertions.assertTrue(testIt.hasNext());
        Assertions.assertEquals(4, testIt.next());

        Continuation continuation = testIt.getContinuation();
        iteratorWithContinuationShouldPointTo(list, continuation, 5);
    }

    @Test
    public void testContinuationOnDifferentCollections() {
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        List<String> list2 = Arrays.asList("a", "b", "c", "d", "e", "f");

        ResumableIteratorImpl<Integer> testIt = new ResumableIteratorImpl<>(list.listIterator(), null);
        advanceIterator(testIt, 3);
        Assertions.assertTrue(testIt.hasNext());
        Assertions.assertEquals(4, testIt.next());

        Continuation continuation = testIt.getContinuation();
        iteratorWithContinuationShouldPointTo(list2, continuation, "e");
    }

    @Test
    public void testContinuationOnDifferentCollectionOutOfBound() {
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        List<String> list2 = Arrays.asList("a", "b");

        ResumableIteratorImpl<Integer> testIt = new ResumableIteratorImpl<>(list.listIterator(), null);
        advanceIterator(testIt, 3);
        Assertions.assertTrue(testIt.hasNext());
        Assertions.assertEquals(4, testIt.next());

        Continuation continuation = testIt.getContinuation();

        try {
            new ResumableIteratorImpl<>(list2.listIterator(), continuation);
        } catch (Exception e) {
            Assertions.assertTrue(e instanceof RelationalException);
            RelationalException relationalException = (RelationalException) e;
            Assertions.assertTrue(relationalException.getMessage().contains("continuation out of iterator bounds"));
        }
    }

    @Test
    public void testMultipleContinuationsOnSameIterator() {

        /*
         *    1 2 3 4 5 6 7 8 9 10
         *          ^   ^
         *          |   |
         *          |__ |_______ c1 (pointing at 5)
         *              |
         *              |________ c2 (pointing at 7)
         */

        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        ResumableIteratorImpl<Integer> testIt = new ResumableIteratorImpl<>(list.listIterator(), null);
        advanceIterator(testIt, 4);
        Continuation continuation1 = testIt.getContinuation();
        advanceIterator(testIt, 2);
        Continuation continuation2 = testIt.getContinuation();

        iteratorWithContinuationShouldPointTo(list, continuation1, 5);
        iteratorWithContinuationShouldPointTo(list, continuation2, 7);
    }

    @Test
    public void testContinuationOnCollectionEdges() {
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        ResumableIteratorImpl<Integer> testIt = new ResumableIteratorImpl<>(list.listIterator(), null);
        Continuation beginContinuation = testIt.getContinuation();
        advanceIterator(testIt, 10);
        Continuation endContinuation = testIt.getContinuation();

        Assertions.assertTrue(beginContinuation.atBeginning());
        iteratorWithContinuationShouldPointTo(list, beginContinuation, 1);
        // if we attempt to use the endContinuation, the iterator will seek to the end.
        Assertions.assertNull(endContinuation.getBytes());
        Assertions.assertTrue(endContinuation.atEnd());
        ResumableIteratorImpl<Integer> endIt = new ResumableIteratorImpl<>(list.listIterator(), endContinuation);
        Assertions.assertFalse(endIt.hasNext());
    }

    @Test
    public void testContinuationOnEmptyCollection() {
        List<Integer> list = List.of();
        ResumableIteratorImpl<Integer> testIt = new ResumableIteratorImpl<>(list.listIterator(), null);
        Continuation continuation = testIt.getContinuation();
        Assertions.assertTrue(continuation.atBeginning());
        Assertions.assertTrue(continuation.atEnd());
    }

    // helper methods.

    private static <T> void advanceIterator(Iterator<T> it, int count) {
        int i = 0;
        while (i < count) {
            assert it.hasNext();
            it.next();
            i++;
        }
    }

    private static <T> void iteratorWithContinuationShouldPointTo(List<T> list, Continuation continuation, T expected) {
        ResumableIteratorImpl<T> it = new ResumableIteratorImpl<>(list.listIterator(), continuation);
        Assertions.assertTrue(it.hasNext());
        Assertions.assertEquals(expected, it.next());
    }
}
