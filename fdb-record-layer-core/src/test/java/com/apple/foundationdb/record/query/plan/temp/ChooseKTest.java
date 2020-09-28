/*
 * AliasMapTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2020 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.temp;

import com.apple.foundationdb.record.query.plan.temp.matching.ComputingMatcher;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Test;

import java.util.Set;

/**
 * Testcase class for {@link ComputingMatcher}.
 */
public class ChooseKTest {
    @Test
    void testChooseK1() {
        final Set<String> elements = ImmutableSet.of("a", "b", "c", "d");

        final EnumeratingIterable<String> combinations = ChooseK.chooseK(elements, 3);
        combinations.forEach(System.out::println);

        //        for (EnumeratingIterator<String> iterator = combinations.iterator(); iterator.hasNext(); ) {
        //            final List<String> combination = iterator.next();
        //            System.out.println(combination);
        //            if (combination.get(0).equals("b")) {
        //                iterator.skip(0);
        //            }
        //        }
    }
    
    @Test
    void testChooseK2() {
        final Set<String> elements = ImmutableSet.of("a", "b", "c", "d", "c", "d", "e");

        for (int k = 0; k < elements.size() + 1; k ++) {
            System.out.println("=== " + elements.size() + " choose " + k + " ======================");
            final EnumeratingIterable<String> combinations = ChooseK.chooseK(elements, k);

            combinations.forEach(System.out::println);
            System.out.println("=========================");
        }

    }

}
