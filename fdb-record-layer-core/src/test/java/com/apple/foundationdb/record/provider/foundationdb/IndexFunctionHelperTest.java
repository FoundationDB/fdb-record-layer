/*
 * IndexFunctionHelperTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2020 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb;

import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import org.junit.jupiter.api.Test;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.concatenateFields;
import static com.apple.foundationdb.record.metadata.Key.Expressions.empty;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.metadata.expressions.KeyExpression.FanType.FanOut;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests for {@link IndexFunctionHelper}.
 */
public class IndexFunctionHelperTest {

    @Test
    public void groupSubKeysBasic() {
        final KeyExpression ungrouped = field("value").ungrouped();
        assertEquals(field("value"), IndexFunctionHelper.getGroupedKey(ungrouped));
        assertEquals(empty(), IndexFunctionHelper.getGroupingKey(ungrouped));

        final KeyExpression group = field("value").groupBy(field("group"));
        assertEquals(field("value"), IndexFunctionHelper.getGroupedKey(group));
        assertEquals(field("group"), IndexFunctionHelper.getGroupingKey(group));
    }

    @Test
    public void groupSubKeysNested() {
        final KeyExpression wholeKey = concat(
                field("a", FanOut).nest(concatenateFields("b", "c")),
                field("d"),
                field("e", FanOut).nest(concatenateFields("f", "g")));
        final KeyExpression group0 = new GroupingKeyExpression(wholeKey, 0);
        assertEquals(wholeKey, IndexFunctionHelper.getGroupingKey(group0));
        assertEquals(empty(), IndexFunctionHelper.getGroupedKey(group0));
        final KeyExpression group1 = new GroupingKeyExpression(wholeKey, 1);
        assertEquals(concat(
                field("a", FanOut).nest(concatenateFields("b", "c")),
                field("d"),
                field("e", FanOut).nest(field("f"))),
                IndexFunctionHelper.getGroupingKey(group1));
        assertEquals(field("e", FanOut).nest(field("g")),
                IndexFunctionHelper.getGroupedKey(group1));
        final KeyExpression group2 = new GroupingKeyExpression(wholeKey, 2);
        assertEquals(concat(
                field("a", FanOut).nest(concatenateFields("b", "c")),
                field("d")),
                IndexFunctionHelper.getGroupingKey(group2));
        assertEquals(field("e", FanOut).nest(concatenateFields("f", "g")),
                IndexFunctionHelper.getGroupedKey(group2));
        final KeyExpression group3 = new GroupingKeyExpression(wholeKey, 3);
        assertEquals(field("a", FanOut).nest(concatenateFields("b", "c")),
                IndexFunctionHelper.getGroupingKey(group3));
        assertEquals(concat(
                field("d"),
                field("e", FanOut).nest(concatenateFields("f", "g"))),
                IndexFunctionHelper.getGroupedKey(group3));
        final KeyExpression group4 = new GroupingKeyExpression(wholeKey, 4);
        assertEquals(field("a", FanOut).nest(field("b")),
                IndexFunctionHelper.getGroupingKey(group4));
        assertEquals(concat(
                field("a", FanOut).nest(field("c")),
                field("d"),
                field("e", FanOut).nest(concatenateFields("f", "g"))),
                IndexFunctionHelper.getGroupedKey(group4));
    }

}
