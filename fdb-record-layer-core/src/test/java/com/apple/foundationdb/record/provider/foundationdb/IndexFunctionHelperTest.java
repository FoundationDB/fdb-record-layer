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

import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests for {@link IndexFunctionHelper}.
 */
public class IndexFunctionHelperTest {

    @Test
    public void groupSubKeysBasic() {
        final KeyExpression ungrouped = Key.Expressions.field("value").ungrouped();
        assertEquals(Key.Expressions.field("value"), IndexFunctionHelper.getGroupedKey(ungrouped));
        assertEquals(Key.Expressions.empty(), IndexFunctionHelper.getGroupingKey(ungrouped));

        final KeyExpression group = Key.Expressions.field("value").groupBy(Key.Expressions.field("group"));
        assertEquals(Key.Expressions.field("value"), IndexFunctionHelper.getGroupedKey(group));
        assertEquals(Key.Expressions.field("group"), IndexFunctionHelper.getGroupingKey(group));
    }

    @Test
    public void groupSubKeysNested() {
        final KeyExpression nested = new GroupingKeyExpression(Key.Expressions.field("values", KeyExpression.FanType.FanOut).nest(Key.Expressions.concatenateFields("one", "two")), 1);
        assertEquals(Key.Expressions.field("values", KeyExpression.FanType.FanOut).nest(Key.Expressions.field("one")), IndexFunctionHelper.getGroupingKey(nested));
        assertEquals(Key.Expressions.field("values", KeyExpression.FanType.FanOut).nest(Key.Expressions.field("two")), IndexFunctionHelper.getGroupedKey(nested));
    }

}
