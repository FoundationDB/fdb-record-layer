/*
 * ExpansionVisitorTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.cascades;

import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static com.apple.foundationdb.record.metadata.Key.Expressions.field;

/**
 * Tests for {@link ExpansionVisitor} default methods and
 * {@link WindowedIndexExpansionVisitor} unsupported expand overload.
 */
class ExpansionVisitorTest {

    @Test
    void testDefaultExpandWithBaseQuantifierSupplierThrowsUnsupportedOperation() {
        final Index index = new Index("test_value_index", field("value"), IndexTypes.VALUE);
        final ValueIndexExpansionVisitor visitor = new ValueIndexExpansionVisitor(index, Collections.emptyList());

        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> visitor.expand(() -> null, null, false),
                "Default expand(Supplier, ...) should throw UnsupportedOperationException");
    }

    @Test
    void testWindowedIndexExpandWithRecordTypeNamesThrowsUnsupportedOperation() {
        final Index index = new Index("test_rank_index", field("score").ungrouped(), IndexTypes.RANK);
        final WindowedIndexExpansionVisitor visitor = new WindowedIndexExpansionVisitor(index, Collections.emptyList());

        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> visitor.expand(ImmutableSet.of("TestRecord"),
                        ImmutableSet.of("TestRecord"),
                        Type.Record.fromFields(ImmutableList.of()),
                        new IndexAccessHint("test_rank_index"),
                        null,
                        false),
                "WindowedIndexExpansionVisitor.expand(Set, Set, ...) should throw UnsupportedOperationException");
    }
}
