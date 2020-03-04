/*
 * SourceMatchingTest.java
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

package com.apple.foundationdb.record.query.plan.temp.view;

import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.predicates.ElementPredicate;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests of the source matching logic in {@link Source#supportsSourceIn(ViewExpressionComparisons, Source)}, which
 * is used to determine when two sources (e.g., a query predicate source and an index source) can be coalesced into
 * a single one.
 */
public class SourceMatchingTest {
    @Test
    public void twoDoublyNestedSources() {
        ViewExpression.Builder viewBuilder = ViewExpression.builder()
                .addRecordType("A");
        Source baseSource = viewBuilder.buildBaseSource();
        Source middleSource = new RepeatedFieldSource(baseSource, "b");
        Source innerSource1 = new RepeatedFieldSource(middleSource, "c");
        Source innerSource2 = new RepeatedFieldSource(middleSource, "d");

        viewBuilder.addTupleElement(new ValueElement(innerSource1));
        viewBuilder.addTupleElement(new ValueElement(innerSource2));

        final ViewExpression view = viewBuilder.build();
        ViewExpressionComparisons comparisons = new ViewExpressionComparisons(view);

        Source otherBaseSource = new RecordTypeSource("A");
        Source otherMiddleSource = new RepeatedFieldSource(otherBaseSource, "b");
        Source otherInnerSource1 = new RepeatedFieldSource(otherMiddleSource, "c");
        Source otherInnerSource2 = new RepeatedFieldSource(otherMiddleSource, "d");

        Comparisons.Comparison comparison = new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, 1);
        Optional<ViewExpressionComparisons> comparisons2 =
                comparisons.matchWith(new ElementPredicate(new ValueElement(otherInnerSource1), comparison));
        assertTrue(comparisons2.isPresent());
        Source otherInnerSource1Fail = new RepeatedFieldSource(otherMiddleSource, "c");
        Optional<ViewExpressionComparisons> comparisons2Fail = comparisons2.get()
                .matchWith(new ElementPredicate(new ValueElement(otherInnerSource1Fail), comparison));
        assertFalse(comparisons2Fail.isPresent());

        assertTrue(innerSource2.supportsSourceIn(comparisons2.get(), otherInnerSource2));
        Optional<ViewExpressionComparisons> comparisons3 =
                comparisons2.get().matchWith(new ElementPredicate(new ValueElement(otherInnerSource2), comparison));
        assertTrue(comparisons3.isPresent());
    }
}
