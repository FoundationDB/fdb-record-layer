/*
 * ScanComparisonsTest.java
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

package com.apple.foundationdb.record.query.plan;

import com.apple.foundationdb.record.Bindings;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.tuple.Tuple;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests for {@link ScanComparisons}.
 */
public class ScanComparisonsTest {

    private EvaluationContext context(String... values) {
        Bindings.Builder bindings = Bindings.newBuilder();
        for (int i = 0; i < values.length; i += 2) {
            bindings.set(values[i], values[i + 1]);
        }
        return EvaluationContext.forBindings(bindings.build());
    }

    private void checkRange(String expected, ScanComparisons.Builder comparisons, EvaluationContext context) {
        assertEquals(expected, comparisons.build().toTupleRange(null, context).toString());
    }

    @Test
    public void justEquals() {
        ScanComparisons.Builder comparisons = new ScanComparisons.Builder();
        comparisons.addEqualityComparison(new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, "abc"));
        comparisons.addEqualityComparison(new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, "xyz"));
        checkRange("[[abc, xyz],[abc, xyz]]", comparisons, context());
    }

    @Test
    public void justLess() {
        ScanComparisons.Builder comparisons = new ScanComparisons.Builder();
        comparisons.addInequalityComparison(new Comparisons.SimpleComparison(Comparisons.Type.LESS_THAN, "xyz"));
        checkRange("([null],[xyz])", comparisons, context());
    }

    @Test
    public void multipleLess() {
        ScanComparisons.Builder comparisons = new ScanComparisons.Builder();
        comparisons.addInequalityComparison(new Comparisons.SimpleComparison(Comparisons.Type.LESS_THAN, "mno"));
        comparisons.addInequalityComparison(new Comparisons.SimpleComparison(Comparisons.Type.LESS_THAN, "xyz"));
        checkRange("([null],[mno])", comparisons, context());
    }

    @Test
    public void justLessEquals() {
        ScanComparisons.Builder comparisons = new ScanComparisons.Builder();
        comparisons.addInequalityComparison(new Comparisons.SimpleComparison(Comparisons.Type.LESS_THAN_OR_EQUALS, "xyz"));
        checkRange("([null],[xyz]]", comparisons, context());
    }

    @Test
    public void lessAndLessEqualsSame() {
        ScanComparisons.Builder comparisons = new ScanComparisons.Builder();
        comparisons.addInequalityComparison(new Comparisons.SimpleComparison(Comparisons.Type.LESS_THAN_OR_EQUALS, "xyz"));
        comparisons.addInequalityComparison(new Comparisons.SimpleComparison(Comparisons.Type.LESS_THAN, "xyz"));
        checkRange("([null],[xyz])", comparisons, context());
    }

    @Test
    public void justGreater() {
        ScanComparisons.Builder comparisons = new ScanComparisons.Builder();
        comparisons.addInequalityComparison(new Comparisons.SimpleComparison(Comparisons.Type.GREATER_THAN, "xyz"));
        checkRange("([xyz],>", comparisons, context());
    }

    @Test
    public void multipleGreater() {
        ScanComparisons.Builder comparisons = new ScanComparisons.Builder();
        comparisons.addInequalityComparison(new Comparisons.SimpleComparison(Comparisons.Type.GREATER_THAN, "mno"));
        comparisons.addInequalityComparison(new Comparisons.SimpleComparison(Comparisons.Type.GREATER_THAN, "xyz"));
        checkRange("([xyz],>", comparisons, context());
    }

    @Test
    public void justGreaterEquals() {
        ScanComparisons.Builder comparisons = new ScanComparisons.Builder();
        comparisons.addInequalityComparison(new Comparisons.SimpleComparison(Comparisons.Type.GREATER_THAN_OR_EQUALS, "xyz"));
        checkRange("[[xyz],>", comparisons, context());
    }

    @Test
    public void greaterAndGreaterEqualsSame() {
        ScanComparisons.Builder comparisons = new ScanComparisons.Builder();
        comparisons.addInequalityComparison(new Comparisons.SimpleComparison(Comparisons.Type.GREATER_THAN_OR_EQUALS, "xyz"));
        comparisons.addInequalityComparison(new Comparisons.SimpleComparison(Comparisons.Type.GREATER_THAN, "xyz"));
        checkRange("([xyz],>", comparisons, context());
    }

    @Test
    public void between() throws Exception {
        ScanComparisons.Builder comparisons = new ScanComparisons.Builder();
        comparisons.addEqualityComparison(new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, "abc"));
        comparisons.addInequalityComparison(new Comparisons.SimpleComparison(Comparisons.Type.GREATER_THAN_OR_EQUALS, "xxx"));
        comparisons.addInequalityComparison(new Comparisons.SimpleComparison(Comparisons.Type.LESS_THAN_OR_EQUALS, "zzz"));
        checkRange("[[abc, xxx],[abc, zzz]]", comparisons, context());
    }

    @Test
    public void parameters() throws Exception {
        ScanComparisons.Builder comparisons = new ScanComparisons.Builder();
        comparisons.addEqualityComparison(new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, "abc"));
        comparisons.addInequalityComparison(new Comparisons.ParameterComparison(Comparisons.Type.GREATER_THAN_OR_EQUALS, "p1"));
        comparisons.addInequalityComparison(new Comparisons.ParameterComparison(Comparisons.Type.LESS_THAN_OR_EQUALS, "p2"));
        checkRange("[[abc, xxx],[abc, zzz]]", comparisons, context("p1", "xxx", "p2", "zzz"));
    }

    @Test
    public void notNull() {
        ScanComparisons.Builder comparisons = new ScanComparisons.Builder();
        comparisons.addInequalityComparison(new Comparisons.NullComparison(Comparisons.Type.NOT_NULL));
        comparisons.addInequalityComparison(new Comparisons.SimpleComparison(Comparisons.Type.LESS_THAN, "zzz"));
        checkRange("([null],[zzz])", comparisons, context());
    }

    @Test
    public void ambiguousUntilBound() {
        ScanComparisons.Builder comparisons = new ScanComparisons.Builder();
        comparisons.addInequalityComparison(new Comparisons.ParameterComparison(Comparisons.Type.GREATER_THAN_OR_EQUALS, "p1"));
        comparisons.addInequalityComparison(new Comparisons.ParameterComparison(Comparisons.Type.GREATER_THAN, "p2"));
        checkRange("([zzz],>", comparisons, context("p1", "xxx", "p2", "zzz"));
        checkRange("[[zzz],>", comparisons, context("p1", "zzz", "p2", "xxx"));
    }

    @Test
    public void multiColumn() {
        ScanComparisons.Builder comparisons = new ScanComparisons.Builder();
        comparisons.addEqualityComparison(new Comparisons.NullComparison(Comparisons.Type.IS_NULL));
        comparisons.addInequalityComparison(new Comparisons.MultiColumnComparison(new Comparisons.SimpleComparison(Comparisons.Type.LESS_THAN, Tuple.from("xxx", "yyy"))));
        checkRange("([null, null],[null, xxx, yyy])", comparisons, context());
    }

}
