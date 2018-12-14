/*
 * BooleanNormalizerTest.java
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

package com.apple.foundationdb.record.query.plan.planning;

import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests for {@link BooleanNormalizer}.
 */
public class BooleanNormalizerTest {

    static final QueryComponent P1 = Query.field("f").equalsValue(1);
    static final QueryComponent P2 = Query.field("f").equalsValue(2);
    static final QueryComponent P3 = Query.field("f").equalsValue(3);
    static final QueryComponent P4 = Query.field("f").equalsValue(4);
    static final QueryComponent P5 = Query.field("f").equalsValue(5);

    static final QueryComponent PNested = Query.field("p").matches(Query.field("c").equalsValue("x"));
    static final QueryComponent POneOf = Query.field("r").oneOfThem().equalsValue("x");
    static final QueryComponent PRank = Query.rank(Key.Expressions.field("score").groupBy(Key.Expressions.field("game"))).lessThan(100);

    @Test
    public void atomic() {
        assertFilterEquals(P1, BooleanNormalizer.normalize(P1));
        assertFilterEquals(Query.not(P1), BooleanNormalizer.normalize(Query.not(P1)));

        assertFilterEquals(PNested, BooleanNormalizer.normalize(PNested));
        assertFilterEquals(POneOf, BooleanNormalizer.normalize(POneOf));
        assertFilterEquals(PRank, BooleanNormalizer.normalize(PRank));
    }
    
    @Test
    public void flatten() {
        assertFilterEquals(Query.and(P1, P2, P3),
                BooleanNormalizer.normalize(Query.and(Query.and(P1, P2), P3)));
        assertFilterEquals(Query.and(P1, P2, P3),
                BooleanNormalizer.normalize(Query.and(P1, Query.and(P2, P3))));
        assertFilterEquals(Query.and(P1, P2, P3, P4, P5),
                BooleanNormalizer.normalize(Query.and(P1, Query.and(P2, Query.and(P3, Query.and(P4, P5))))));
        assertFilterEquals(Query.and(P1, P2, P3, P4, P5),
                BooleanNormalizer.normalize(Query.and(Query.and(Query.and(Query.and(P1, P2), P3), P4), P5)));
        assertFilterEquals(Query.or(P1, P2, P3),
                BooleanNormalizer.normalize(Query.or(Query.or(P1, P2), P3)));
        assertFilterEquals(Query.or(P1, P2, P3),
                BooleanNormalizer.normalize(Query.or(P1, Query.or(P2, P3))));
    }

    @Test
    public void distribute() {
        assertFilterEquals(Query.or(Query.and(P1, P2), Query.and(P3, P4)),
                BooleanNormalizer.normalize(Query.or(Query.and(P1, P2), Query.and(P3, P4))));
        assertFilterEquals(Query.or(Query.and(P1, P2), Query.and(P1, P3)),
                BooleanNormalizer.normalize(Query.and(P1, Query.or(P2, P3))));
        assertFilterEquals(Query.or(Query.and(P1, P3), Query.and(P2, P3), Query.and(P1, P4), Query.and(P2, P4)),
                BooleanNormalizer.normalize(Query.and(Query.or(P1, P2), Query.or(P3, P4))));
    }

    @Test
    public void deMorgan() {
        assertFilterEquals(Query.or(Query.not(P1), Query.not(P2)),
                BooleanNormalizer.normalize(Query.not(Query.and(P1, P2))));
        assertFilterEquals(Query.and(Query.not(P1), Query.not(P2)),
                BooleanNormalizer.normalize(Query.not(Query.or(P1, P2))));
    }

    @Test
    public void complex() {
        assertFilterEquals(Query.or(Query.and(P1, Query.not(P2)), Query.and(P1, Query.not(P3), Query.not(P4))),
                BooleanNormalizer.normalize(Query.and(P1, Query.not(Query.and(P2, Query.or(P3, P4))))));
    }

    // Query components do not implement equals, but they have distinctive enough printed representations.
    protected static void assertFilterEquals(@Nonnull final QueryComponent expected, @Nonnull final QueryComponent actual) {
        assertEquals(expected.toString(), actual.toString());
    }
}
