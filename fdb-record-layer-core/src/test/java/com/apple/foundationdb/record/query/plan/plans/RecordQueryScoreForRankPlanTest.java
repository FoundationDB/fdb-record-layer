/*
 * RecordQueryScoreForRankPlanTest.java
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

package com.apple.foundationdb.record.query.plan.plans;

import com.apple.foundationdb.record.FunctionNames;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.metadata.IndexAggregateFunction;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.planning.BindingFunction;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link RecordQueryScoreForRankPlan}.
 */
class RecordQueryScoreForRankPlanTest {
    /**
     * Tests that a {@link RecordQueryScoreForRankPlan} survives a round trip through its proto representation.
     */
    @Test
    void testSerialization() {
        final RecordQueryScoreForRankPlan plan = new RecordQueryScoreForRankPlan(
                new RecordQueryScanPlan(ScanComparisons.EMPTY, false),
                List.of(new RecordQueryScoreForRankPlan.ScoreForRank("rank1", BindingFunction.LONG,
                                new IndexAggregateFunction(FunctionNames.SCORE_FOR_RANK, Key.Expressions.field("score"), "rank_index"),
                                List.of(new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, 3L))),
                        new RecordQueryScoreForRankPlan.ScoreForRank("rank2", BindingFunction.INT,
                                new IndexAggregateFunction(FunctionNames.SCORE_FOR_RANK_ELSE_SKIP, Key.Expressions.field("score"), "rank_index"),
                                List.of(new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, "g"),
                                        new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, 7L)))));

        final var proto = plan.toProto(PlanSerializationContext.newForCurrentMode());
        final RecordQueryScoreForRankPlan deserialized = RecordQueryScoreForRankPlan.fromProto(
                PlanSerializationContext.newForCurrentMode(), proto);

        assertThat(deserialized.getRanks()).isEqualTo(plan.getRanks());
        assertThat(deserialized.toProto(PlanSerializationContext.newForCurrentMode())).isEqualTo(proto);
    }
}
