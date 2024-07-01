/*
 * LucenePlanSerializationTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2023 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.lucene;

import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.planprotos.PPlanReference;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryFetchFromPartialRecordPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.Test;

import java.util.List;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.metadata.Key.Expressions.function;

/**
 * Tests around plan serialization.
 */
public class LucenePlanSerializationTest {

    private static final List<KeyExpression> COMPLEX_MULTIPLE_TEXT_INDEXES_WITH_AUTO_COMPLETE_STORED_FIELDS =
            List.of(function(LuceneFunctionNames.LUCENE_TEXT, field("text")), function(LuceneFunctionNames.LUCENE_TEXT, field("text2")));

    private static final Index COMPLEX_MULTIPLE_TEXT_INDEXES_WITH_AUTO_COMPLETE = new Index("Complex$text_multipleIndexes",
            concat(COMPLEX_MULTIPLE_TEXT_INDEXES_WITH_AUTO_COMPLETE_STORED_FIELDS),
            LuceneIndexTypes.LUCENE,
            ImmutableMap.of());

    @Test
    public void simpleLuceneIndexPlan() throws Exception {
        final RecordQueryPlan plan =
                LuceneIndexQueryPlan.of(COMPLEX_MULTIPLE_TEXT_INDEXES_WITH_AUTO_COMPLETE.getName(),
                        new LuceneScanQueryParameters(ScanComparisons.EMPTY,
                                new LuceneAutoCompleteQueryClause("good", false, ImmutableSet.of("text", "text2"))),
                        RecordQueryFetchFromPartialRecordPlan.FetchIndexRecords.PRIMARY_KEY,
                        false,
                        null,
                        COMPLEX_MULTIPLE_TEXT_INDEXES_WITH_AUTO_COMPLETE_STORED_FIELDS);
        PlanSerializationContext planSerializationContext = PlanSerializationContext.newForCurrentMode();
        final PPlanReference proto = planSerializationContext.toPlanReferenceProto(plan);
        final byte[] bytes = proto.toByteArray();
        final PPlanReference parsedProto =
                PPlanReference.parseFrom(bytes);
        planSerializationContext = PlanSerializationContext.newForCurrentMode();
        final RecordQueryPlan parsedPlan = planSerializationContext.fromPlanReferenceProto(parsedProto);
        Verify.verify(parsedPlan instanceof LuceneIndexQueryPlan);
        // TODO proper verification as we currently do not properly round-trip;
    }
}
