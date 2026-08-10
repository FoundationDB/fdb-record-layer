/*
 * LuceneTraversalKindTest.java
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

package com.apple.foundationdb.record.lucene;

import com.apple.foundationdb.record.query.plan.IndexTraversalKind;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryFetchFromPartialRecordPlan.FetchIndexRecords;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that Lucene names the structure its scans traverse, which it can only do because
 * {@link IndexTraversalKind} is open to kinds defined outside the core.
 */
class LuceneTraversalKindTest {
    @Test
    void luceneScanParametersReportTheLuceneTraversalKind() {
        assertThat(luceneScanParameters().getIndexTraversalKind())
                .isEqualTo(LuceneTraversalKinds.LUCENE)
                .isNotEqualTo(IndexTraversalKind.BY_VALUE)
                .isNotEqualTo(IndexTraversalKind.UNKNOWN);
    }

    @Test
    void luceneIndexQueryPlanTakesItsTraversalKindFromItsScanParameters() {
        final LuceneIndexQueryPlan plan = LuceneIndexQueryPlan.of("a_lucene_index",
                luceneScanParameters(),
                FetchIndexRecords.PRIMARY_KEY,
                false,
                null,
                null);
        assertThat(plan.getIndexTraversalKind()).isEqualTo(LuceneTraversalKinds.LUCENE);
    }

    @Nonnull
    private static LuceneScanParameters luceneScanParameters() {
        return new LuceneScanQueryParameters(ScanComparisons.EMPTY,
                new LuceneAutoCompleteQueryClause("good", false, ImmutableSet.of("text")));
    }
}
