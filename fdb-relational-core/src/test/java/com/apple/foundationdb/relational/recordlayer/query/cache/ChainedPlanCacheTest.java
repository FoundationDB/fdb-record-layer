/*
 * ChainedPlanCacheTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.recordlayer.query.cache;

import com.apple.foundationdb.record.IndexState;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.RecordStoreState;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.relational.recordlayer.query.LogicalQuery;

import com.google.common.collect.Sets;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.AutoCloseableSoftAssertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ChainedPlanCacheTest {

    @Test
    void canCacheEntries() {
        LogicalQuery lq = new LogicalQuery("select * from restaurant", 0);
        RecordMetaData md = mock(RecordMetaData.class);
        RecordStoreState storeState = mock(RecordStoreState.class);
        SchemaState ss = new SchemaState() {
            @Override
            public RecordMetaData getSchemaMetaData() {
                return md;
            }

            @Override
            public RecordStoreState getState() {
                return storeState;
            }
        };

        PlanCache pc = new ChainedPlanCache(128);

        //shouldn't have a plan for this query yet
        Assertions.assertThat(pc.getPlan(lq, ss)).isNull();

        RecordQueryPlan plan = mock(RecordQueryPlan.class);
        pc.cacheEntry(lq, plan);

        //now we should get it back
        RecordQueryPlan cachedPlan = pc.getPlan(lq, ss);
        //use == to check it's the right thing
        Assertions.assertThat(cachedPlan).isSameAs(plan);
    }

    @Test
    void cacheValidatesIndexNames() {
        LogicalQuery lq = new LogicalQuery("select * from restaurant", 0);

        PlanCache pc = new ChainedPlanCache(128);
        RecordQueryPlan plan = mock(RecordQueryPlan.class);
        when(plan.getUsedIndexes()).thenReturn(Set.of("idx1"));
        pc.cacheEntry(lq, plan);

        //now check the cache with a different index set, it shouldn't return anything
        SchemaState secondSs = mockSchemaState("idx2");
        Assertions.assertThat(pc.getPlan(lq, secondSs)).isNull();
    }

    @Test
    void canCacheMultipleEntriesWithDifferentIndexes() {
        LogicalQuery lq = new LogicalQuery("select * from restaurant", 0);

        RecordQueryPlan plan = mock(RecordQueryPlan.class);
        when(plan.getUsedIndexes()).thenReturn(Set.of("idx1"));

        PlanCache pc = new ChainedPlanCache(128);
        pc.cacheEntry(lq, plan);

        //now check the cache with a different index set, it shouldn't return anything
        RecordQueryPlan secondPlan = mock(RecordQueryPlan.class);
        when(secondPlan.getUsedIndexes()).thenReturn(Set.of("idx2"));
        pc.cacheEntry(lq, secondPlan);

        SchemaState firstSS = mockSchemaState("idx1");
        SchemaState secondSS = mockSchemaState("idx2");

        List<String> allIndexNames = List.of("idx1", "idx2");
        SchemaState thirdSS = mockSchemaState(allIndexNames, Set.of("idx1"));
        SchemaState fourthSS = mockSchemaState(allIndexNames, Set.of("idx2"));
        SchemaState fifthSS = mockSchemaState(allIndexNames, Collections.emptySet());

        //make sure both versions can be returned depending on available indexes
        try (AutoCloseableSoftAssertions softly = new AutoCloseableSoftAssertions()) {
            softly.assertThat(pc.getPlan(lq, firstSS)).isSameAs(plan);
            softly.assertThat(pc.getPlan(lq, secondSS)).isSameAs(secondPlan);
            softly.assertThat(pc.getPlan(lq, thirdSS)).isSameAs(plan);
            softly.assertThat(pc.getPlan(lq, fourthSS)).isSameAs(secondPlan);
            softly.assertThat(pc.getPlan(lq, fifthSS)).isNull();
        }
    }

    @Test
    void findCacheEntryWithExtraneousIndexes() {
        final PlanCache pc = new ChainedPlanCache(128);

        LogicalQuery lq1 = new LogicalQuery("select * from restaurant", 0);
        RecordQueryPlan plan1 = mock(RecordQueryPlan.class);
        when(plan1.getUsedIndexes()).thenReturn(Set.of("idx2"));
        pc.cacheEntry(lq1, plan1);

        LogicalQuery lq2 = new LogicalQuery("select * from reviewer", 0);
        RecordQueryPlan plan2 = mock(RecordQueryPlan.class);
        when(plan2.getUsedIndexes()).thenReturn(Set.of("idx3", "idx5"));
        pc.cacheEntry(lq2, plan2);

        try (AutoCloseableSoftAssertions softly = new AutoCloseableSoftAssertions()) {
            final List<String> allIndexes = List.of("idx1", "idx2", "idx3", "idx4", "idx5", "idx6");
            for (Set<String> readableIndexes : Sets.powerSet(Set.of(allIndexes.toArray(new String[0])))) {
                SchemaState schemaState = mockSchemaState(allIndexes, readableIndexes);
                boolean shouldUseCachedPlan1 = readableIndexes.contains("idx2");
                softly.assertThat(pc.getPlan(lq1, schemaState))
                        .as("plan requiring idx2 should%s use cached plan with readable indexes: %s", shouldUseCachedPlan1 ? "" : " not", readableIndexes)
                        .isSameAs(shouldUseCachedPlan1 ? plan1 : null);
                boolean shouldUseCachedPlan2 = readableIndexes.contains("idx3") && readableIndexes.contains("idx5");
                softly.assertThat(pc.getPlan(lq2, schemaState))
                        .as("plan requiring idx3 and idx5 should%s use cached plan with readable indexes: %s", shouldUseCachedPlan2 ? "" : " not", readableIndexes)
                        .isSameAs(shouldUseCachedPlan2 ? plan2 : null);
            }
        }
    }

    private SchemaState mockSchemaState(String... indexNames) {
        List<String> allIndexNames = List.of(indexNames);
        Set<String> readableIndexes = new HashSet<>(allIndexNames);
        return mockSchemaState(allIndexNames, readableIndexes);
    }

    private SchemaState mockSchemaState(List<String> allIndexNames, Collection<String> readableIndexes) {
        List<Index> indexes = new ArrayList<>();
        for (String indexName : allIndexNames) {
            Index idx = mock(Index.class);
            when(idx.getName()).thenReturn(indexName);
            indexes.add(idx);
        }

        RecordMetaData md = mock(RecordMetaData.class);
        when(md.getAllIndexes()).thenReturn(indexes);
        for (Index idx : indexes) {
            when(md.hasIndex(idx.getName())).thenReturn(true);
        }
        Map<String, IndexState> indexStateMap = new HashMap<>();
        for (String indexName : allIndexNames) {
            if (!readableIndexes.contains(indexName)) {
                indexStateMap.put(indexName, IndexState.DISABLED);
            }
        }
        RecordStoreState storeState = new RecordStoreState(
                RecordMetaDataProto.DataStoreInfo.getDefaultInstance(),
                indexStateMap
        );

        return new SchemaState() {
            @Override
            public RecordMetaData getSchemaMetaData() {
                return md;
            }

            @Override
            public RecordStoreState getState() {
                return storeState;
            }
        };
    }
}
