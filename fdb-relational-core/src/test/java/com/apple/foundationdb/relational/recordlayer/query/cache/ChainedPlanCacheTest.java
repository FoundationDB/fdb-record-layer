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
import com.apple.foundationdb.record.RecordStoreState;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.relational.recordlayer.query.LogicalQuery;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
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

    @Disabled
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
        SchemaState secondSs = mockSchemaState("idx2");
        //make sure both versions can be returned
        Assertions.assertThat(pc.getPlan(lq, firstSS)).isSameAs(plan);
        Assertions.assertThat(pc.getPlan(lq, secondSs)).isSameAs(secondPlan);
    }

    private SchemaState mockSchemaState(String... indexNames) {
        List<Index> indexes = new ArrayList<>();
        Map<String, IndexState> allIdxs = new HashMap<>();
        for (String indexName : indexNames) {
            Index idx = mock(Index.class);
            when(idx.getName()).thenReturn(indexName);
            indexes.add(idx);
            IndexState idxState = IndexState.READABLE;
            allIdxs.put(idx.getName(), idxState);
        }

        RecordMetaData md = mock(RecordMetaData.class);
        when(md.getAllIndexes()).thenReturn(indexes);
        for (Index idx : indexes) {
            when(md.hasIndex(idx.getName())).thenReturn(true);
        }
        RecordStoreState firstState = mock(RecordStoreState.class);
        when(firstState.getIndexStates()).thenReturn(allIdxs);
        when(firstState.getState(any(String.class))).thenAnswer(invocation -> allIdxs.get((String) invocation.getArgument(0)));

        return new SchemaState() {
            @Override
            public RecordMetaData getSchemaMetaData() {
                return md;
            }

            @Override
            public RecordStoreState getState() {
                return firstState;
            }
        };
    }
}
