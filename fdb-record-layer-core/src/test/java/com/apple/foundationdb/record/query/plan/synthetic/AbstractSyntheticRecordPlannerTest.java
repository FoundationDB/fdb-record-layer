/*
 * AbstractSyntheticRecordPlannerTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.synthetic;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.metadata.JoinedRecordType;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.TestRecordsJoinIndexProto;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.query.plan.PlannableIndexTypes;
import com.apple.foundationdb.record.query.plan.QueryPlanner;
import com.apple.foundationdb.record.query.plan.RecordQueryPlanner;
import com.apple.foundationdb.record.test.FDBDatabaseExtension;
import com.apple.foundationdb.record.test.TestKeySpacePathManagerExtension;
import com.apple.foundationdb.record.test.TestKeySpace;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.test.Tags;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.RegisterExtension;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Base class for {@link SyntheticRecordPlanner} tests.
 */
@Tag(Tags.RequiresFDB)
@API(API.Status.EXPERIMENTAL)
public abstract class AbstractSyntheticRecordPlannerTest {
    @RegisterExtension
    final FDBDatabaseExtension dbExtension = new FDBDatabaseExtension();
    @RegisterExtension
    final TestKeySpacePathManagerExtension pathManager = new TestKeySpacePathManagerExtension(dbExtension);

    protected FDBDatabase fdb;
    protected FDBStoreTimer timer;
    protected RecordMetaDataBuilder metaDataBuilder;
    protected FDBRecordStore.Builder recordStoreBuilder;

    protected FDBRecordContext openContext() {
        return fdb.openContext(null, timer);
    }

    protected QueryPlanner setupPlanner(@Nonnull FDBRecordStore recordStore, @Nullable PlannableIndexTypes indexTypes) {
        if (indexTypes == null) {
            indexTypes = PlannableIndexTypes.DEFAULT;
        }
        return new RecordQueryPlanner(recordStore.getRecordMetaData(), recordStore.getRecordStoreState(), indexTypes, recordStore.getTimer());
    }

    protected static void assertConstituentPlansMatch(SyntheticRecordPlanner planner, JoinedRecordType joinedRecordType,
                                                    Map<String, Matcher<? super SyntheticRecordFromStoredRecordPlan>> constituentMatchers) {
        for (JoinedRecordType.JoinConstituent constituent : joinedRecordType.getConstituents()) {
            assertThat("constituent matchers missing matcher for constituent " + constituent.getName(),
                    constituentMatchers, Matchers.hasKey(constituent.getName()));
            Matcher<? super SyntheticRecordFromStoredRecordPlan> matcher = constituentMatchers.get(constituent.getName());
            final SyntheticRecordFromStoredRecordPlan plan = planner.forJoinConstituent(joinedRecordType, constituent);
            assertThat(plan, matcher);
        }
    }

    @BeforeEach
    public void initBuilders() {
        metaDataBuilder = RecordMetaData.newBuilder()
                .setRecords(TestRecordsJoinIndexProto.getDescriptor());

        fdb = dbExtension.getDatabase();
        KeySpacePath path = pathManager.createPath(TestKeySpace.RECORD_STORE);
        recordStoreBuilder = FDBRecordStore.newBuilder()
                .setMetaDataProvider(metaDataBuilder)
                .setKeySpacePath(path);
        timer = new FDBStoreTimer();
    }
}
