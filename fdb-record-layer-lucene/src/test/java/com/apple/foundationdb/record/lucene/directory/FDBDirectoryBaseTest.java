/*
 * FDBDirectoryBaseTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2021 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.lucene.directory;

import com.apple.foundationdb.record.lucene.LuceneRecordContextProperties;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseFactory;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContextConfig;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBTransactionPriority;
import com.apple.foundationdb.record.provider.foundationdb.TestKeySpace;
import com.apple.foundationdb.record.provider.foundationdb.properties.RecordLayerPropertyStorage;
import com.apple.foundationdb.subspace.Subspace;
import org.junit.jupiter.api.BeforeEach;

import java.util.Random;

/**
 * Abstract class for testing FDBDirectory for Lucene.
 *
 */
public abstract class FDBDirectoryBaseTest {
    protected FDBDatabase fdb;
    protected Subspace subspace;
    protected FDBDirectory directory;
    protected Random random = new Random();

    protected FDBStoreTimer timer = new FDBStoreTimer();

    @BeforeEach
    public void setUp() {
        if (fdb == null) {
            fdb = FDBDatabaseFactory.instance().getDatabase();
        }
        if (subspace == null) {
            subspace = fdb.run(context -> TestKeySpace.getKeyspacePath("record-test", "unit", "indexTest", "version").toSubspace(context));
        }
        fdb.run(context -> {
            context.ensureActive().clear(subspace.range());
            return null;
        });
        FDBRecordContext context = fdb.openContext(getContextConfig());
        directory = new FDBDirectory(subspace, context);
    }

    private FDBRecordContextConfig getContextConfig() {
        return FDBRecordContextConfig.newBuilder()
                .setTimer(timer)
                .setPriority(FDBTransactionPriority.DEFAULT)
                .setRecordContextProperties(RecordLayerPropertyStorage.newBuilder().addProp(LuceneRecordContextProperties.LUCENE_INDEX_COMPRESSION_ENABLED, true).build())
                .build();
    }

    protected int randomInt(int minimum) {
        return Math.abs(random.nextInt(10 * 1024)) + minimum;
    }

}
