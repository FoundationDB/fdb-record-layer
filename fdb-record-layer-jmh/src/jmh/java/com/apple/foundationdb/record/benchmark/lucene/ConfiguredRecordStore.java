/*
 * ConfiguredRecordStore.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2022 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.benchmark.lucene;

import com.apple.foundationdb.record.benchmark.BenchmarkRecordStore;
import com.apple.foundationdb.record.benchmark.BenchmarkTimer;
import com.apple.foundationdb.record.lucene.LuceneRecordContextProperties;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContextConfig;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.properties.RecordLayerPropertyStorage;

import javax.annotation.Nonnull;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.function.Consumer;

public class ConfiguredRecordStore extends BenchmarkRecordStore {
    private LuceneConfiguration params;

    private ExecutorService luceneExecutor;

    public void setup(LuceneConfiguration params) {
        this.params = params;
        ThreadFactory daemonFactory = r -> {
            Thread t = new Thread(r);
            t.setDaemon(true);
            return t;
        };
        if (params.numLuceneThreads < 0) {
            luceneExecutor = Executors.newCachedThreadPool(daemonFactory);
        } else {
            luceneExecutor = Executors.newFixedThreadPool(params.numLuceneThreads, daemonFactory);
        }
        int numContextThreads = params.numContextThreads;
        ExecutorService contextExecutor;
        if (numContextThreads < 0) {
            contextExecutor = Executors.newCachedThreadPool(daemonFactory);
        } else {
            contextExecutor = Executors.newFixedThreadPool(numContextThreads, daemonFactory);
        }
        factory.setExecutor(contextExecutor);

        init();
    }

    @Override
    public void run(@Nonnull final BenchmarkTimer timer, @Nonnull final Consumer<FDBRecordStore> body) {
        FDBRecordContextConfig.Builder cfg =
                FDBRecordContextConfig.newBuilder()
                        .setTimer(timer.getTimer())
                        .setMdcContext(timer.getMdc())
                        .setRecordContextProperties(RecordLayerPropertyStorage.newBuilder()
                                .addProp(LuceneRecordContextProperties.LUCENE_EXECUTOR_SERVICE, luceneExecutor)
                                .addProp(LuceneRecordContextProperties.LUCENE_INDEX_CURSOR_PAGE_SIZE, params.lucenePageSize)
                                .build());
        run(cfg, body);
    }
}
