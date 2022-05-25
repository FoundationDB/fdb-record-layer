/*
 * TransactionBoundEmbeddedRelationalExtension.java
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

package com.apple.foundationdb.relational.recordlayer;

import com.apple.foundationdb.relational.api.EmbeddedRelationalEngine;
import com.apple.foundationdb.relational.api.metrics.NoOpMetricRegistry;
import com.apple.foundationdb.relational.transactionbound.TransactionBoundStorageCluster;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.List;
import java.util.Map;

public class TransactionBoundEmbeddedRelationalExtension implements RelationalExtension, BeforeEachCallback, AfterEachCallback {
    private EmbeddedRelationalEngine engine;

    public TransactionBoundEmbeddedRelationalExtension() {
    }

    @Override
    public void afterEach(ExtensionContext context) throws Exception {
        engine.deregisterDriver();
    }

    @Override
    public void beforeEach(ExtensionContext context) throws Exception {
        engine = new EmbeddedRelationalEngine(
                List.of(TransactionBoundStorageCluster.INSTANCE),
                NoOpMetricRegistry.INSTANCE
                );
        engine.registerDriver(); //register the engine driver
    }

    public EmbeddedRelationalEngine getEngine() {
        return engine;
    }

    public Map<String, Object> getStoreTimerMetrics() {
        return null;
    }
}
