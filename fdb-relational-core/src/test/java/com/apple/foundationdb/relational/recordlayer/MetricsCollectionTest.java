/*
 * MetricsCollectionTest.java
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

import com.apple.foundationdb.relational.api.TransactionConfig;
import com.apple.foundationdb.relational.utils.SimpleDatabaseRule;
import com.apple.foundationdb.relational.utils.TestSchemas;

import com.codahale.metrics.MetricSet;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class MetricsCollectionTest {
    @RegisterExtension
    @Order(0)
    public static final EmbeddedRelationalExtension relational = new EmbeddedRelationalExtension();

    @RegisterExtension
    @Order(1)
    public final SimpleDatabaseRule database = new SimpleDatabaseRule(relational, TransactionConfig.class, TestSchemas.restaurant());

    @Test
    void canRecoverFDBLevelMetrics() {
        /*
         * this test chooses a small set of metrics that we can analyze to know comes from the internals of FDB,
         * and makes sure that they are carried through to the MetricRegistry and thus out to our end callers.
         *
         * Most of these metrics are created simply by defining the system and creating a database, so we don't have
         * to do too much
         */
        MetricSet ms = relational.getEngine().getEngineMetrics();
        Assertions.assertThat(ms.getMetrics())
                .containsKey("JNI_CALLS")
                .containsKey("FETCHES")
                .containsKey("BYTES_WRITTEN")
                .containsKey("RANGE_DELETES")
                .containsKey("RANGE_READS");
    }

    @Test
    void canRecoverRecordLayerMetrics() {
        /*
         * this test chooses a small set of metrics that we can analyze to know comes from the internals of RecordLayer,
         * and makes sure that they are carried through to the MetricRegistry and thus out to our end callers.
         *
         * Most of these metrics are created simply by defining the system and creating a database, so we don't have
         * to do too much
         */
        MetricSet ms = relational.getEngine().getEngineMetrics();
        Assertions.assertThat(ms.getMetrics())
                .containsKey("WAIT_LOAD_RECORD")
                .containsKey("CREATE_RECORD_STORE")
                .containsKey("INDEXES_NEED_REBUILDING")
                .containsKey("WAIT_CHECK_VERSION")
                .containsKey("OPEN_CONTEXT");
    }
}
