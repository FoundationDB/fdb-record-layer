/*
 * TransactionConfigBuilderTest.java
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

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class TransactionConfigBuilderTest {
    private TransactionConfig.Builder builder = TransactionConfig.newBuilder();

    @Test
    void testSetTransactionId() {
        TransactionConfig config = builder.setTransactionId("FOO").build();
        assertThat(config.getTransactionId()).isEqualTo("FOO");
    }

    @Test
    void testSetLoggingContext() {
        Map<String, String> loggingContext = Map.of("A", "B", "C", "D");
        TransactionConfig config = builder.setLoggingContext(loggingContext).build();
        assertThat(config.getLoggingContext()).containsExactlyEntriesOf(loggingContext);
    }

    @Test
    void testSetWeakReadSemantics() {
        TransactionConfig.WeakReadSemantics semantics = new TransactionConfig.WeakReadSemantics(42, 43, true);
        TransactionConfig config = builder.setWeakReadSemantics(semantics).build();
        assertThat(config.getWeakReadSemantics()).isEqualTo(semantics);
    }

    @Test
    void testSetTransactionPriority() {
        TransactionConfig config = builder.build();
        assertThat(config.getTransactionPriority()).isEqualTo(TransactionConfig.Priority.DEFAULT);
        config = builder.setTransactionPriority(TransactionConfig.Priority.SYSTEM_IMMEDIATE).build();
        assertThat(config.getTransactionPriority()).isEqualTo(TransactionConfig.Priority.SYSTEM_IMMEDIATE);
        config = builder.setTransactionPriority(TransactionConfig.Priority.BATCH).build();
        assertThat(config.getTransactionPriority()).isEqualTo(TransactionConfig.Priority.BATCH);
    }

    @Test
    void testSetTransactionTimeoutMillis() {
        TransactionConfig config = builder.build();
        assertThat(config.getTransactionTimeoutMillis()).isEqualTo(-1L);
        config = builder.setTransactionTimeoutMillis(100).build();
        assertThat(config.getTransactionTimeoutMillis()).isEqualTo(100);
    }

    @Test
    void testSetEnableAssertions() {
        TransactionConfig config = builder.build();
        assertThat(config.isEnableAssertions()).isFalse();
        config = builder.setEnableAssertions(true).build();
        assertThat(config.isEnableAssertions()).isTrue();
    }

    @Test
    void testSetLogTransaction() {
        TransactionConfig config = builder.build();
        assertThat(config.isLogTransaction()).isFalse();
        config = builder.setLogTransaction(true).build();
        assertThat(config.isLogTransaction()).isTrue();
    }

    @Test
    void testSetTrackOpen() {
        TransactionConfig config = builder.build();
        assertThat(config.isTrackOpen()).isFalse();
        config = builder.setTrackOpen(true).build();
        assertThat(config.isTrackOpen()).isTrue();
    }

    @Test
    void testSetSaveOpenStackTrace() {
        TransactionConfig config = builder.build();
        assertThat(config.isSaveOpenStackTrace()).isFalse();
        config = builder.setSaveOpenStackTrace(true).build();
        assertThat(config.isSaveOpenStackTrace()).isTrue();
    }
}
