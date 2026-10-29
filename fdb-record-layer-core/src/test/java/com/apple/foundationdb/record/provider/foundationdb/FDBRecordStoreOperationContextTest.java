/*
 * FDBRecordStoreOperationContextTest.java
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

package com.apple.foundationdb.record.provider.foundationdb;

import com.apple.foundationdb.record.IndexState;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.RecordMetaDataProvider;
import com.apple.foundationdb.record.TestHelpers;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.test.Tags;
import org.apache.logging.log4j.Level;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that operations on a store log a single summary of what they did, in {@link RecordStoreOperationContext}'s format.
 */
@Tag(Tags.RequiresFDB)
class FDBRecordStoreOperationContextTest extends FDBRecordStoreTestBase {
    private static final String SUMMARY = "record store state changed";
    // Logged directly at DEBUG, outside the summary mechanism.
    private static final String INDEXES_NEED_REBUILDING = "indexes need rebuilding";
    private static final String INDEX_NAME = "log_test_index";

    private static final class Captured {
        private List<String> info;
        private List<String> debug;
    }

    @Nonnull
    private static RecordMetaDataBuilder metaDataBuilder() {
        return RecordMetaData.newBuilder().setRecords(TestRecords1Proto.getDescriptor());
    }

    // The store's identification in log messages, as KeyValueLogMessage renders it, used to ignore other tests' logs.
    @Nonnull
    private String storeIdentity(@Nonnull FDBRecordContext context) {
        final SubspaceProviderByKeySpacePath provider = new SubspaceProviderByKeySpacePath(path);
        provider.getSubspace(context);
        return provider.toString(context).replace("\"", "'");
    }

    @Nonnull
    private Captured capture(@Nonnull String storeIdentity, @Nonnull Callable<?> body) {
        final Captured captured = new Captured();
        final List<String> debug = TestHelpers.collectLogs(FDBRecordStore.class, Level.DEBUG, () -> {
            captured.info = TestHelpers.collectLogs(FDBRecordStore.class, Level.INFO, body);
            return null;
        });
        captured.info = captured.info.stream().filter(message -> message.contains(storeIdentity)).collect(Collectors.toList());
        captured.debug = debug.stream()
                .filter(message -> message.contains(storeIdentity) && !message.startsWith(INDEXES_NEED_REBUILDING))
                .collect(Collectors.toList());
        return captured;
    }

    private static void assertOnlySummaries(@Nonnull Captured captured) {
        assertThat(captured.info).allSatisfy(message -> assertThat(message).startsWith(SUMMARY));
        assertThat(captured.debug).allSatisfy(message -> assertThat(message).startsWith(SUMMARY));
    }

    private void createStore(@Nonnull RecordMetaData metaData) {
        try (FDBRecordContext context = openContext()) {
            getStoreBuilder(context, metaData, path).createOrOpen();
            commit(context);
        }
    }

    @Test
    void newStoreLogsOneSummary() {
        final RecordMetaData metaData = metaDataBuilder().build();
        try (FDBRecordContext context = openContext()) {
            final Captured captured = capture(storeIdentity(context), () -> getStoreBuilder(context, metaData, path).createOrOpen());
            assertThat(captured.info).hasSize(1);
            assertThat(captured.info.get(0)).contains("new_store=\"true\"");
            assertOnlySummaries(captured);
        }
    }

    @Test
    void reopeningWithoutChangesLogsNothing() {
        final RecordMetaData metaData = metaDataBuilder().build();
        createStore(metaData);
        try (FDBRecordContext context = openContext()) {
            final Captured captured = capture(storeIdentity(context), () -> getStoreBuilder(context, metaData, path).createOrOpen());
            assertThat(captured.info).isEmpty();
            assertThat(captured.debug).isEmpty();
        }
    }

    @Test
    void addedIndexLeftDisabledIsOneSummary() {
        final RecordMetaDataBuilder builder = metaDataBuilder();
        createStore(builder.build());
        builder.addIndex("MySimpleRecord", INDEX_NAME, "num_value_2");
        final RecordMetaData metaData = builder.build();
        try (FDBRecordContext context = openContext()) {
            final Captured captured = capture(storeIdentity(context), () -> getStoreBuilder(context, metaData, path)
                    .setUserVersionChecker(new FixedChecker(0, IndexState.DISABLED))
                    .createOrOpen());
            assertThat(captured.info).hasSize(1);
            assertThat(captured.info.get(0))
                    .contains("old_meta_data_version=")
                    .contains("new_meta_data_version=")
                    .contains("index_changes=\"" + INDEX_NAME + ":" + INDEX_NAME + ":-:-:DISABLED\"")
                    .doesNotContain("new_store=");
            assertOnlySummaries(captured);
            assertThat(captured.debug).isEmpty();
        }
    }

    @Test
    void addedIndexBuiltInlineIsOneSummary() {
        final RecordMetaDataBuilder builder = metaDataBuilder();
        createStore(builder.build());
        builder.addIndex("MySimpleRecord", INDEX_NAME, "num_value_2");
        final RecordMetaData metaData = builder.build();
        try (FDBRecordContext context = openContext()) {
            final Captured captured = capture(storeIdentity(context), () -> getStoreBuilder(context, metaData, path)
                    .setUserVersionChecker(new FixedChecker(0, IndexState.READABLE))
                    .createOrOpen());
            assertThat(captured.info).hasSize(1);
            assertThat(captured.info.get(0)).contains("index_changes=\"" + INDEX_NAME + ":" + INDEX_NAME + ":built_inline:-:READABLE\"");
            assertOnlySummaries(captured);
        }
    }

    @Test
    void removedFormerIndexIsNotLoggedAsReadable() {
        final RecordMetaDataBuilder builder = metaDataBuilder();
        builder.addIndex("MySimpleRecord", INDEX_NAME, "num_value_2");
        createStore(builder.build());
        builder.removeIndex(INDEX_NAME);
        final RecordMetaData metaData = builder.build();
        try (FDBRecordContext context = openContext()) {
            final Captured captured = capture(storeIdentity(context), () -> getStoreBuilder(context, metaData, path).createOrOpen());
            assertThat(captured.info).hasSize(1);
            assertThat(captured.info.get(0))
                    .contains("removed_former_indexes=\"" + INDEX_NAME + ":" + INDEX_NAME + "\"")
                    .doesNotContain("index_changes=")
                    .doesNotContain("READABLE");
            assertOnlySummaries(captured);
        }
    }

    @Test
    void userVersionChangeIsOneSummary() {
        final RecordMetaData metaData = metaDataBuilder().build();
        try (FDBRecordContext context = openContext()) {
            getStoreBuilder(context, metaData, path).setUserVersionChecker(new FixedChecker(1, IndexState.DISABLED)).createOrOpen();
            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            final Captured captured = capture(storeIdentity(context), () -> getStoreBuilder(context, metaData, path)
                    .setUserVersionChecker(new FixedChecker(2, IndexState.DISABLED))
                    .createOrOpen());
            assertThat(captured.info).hasSize(1);
            assertThat(captured.info.get(0))
                    .contains("old_user_version=\"1\"")
                    .contains("new_user_version=\"2\"");
            assertOnlySummaries(captured);
        }
    }

    @Test
    void directIndexStateChangeIsOneSummary() {
        final RecordMetaDataBuilder builder = metaDataBuilder();
        builder.addIndex("MySimpleRecord", INDEX_NAME, "num_value_2");
        final RecordMetaData metaData = builder.build();
        createStore(metaData);
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore store = getStoreBuilder(context, metaData, path).createOrOpen();
            final Captured captured = capture(storeIdentity(context), () ->
                    context.asyncToSync(FDBStoreTimer.Waits.WAIT_ERROR_CHECK, store.markIndexDisabled(INDEX_NAME)));
            assertThat(captured.info).hasSize(1);
            assertThat(captured.info.get(0))
                    .contains("store_operation=\"MARK_INDEX_DISABLED\"")
                    .contains("index_changes=\"" + INDEX_NAME + ":" + INDEX_NAME + ":-:READABLE:DISABLED\"");
            assertOnlySummaries(captured);
        }
    }

    @Test
    void explicitRebuildIsOneSummary() {
        final RecordMetaDataBuilder builder = metaDataBuilder();
        builder.addIndex("MySimpleRecord", INDEX_NAME, "num_value_2");
        final RecordMetaData metaData = builder.build();
        createStore(metaData);
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore store = getStoreBuilder(context, metaData, path).createOrOpen();
            final Index index = metaData.getIndex(INDEX_NAME);
            final Captured captured = capture(storeIdentity(context), () ->
                    context.asyncToSync(FDBStoreTimer.Waits.WAIT_ERROR_CHECK, store.rebuildIndex(index)));
            assertThat(captured.info).hasSize(1);
            assertThat(captured.info.get(0))
                    .contains("store_operation=\"REBUILD_INDEX\"")
                    .contains("reason=\"EXPLICIT\"")
                    .contains("index_changes=\"" + INDEX_NAME + ":" + INDEX_NAME + ":built_inline:READABLE:READABLE\"");
            assertOnlySummaries(captured);
        }
    }

    private static final class FixedChecker implements FDBRecordStoreBase.UserVersionChecker {
        private final int userVersion;
        @Nonnull
        private final IndexState indexState;

        private FixedChecker(int userVersion, @Nonnull IndexState indexState) {
            this.userVersion = userVersion;
            this.indexState = indexState;
        }

        @Override
        public CompletableFuture<Integer> checkUserVersion(@Nonnull RecordMetaDataProto.DataStoreInfo storeHeader,
                                                           RecordMetaDataProvider metaData) {
            return CompletableFuture.completedFuture(userVersion);
        }

        @Deprecated
        @Override
        public CompletableFuture<Integer> checkUserVersion(int oldUserVersion, int oldMetaDataVersion,
                                                           RecordMetaDataProvider metaData) {
            return Assertions.fail(); // the other overload is the one that is called
        }

        @Nonnull
        @Override
        public CompletableFuture<IndexState> needRebuildIndex(Index index,
                                                              Supplier<CompletableFuture<Long>> lazyRecordCount,
                                                              Supplier<CompletableFuture<Long>> lazyEstimatedSize,
                                                              boolean indexOnNewRecordTypes) {
            return CompletableFuture.completedFuture(indexState);
        }
    }
}
