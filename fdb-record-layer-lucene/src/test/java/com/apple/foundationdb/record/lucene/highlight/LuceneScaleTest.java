/*
 * LuceneScaleTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2023 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.lucene.highlight;


import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestRecordsTextProto;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.lucene.LuceneFunctionNames;
import com.apple.foundationdb.record.lucene.LuceneIndexOptions;
import com.apple.foundationdb.record.lucene.LuceneIndexTestUtils;
import com.apple.foundationdb.record.lucene.LuceneIndexTypes;
import com.apple.foundationdb.record.lucene.LucenePlanner;
import com.apple.foundationdb.record.lucene.LuceneQueryComponent;
import com.apple.foundationdb.record.lucene.synonym.EnglishSynonymMapConfig;
import com.apple.foundationdb.record.lucene.synonym.SynonymMapRegistryImpl;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.TestKeySpace;
import com.apple.foundationdb.record.provider.foundationdb.indexes.TextIndexTestUtils;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.record.query.plan.PlannableIndexTypes;
import com.apple.foundationdb.record.query.plan.QueryPlanner;
import com.apple.foundationdb.record.query.plan.cascades.CascadesPlanner;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.google.protobuf.Message;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.metadata.Key.Expressions.function;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Low level scale test that does a variety of operations against lucene, generating a csv that shows various
 * store timer events as the number of records in the account goes up.
 * <p>
 *     Since this is run on a variety of hardware, in a non-isolated environment, metrics around specific millis aren't
 *     super reliable, but the metrics around data read, or written can be a good indication of how a change in code
 *     might impact performance in a production environment.
 * </p>
 * <p>
 *     The nested class {@link Config} is intended to hold the options, which you may adjust to see how a specific
 *     change interacts with a single run. We may at some point want to pull this out into something that is not
 *     committed, but this should work for now.
 * </p>
 * <p>
 *     The key test here is {@link #runPerfTest()}, which, depending on the config, does a loop of inserting some
 *     records, then doing some operations and capturing metrics and dumping them to csvs in
 *     {@code .out/LuceneScaleTest*}. There are some other tests in this class, mostly to make sure things work, or
 *     to run a profiler or debugger.
 * </p>
 */
@Tag(Tags.RequiresFDB)
@Tag(Tags.Performance)
// Generally, run this as long as you feel like, and stop when you want, or until it hits Config.LOOP_COUNT
@Timeout(value = 8, unit = TimeUnit.DAYS)
@SuppressWarnings("java:S2699")
public class LuceneScaleTest extends FDBRecordStoreTestBase {
    private static final Logger logger = LogManager.getLogger(LuceneScaleTest.class);

    /**
     * A holder of all the config that one might want to change when running the test, all in one place.
     */
    private static class Config {
        /**
         * The number of times to loop through the commands.
         */
        public static final int LOOP_COUNT = 1000;
        /**
         * If {@code true}, configure index to use {@link LuceneIndexOptions#PRIMARY_KEY_SERIALIZATION_FORMAT}.
         */
        static final boolean USE_PRIMARY_KEY_SERIALIZATION = false;
        /**
         * If {@code true}, configure index to use {@link LuceneIndexOptions#PRIMARY_KEY_SEGMENT_INDEX_ENABLED}.
         */
        static final boolean USE_PRIMARY_KEY_SEGMENT_INDEX = true;
        /**
         * If {@code true}, configure test to clear the path before running the test, otherwise continue with the records
         * that already existed, and the csvs that were already created.
         */
        static final boolean CLEAR_BEFORE_RUN = true;
        /**
         * The set of commands to run when running {@link #runPerfTest()}.
         */
        static final Set<Command> COMMANDS_TO_RUN = EnumSet.allOf(Command.class);
        /**
         * An id to allow running the test multiple times with different configs without having to start over from the
         * beginning;, this can be any string.
         */
        static final String ISOLATION_ID = "default";
    }

    private enum Command {
        IncreaseCount,
        Insert,
        Update,
        Search
    }

    private static final String RECORD_COUNT_COLUMN = "recordCount";
    private static final String OPERATION_MILLIS = "operationMillis";
    private static final String TOTAL_TEST_MILLIS = "totalTestMillis";
    static final List<String> CSV_COLUMNS = List.of(RECORD_COUNT_COLUMN, OPERATION_MILLIS, TOTAL_TEST_MILLIS,
            "bytes_deleted_count",
            "bytes_fetched_count", "bytes_read_count", "bytes_written_count", "commit_count", "commit_micros",
            "commit_read_only_count", "commit_read_only_micros", "commits_count", "commits_micros", "deletes_count",
            "empty_scans_count", "fetches_count", "get_read_version_count", "get_record_range_raw_first_chunk_count",
            "get_scan_range_raw_first_chunk_count", "jni_calls_count", "lucene_delete_file_count",
            "lucene_fdb_read_block_count", "lucene_get_file_length_count", "lucene_get_increment_calls_count",
            "lucene_list_all_count", "lucene_load_file_cache_count", "lucene_merge_count", "lucene_read_block_count",
            "lucene_rename_file_count", "lucene_write_call_count", "lucene_write_file_reference_call_count",
            "lucene_write_file_reference_size_count", "lucene_write_size_count", "mutations_count",
            "open_context_count", "range_deletes_count", "range_fetches_count", "range_keyvalues_fetched_count",
            "range_query_direct_buffer_miss_count", "range_reads_count", "reads_count", "save_record_count",
            "writes_count", "lucene_delete_document_by_query_count", "lucene_delete_document_by_primary_key_count");

    private static final String INDEX_NAME = "text_and_number_idx";
    private static final Index INDEX = new Index(
            INDEX_NAME,
            concat(function(LuceneFunctionNames.LUCENE_TEXT, field("text")), function(LuceneFunctionNames.LUCENE_STORED, field("is_seen"))),
            LuceneIndexTypes.LUCENE, configIndexOptions());

    private static Map<String, String> configIndexOptions() {
        final ImmutableMap.Builder<String, String> map = ImmutableMap.builder();
        if (Config.USE_PRIMARY_KEY_SERIALIZATION) {
            map.put(LuceneIndexOptions.PRIMARY_KEY_SERIALIZATION_FORMAT, "[INT64, INT64]");
        }
        if (Config.USE_PRIMARY_KEY_SEGMENT_INDEX) {
            map.put(LuceneIndexOptions.PRIMARY_KEY_SEGMENT_INDEX_ENABLED, "true");
        }
        return map.build();
    }

    public LuceneScaleTest() {
        super(TestKeySpace.getKeyspacePath("record-test", "performance", "luceneScaleTest")
                .add("run", Config.ISOLATION_ID));
    }

    @BeforeAll
    public static void setup() {
        //set up the English Synonym Map so that we don't spend forever setting it up for every test, because this takes a long time
        SynonymMapRegistryImpl.instance().getSynonymMap(EnglishSynonymMapConfig.ExpandedEnglishSynonymMapConfig.CONFIG_NAME);
    }

    @Override
    protected void clear() {
        if (Config.CLEAR_BEFORE_RUN) {
            super.clear();
        }
    }

    @Override
    public void setupPlanner(@Nullable PlannableIndexTypes indexTypes) {
        if (useRewritePlanner) {
            planner = new CascadesPlanner(recordStore.getRecordMetaData(), recordStore.getRecordStoreState());
        } else {
            if (indexTypes == null) {
                indexTypes = new PlannableIndexTypes(
                        Sets.newHashSet(IndexTypes.VALUE, IndexTypes.VERSION),
                        Sets.newHashSet(IndexTypes.RANK, IndexTypes.TIME_WINDOW_LEADERBOARD),
                        Sets.newHashSet(IndexTypes.TEXT),
                        Sets.newHashSet(LuceneIndexTypes.LUCENE)
                );
            }

            planner = new LucenePlanner(recordStore.getRecordMetaData(), recordStore.getRecordStoreState(), indexTypes, recordStore.getTimer());
        }
    }

    @Test
    void updateProfile() {
        // run some updates in a way that is suitable for using with the profiler
        DataModel dataModel = new DataModel();
        dataModel.prep();
        final int updatesPerContext = 10;
        final int updateBatches = 1000;
        final String recordCount = "recordCount";
        timer.reset();
        for (int j = 0; j < updateBatches; j++) {
            dataModel.updateRecords(updatesPerContext);
        }
        final Map<String, Number> keysAndValues = timer.getKeysAndValues();
        logger.info(KeyValueLogMessage.build("Did updates")
                .addKeysAndValues(keysAndValues)
                .addKeyAndValue("updatesPerContext", updatesPerContext)
                .addKeyAndValue("updateBatches", updateBatches)
                .addKeyAndValue(recordCount, dataModel.maxDocId)
                .toString());
    }

    @Test
    void runPerfTest() throws IOException, ExecutionException, InterruptedException {
        DataModel dataModel = new DataModel();
        dataModel.prep();
        final int updatesPerContext = 10;
        final int operationCount = 10;

        final long testStartMillis = System.currentTimeMillis();

        try (var updatesCsv = createPrintStream(".out/LuceneScaleTest.updates.csv", dataModel.continuing);
                var insertsCsv = createPrintStream(".out/LuceneScaleTest.inserts.csv", dataModel.continuing);
                var searchesCsv = createPrintStream(".out/LuceneScaleTest.searches.csv", dataModel.continuing)) {

            for (int i = 0; i < Config.LOOP_COUNT; i++) {
                long startMillis;
                if (Config.COMMANDS_TO_RUN.contains(Command.IncreaseCount)) {
                    dataModel.saveNewRecords(90);
                }
                if (Config.COMMANDS_TO_RUN.contains(Command.Insert)) {
                    timer.reset();
                    startMillis = System.currentTimeMillis();
                    for (int j = 0; j < operationCount; j++) {
                        dataModel.saveNewRecord();
                    }
                    updateCsv("Did insert", dataModel, insertsCsv, startMillis, testStartMillis, Map.of());
                }
                if (Config.COMMANDS_TO_RUN.contains(Command.Update)) {
                    timer.reset();
                    startMillis = System.currentTimeMillis();
                    for (int j = 0; j < operationCount; j++) {
                        dataModel.updateRecords(updatesPerContext);
                    }
                    updateCsv("Did updates", dataModel, updatesCsv, startMillis, testStartMillis,
                            Map.of("updatesPerContext", updatesPerContext,
                                    "updateBatches", operationCount));
                }
                if (Config.COMMANDS_TO_RUN.contains(Command.Search)) {
                    timer.reset();
                    startMillis = System.currentTimeMillis();
                    for (int j = 0; j < operationCount; j++) {
                        dataModel.search();
                    }
                    updateCsv("Did Search", dataModel, searchesCsv, startMillis, testStartMillis, Map.of());
                }
                dataModel.updateSearchWords();

            }
        }
    }

    private void updateCsv(final String logTtl, final DataModel dataModel, final PrintStream csvPrintStream,
                           final long startMillis, final long testStartMillis, final Map<?, ?> additionalKeysAndValues) {
        final Map<String, Number> keysAndValues = timer.getKeysAndValues();
        logger.info(KeyValueLogMessage.build(logTtl)
                .addKeysAndValues(keysAndValues)
                .addKeysAndValues(additionalKeysAndValues)
                .addKeyAndValue(RECORD_COUNT_COLUMN, dataModel.maxDocId)
                .toString());

        for (final String key : CSV_COLUMNS) {
            if (Objects.equals(key, RECORD_COUNT_COLUMN)) {
                csvPrintStream.print(dataModel.maxDocId);
            } else if (Objects.equals(key, OPERATION_MILLIS)) {
                csvPrintStream.print(System.currentTimeMillis() - startMillis);
            } else if (Objects.equals(key, TOTAL_TEST_MILLIS)) {
                csvPrintStream.print(System.currentTimeMillis() - testStartMillis);
            } else {
                csvPrintStream.print(keysAndValues.get(key));
            }
            csvPrintStream.print(",");
        }
        csvPrintStream.println();
    }

    @Nonnull
    private static PrintStream createPrintStream(final String name, final boolean append) throws FileNotFoundException {
        final PrintStream printStream = new PrintStream(new FileOutputStream(name, append), true);

        boolean success = false;
        try {
            if (!append) {
                printStream.println(String.join(",", CSV_COLUMNS));
            }
            success = true;
        } finally {
            if (!success) {
                printStream.close();
            }
        }
        return printStream;
    }

    private class DataModel {
        int maxDocId = 0;
        Random random = new Random();
        private boolean continuing;
        private final List<String> searchWords = new ArrayList<>(SEARCH_WORD_COUNT);
        private static final int SEARCH_WORD_COUNT = 20;
        private int lastSearchWordsUpdate = -10000;


        void prep() {
            try (FDBRecordContext context = openContext()) {
                maxDocId = context.asyncToSync(FDBStoreTimer.Waits.WAIT_LOAD_SYSTEM_KEY,
                        openStore(context).scanRecords(TupleRange.ALL, null, ScanProperties.FORWARD_SCAN)
                                .getCount());
                continuing = maxDocId > 0;
            }
            if (maxDocId > 0) {
                updateSearchWords();
            }
        }

        private void updateSearchWords() {
            if (Math.floor(lastSearchWordsUpdate / 1000.0) >= Math.floor(maxDocId / 1000.0)) {
                return;
            }
            logger.info("Updating search words " + maxDocId);
            try (FDBRecordContext context = openContext()) {
                final FDBRecordStore store = openStore(context);
                final int count = Math.min(SEARCH_WORD_COUNT, maxDocId);
                for (int i = 0; i < count; i++) {
                    final String text = TestRecordsTextProto.ComplexDocument.newBuilder()
                            .mergeFrom(getRandomRecord(store))
                            .getText();
                    final String messageWord = getRandomWord(text);
                    searchWords.add(messageWord);
                }
            }
            lastSearchWordsUpdate = maxDocId;
        }

        private String getRandomWord(final String text) {
            final String[] messageWords = text
                    .split(" ");
            return messageWords[random.nextInt(messageWords.length)];
        }

        void saveNewRecords(int count) {
            for (int i = 0; i < count; i++) {
                // TODO save more than one record per transaction
                saveNewRecord();
            }
        }

        private FDBRecordStore openStore(final FDBRecordContext context) {
            final Pair<FDBRecordStore, QueryPlanner> res = LuceneIndexTestUtils.rebuildIndexMetaData(context, path, TextIndexTestUtils.COMPLEX_DOC, INDEX, false);
            recordStore = res.getLeft();
            planner = res.getRight();
            return recordStore;
        }

        void saveNewRecord() {
            try (FDBRecordContext context = openContext()) {
                final FDBRecordStore store = openStore(context);
                final String text = LuceneIndexTestUtils.generateRandomWords(500)[1];
                store.saveRecord(
                        TestRecordsTextProto.ComplexDocument.newBuilder()
                                .setDocId(maxDocId)
                                .setText(text)
                                .setIsSeen(random.nextBoolean())
                                .setGroup(1)
                                .build());
                maxDocId++;
                if (searchWords.size() < SEARCH_WORD_COUNT) {
                    final String messageWord = getRandomWord(text);
                    searchWords.add(messageWord);
                } else {
                    if (random.nextInt(100) == 0) {
                        final String messageWord = getRandomWord(text);
                        searchWords.set(random.nextInt(SEARCH_WORD_COUNT), messageWord);
                    }
                }
                context.commit();
            }
        }

        public void updateRecords(final int count) {
            try (FDBRecordContext context = openContext()) {
                final FDBRecordStore store = openStore(context);
                for (int i = 0; i < count; i++) {
                    final TestRecordsTextProto.ComplexDocument.Builder builder = TestRecordsTextProto.ComplexDocument.newBuilder()
                            .mergeFrom(getRandomRecord(store));
                    builder.setIsSeen(!builder.getIsSeen());
                    store.saveRecord(builder.build());
                }
                context.commit();
            }
        }

        @Nonnull
        private Message getRandomRecord(final FDBRecordStore store) {
            // TODO randomly get a record, but skew it towards more recent ones...
            return store.loadRecord(Tuple.from(1, random.nextInt(maxDocId))).getRecord();
        }

        public void search() throws ExecutionException, InterruptedException {
            useRewritePlanner = false;
            try (FDBRecordContext context = openContext()) {
                final FDBRecordStore store = openStore(context);
                final String searchWord = searchWords.get(random.nextInt(searchWords.size()));
                QueryComponent filter = new LuceneQueryComponent("text:" + searchWord, List.of("text"));
                RecordQuery query = RecordQuery.newBuilder()
                        .setRecordType(TextIndexTestUtils.COMPLEX_DOC)
                        .setFilter(filter)
                        .build();

                final RecordQueryPlan plan = planner.plan(query);
                assertThat(plan.getUsedIndexes(), Matchers.contains(INDEX_NAME));
                try (RecordCursor<FDBQueriedRecord<Message>> results = plan.execute(store)) {
                    assertThat(results.getCount().get(), Matchers.greaterThan(0));
                }
            }
        }
    }
}
