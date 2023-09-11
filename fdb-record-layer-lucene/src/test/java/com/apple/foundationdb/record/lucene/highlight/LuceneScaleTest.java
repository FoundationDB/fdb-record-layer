package com.apple.foundationdb.record.lucene.highlight;


import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestRecordsTextProto;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.lucene.LuceneFunctionNames;
import com.apple.foundationdb.record.lucene.LuceneIndexTestUtils;
import com.apple.foundationdb.record.lucene.LuceneIndexTypes;
import com.apple.foundationdb.record.lucene.LucenePlanner;
import com.apple.foundationdb.record.lucene.LuceneQueryComponent;
import com.apple.foundationdb.record.lucene.directory.FDBDirectory;
import com.apple.foundationdb.record.lucene.synonym.EnglishSynonymMapConfig;
import com.apple.foundationdb.record.lucene.synonym.SynonymMapRegistryImpl;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.indexes.TextIndexTestUtils;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.record.query.plan.PlannableIndexTypes;
import com.apple.foundationdb.record.query.plan.QueryPlanner;
import com.apple.foundationdb.record.query.plan.cascades.CascadesPlanner;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
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
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.metadata.Key.Expressions.function;
import static org.hamcrest.MatcherAssert.assertThat;

@Tag(Tags.RequiresFDB)
@Tag(Tags.Slow)
@Timeout(value = 8, unit = TimeUnit.HOURS)
public class LuceneScaleTest extends FDBRecordStoreTestBase {
    private static final Logger logger = LogManager.getLogger(LuceneScaleTest.class);

    private static final String RECORD_COUNT_COLUMN = "recordCount";
    private static final String TOTAL_TIME_COLUMN = "totalTime";
    static final List<String> CSV_COLUMNS = List.of(RECORD_COUNT_COLUMN, TOTAL_TIME_COLUMN, "bytes_deleted_count",
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
            "writes_count");

    private static final String INDEX_NAME = "text_and_number_idx";
    private static final Index INDEX = new Index(
            INDEX_NAME,
            concat(function(LuceneFunctionNames.LUCENE_TEXT, field("text")), function(LuceneFunctionNames.LUCENE_STORED, field("is_seen"))),
            LuceneIndexTypes.LUCENE,
            Map.of(
                    //LuceneIndexOptions.PRIMARY_KEY_SERIALIZATION_FORMAT, "[INT64, INT64]"
            ));

    @BeforeAll
    public static void setup() {
        //set up the English Synonym Map so that we don't spend forever setting it up for every test, because this takes a long time
        SynonymMapRegistryImpl.instance().getSynonymMap(EnglishSynonymMapConfig.ExpandedEnglishSynonymMapConfig.CONFIG_NAME);
    }

    @Override
    protected void clear() {
//        super.clear();
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
            clearBlockReads();
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
    void runOneInsert() {
        DataModel dataModel = new DataModel();
        dataModel.prep();
        dataModel.saveNewRecords(1);
    }

    @Test
    void runPerfTest() throws IOException, ExecutionException, InterruptedException {
        DataModel dataModel = new DataModel();
        dataModel.prep();
        final int updatesPerContext = 10;
        final int operationCount = 10;

        try (var updatesCsv = createPrintStream(".out/LuceneScaleTest.updates.csv", dataModel.continuing);
             var insertsCsv = createPrintStream(".out/LuceneScaleTest.inserts.csv", dataModel.continuing);
             var searchesCsv = createPrintStream(".out/LuceneScaleTest.searches.csv", dataModel.continuing)) {

            final boolean increaseCount = true;
            final boolean doInsert = true;
            final boolean doUpdate = true;
            final boolean doSearch = true;
            for (int i = 0; i < 1000; i++) {
                long startMillis;
                if (increaseCount) {
                    dataModel.saveNewRecords(90);
                }
                if (doInsert) {
                    timer.reset();
                    startMillis = System.currentTimeMillis();
                    for (int j = 0; j < operationCount; j++) {
                        clearBlockReads();
                        dataModel.saveNewRecord();
                        dumpBlockReads(dataModel);
                    }
                    updateCsv("Did insert", dataModel, insertsCsv, startMillis, Map.of());
                }
                if (doUpdate) {
                    timer.reset();
                    startMillis = System.currentTimeMillis();
                    for (int j = 0; j < operationCount; j++) {
                        clearBlockReads();
                        dataModel.updateRecords(updatesPerContext);
                        //dumpBlockReads(dataModel);
                    }
                    updateCsv("Did updates", dataModel, updatesCsv, startMillis,
                            Map.of("updatesPerContext", updatesPerContext,
                                    "updateBatches", operationCount));
                }
                if (doSearch) {
                    timer.reset();
                    startMillis = System.currentTimeMillis();
                    for (int j = 0; j < operationCount; j++) {
                        clearBlockReads();
                        dataModel.search();
                        // TODO dump block reads
                    }
                    updateCsv("Did Search", dataModel, searchesCsv, startMillis, Map.of());
                }
                dataModel.updateSearchWords();

            }
        }
    }

    private static void clearBlockReads() {
        FDBDirectory.blocksRead.clear();
        FDBDirectory.readStacks.clear();
    }

    private void updateCsv(final String logTtl, final DataModel dataModel, final PrintStream csvPrintStream,
                           final long startMillis, final Map<?, ?> additionalKeysAndValues) {
        final Map<String, Number> keysAndValues = timer.getKeysAndValues();
        logger.info(KeyValueLogMessage.build(logTtl)
                .addKeysAndValues(keysAndValues)
                .addKeysAndValues(additionalKeysAndValues)
                .addKeyAndValue(RECORD_COUNT_COLUMN, dataModel.maxDocId)
                .toString());

        for (final String key : CSV_COLUMNS) {
            if (Objects.equals(key, RECORD_COUNT_COLUMN)) {
                csvPrintStream.print(dataModel.maxDocId);
            } else if (Objects.equals(key, TOTAL_TIME_COLUMN)) {
                csvPrintStream.print(System.currentTimeMillis() - startMillis);
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

    private static void dumpBlockReads(final DataModel dataModel) throws FileNotFoundException {
        final int maxDocId = dataModel.maxDocId;
        if (maxDocId < 10 ||
            (maxDocId < 100 && maxDocId % 10 == 0) ||
            (maxDocId < 1000 && maxDocId % 100 == 0) ||
            (maxDocId % 1000 == 0)) {
            try (var blockReads = createPrintStream(String.format(".out/blockReads-%06d.txt", maxDocId), false)) {
                blockReads.println("=====================");
                blockReads.println("recordCount: " + maxDocId);
                blockReads.println("OutsideCache: " + FDBDirectory.blocksRead.values().stream().mapToInt(FDBDirectory.DoubleCounter::getOutsideCache).sum());
                blockReads.println("InsideCache: " + FDBDirectory.blocksRead.values().stream().mapToInt(FDBDirectory.DoubleCounter::getInsideCache).sum());
                FDBDirectory.blocksRead.entrySet().stream().sorted(Comparator.comparing(entry -> entry.getKey().toString()))
                        .forEach(entry -> blockReads.println(entry.getKey() + ": " + entry.getValue()));
                blockReads.println("OutsideCache: " + FDBDirectory.readStacks.values().stream().mapToInt(FDBDirectory.DoubleCounter::getOutsideCache).sum());
                blockReads.println("InsideCache: " + FDBDirectory.readStacks.values().stream().mapToInt(FDBDirectory.DoubleCounter::getInsideCache).sum());
                FDBDirectory.readStacks.entrySet().stream()
                        .sorted(Comparator.comparing(entry -> entry.getValue().getInsideCache()))
                        .forEach(entry -> {
                            blockReads.println("Count: " + entry.getValue());
                            blockReads.println(entry.getKey());
                        });
            }
        }
    }

    private class DataModel {
        int maxDocId = 0;
        Random random = new Random();
        private boolean continuing;
        private final List<String> searchWords = new ArrayList<>(SEARCH_WORD_COUNT);
        private static final int SEARCH_WORD_COUNT = 20;
        private int lastSearchWordsUpdate = -10000;

        private PlannableIndexTypes plannableIndexTypes = new PlannableIndexTypes(
                Sets.newHashSet(IndexTypes.VALUE, IndexTypes.VERSION),
                Sets.newHashSet(IndexTypes.RANK, IndexTypes.TIME_WINDOW_LEADERBOARD),
                Sets.newHashSet(IndexTypes.TEXT),
                Sets.newHashSet(LuceneIndexTypes.LUCENE));


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
