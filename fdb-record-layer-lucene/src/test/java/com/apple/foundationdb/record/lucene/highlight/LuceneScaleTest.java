package com.apple.foundationdb.record.lucene.highlight;


import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestRecordsTextProto;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.lucene.LuceneFunctionNames;
import com.apple.foundationdb.record.lucene.LuceneIndexOptions;
import com.apple.foundationdb.record.lucene.LuceneIndexTestUtils;
import com.apple.foundationdb.record.lucene.LuceneIndexTypes;
import com.apple.foundationdb.record.lucene.directory.FDBDirectory;
import com.apple.foundationdb.record.lucene.synonym.EnglishSynonymMapConfig;
import com.apple.foundationdb.record.lucene.synonym.SynonymMapRegistryImpl;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.indexes.TextIndexTestUtils;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.metadata.Key.Expressions.function;

@Tag(Tags.RequiresFDB)
@Tag(Tags.Slow)
@Timeout(value = 8, unit = TimeUnit.HOURS)
public class LuceneScaleTest extends FDBRecordStoreTestBase {
    private static final Logger logger = LogManager.getLogger(LuceneScaleTest.class);

    private static final Index INDEX = new Index(
            "text_and_number_idx",
            concat(function(LuceneFunctionNames.LUCENE_TEXT, field("text")), function(LuceneFunctionNames.LUCENE_STORED, field("is_seen"))),
            LuceneIndexTypes.LUCENE,
            Map.of(
                    LuceneIndexOptions.PRIMARY_KEY_SERIALIZATION_FORMAT, "[INT64, INT64]"
            ));

    @BeforeAll
    public static void setup() {
        //set up the English Synonym Map so that we don't spend forever setting it up for every test, because this takes a long time
        SynonymMapRegistryImpl.instance().getSynonymMap(EnglishSynonymMapConfig.ExpandedEnglishSynonymMapConfig.CONFIG_NAME);
    }

    @Test
    void update() throws IOException {
        DataModel dataModel = new DataModel();
        dataModel.prep();
        final int updatesPerContext = 10;
        final int updateBatches = 100;
        final String recordCount = "recordCount";
        final String updateTime = "updateTime";

        try (var csv = new PrintStream(new FileOutputStream(".out/results.csv", true), true);
             var blockReads = new PrintStream(new FileOutputStream(".out/blockReads.txt", false), true)) {
            List<String> keys = null;
            for (int i = 0; i < 1000; i++) {
                dataModel.saveNewRecords(100);
                timer.reset();
                final long startMillis = System.currentTimeMillis();
                for (int j = 0; j < updateBatches; j++) {
                    FDBDirectory.blocksRead.clear();
                    FDBDirectory.readStacks.clear();
                    dataModel.updateRecords(updatesPerContext);
                    blockReads.println("=====================");
                    blockReads.println("recordCount: " + dataModel.maxDocId);
                    FDBDirectory.blocksRead.forEach((tuple, count) -> {
                        blockReads.println(tuple + ": " + count);
                    });
                    FDBDirectory.readStacks.entrySet().stream()
                            .sorted(Comparator.comparing(entry -> entry.getValue().get()))
                            .forEach(entry -> {
                        blockReads.println("Count: " + entry.getValue());
                        blockReads.println(entry.getKey());
                    });
                }
                final Map<String, Number> keysAndValues = timer.getKeysAndValues();
                logger.info(KeyValueLogMessage.build("Did updates")
                        .addKeysAndValues(keysAndValues)
                        .addKeyAndValue("updatesPerContext", updatesPerContext)
                        .addKeyAndValue("updateBatches", updateBatches)
                        .addKeyAndValue(recordCount, dataModel.maxDocId)
                        .toString());
                if (keys == null) {
                    keys = Stream.concat(Stream.of(recordCount, updateTime), keysAndValues.keySet().stream().sorted())
                            .collect(Collectors.toList());
                    if (!dataModel.continuing) {
                        csv.println(String.join(",", keys));
                    }
                }

                for (final String key : keys) {
                    if (Objects.equals(key, recordCount)) {
                        csv.print(dataModel.maxDocId);
                    } else if (Objects.equals(key, updateTime)) {
                        csv.print(System.currentTimeMillis() - startMillis);
                    } else {
                        csv.print(keysAndValues.get(key));
                    }
                    csv.print(",");
                }
                csv.println();
            }
        }
    }

    private class DataModel {
        int maxDocId = 0;
        Random random = new Random();
        private boolean continuing;

        /**
         * Prep the run, loading a few documents to make sure that static initializers have all been exercised.
         */
        void prep() {
            try (FDBRecordContext context = openContext()) {
                maxDocId = context.asyncToSync(FDBStoreTimer.Waits.WAIT_LOAD_SYSTEM_KEY,
                        openStore(context).scanRecords(TupleRange.ALL, null, ScanProperties.FORWARD_SCAN)
                        .getCount()).intValue();
                continuing = maxDocId > 0;
            }
            for (int i = 0; i < 10; i++) {
                saveNewRecord();
            }
        }

        void saveNewRecords(int count) {
            for (int i = 0; i < count; i++) {
                saveNewRecord();
            }
        }

        private FDBRecordStore openStore(final FDBRecordContext context) {
            final FDBRecordStore store = LuceneIndexTestUtils.rebuildIndexMetaData(context, path, TextIndexTestUtils.COMPLEX_DOC, INDEX, useRewritePlanner).getLeft();
            return store;
        }

        void saveNewRecord() {
            try (FDBRecordContext context = openContext()) {
                final FDBRecordStore store = openStore(context);
                store.saveRecord(
                        TestRecordsTextProto.ComplexDocument.newBuilder()
                                .setDocId(maxDocId)
                                .setText(LuceneIndexTestUtils.generateRandomWords(500)[1])
                                .setIsSeen(random.nextBoolean())
                                .setGroup(1)
                                .build());
                maxDocId++;
                context.commit();
            }
        }

        public void updateRecords(final int count) {
            try (FDBRecordContext context = openContext()) {
                final FDBRecordStore store = openStore(context);
                for (int i = 0; i < count; i++) {
                    final TestRecordsTextProto.ComplexDocument.Builder builder = TestRecordsTextProto.ComplexDocument.newBuilder()
                            .mergeFrom(store.loadRecord(Tuple.from(1, random.nextInt(maxDocId))).getRecord());
                    builder.setIsSeen(!builder.getIsSeen());
                    store.saveRecord(builder.build());
                }
                context.commit();
            }
        }
    }
}
