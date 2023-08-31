package com.apple.foundationdb.record.lucene.codec;

import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseFactory;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.compressing.CompressingCodec;
import org.apache.lucene.codecs.mockrandom.MockRandomPostingsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.BaseStoredFieldsFormatTestCase;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.RandomCodec;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.SimpleMergedSegmentWarmer;
import org.apache.lucene.index.VisibleAccumulator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.RamUsageTester;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Locale;
import java.util.Random;
import java.util.Set;

// Tip: if you see a failure that has something like:
// 	at __randomizedtesting.SeedInfo.seed([C185081D42F0F43C]:0)
// or
// 	at __randomizedtesting.SeedInfo.seed([C185081D42F0F43C:33261A5D888FEB6A]:0)
// You can add
// @Seed("C185081D42F0F43C")
// to rerun the test class with the same seed. That will work even if you then only run one of the tests
@ThreadLeakFilters(defaultFilters = true, filters = {
        FDBThreadFilter.class
})
public class LuceneOptimizedStoredFieldsFormatTest extends BaseStoredFieldsFormatTestCase {

    public LuceneOptimizedStoredFieldsFormatTest() {
        FDBDatabaseFactory factory = FDBDatabaseFactory.instance();
        factory.getDatabase();
    }

    @Override
    protected Codec getCodec() {
        if (System.getProperty("tests.directory", "random").equals(TestFDBDirectory.class.getName())) {
            return new LuceneOptimizedCodec();
        } else {
            return CompressingCodec.randomInstance(new Random());
        }
    }

    @Override
    public void testRamBytesUsed() throws IOException {
        //TestFDBDirectory.setFullBufferToSurviveDeletes(true);
        // there are two changes to the code below:
        // 1. We create the SimpleMergedSegmentWarmer immediately after assigning reader1
        //    This is because we try to be lazy about loading, and the original code warmed the readers *after*
        //    deleting the segment
        // 2. It uses VisibleAccumulator, because Accumulator is not accessible here
        try {
            if (Codec.getDefault() instanceof RandomCodec) {
              // this test relies on the fact that two segments will be written with
              // the same codec so we need to disable MockRandomPF
              final Set<String> avoidCodecs = new HashSet<>(((RandomCodec) Codec.getDefault()).avoidCodecs);
              avoidCodecs.add(new MockRandomPostingsFormat().getName());
              Codec.setDefault(new RandomCodec(random(), avoidCodecs));
            }
            Directory dir = applyCreatedVersionMajor(newDirectory());
            IndexWriterConfig cfg = newIndexWriterConfig(new MockAnalyzer(random()));
            IndexWriter w = new IndexWriter(dir, cfg);
            // we need to index enough documents so that constant overhead doesn't dominate
            final int numDocs = atLeast(10000);
            LeafReader reader1 = null;
            for (int i = 0; i < numDocs; ++i) {
              Document d = new Document();
              addRandomFields(d);
              w.addDocument(d);
              if (i == 100) {
                w.forceMerge(1);
                w.commit();
                  reader1 = getOnlyLeafReader(DirectoryReader.open(dir));
                  new SimpleMergedSegmentWarmer(InfoStream.NO_OUTPUT).warm(reader1);
              }
            }
            w.forceMerge(1);
            w.commit();
            w.close();

            LeafReader reader2 = getOnlyLeafReader(DirectoryReader.open(dir));

            for (LeafReader reader : Arrays.asList(reader1, reader2)) {
              new SimpleMergedSegmentWarmer(InfoStream.NO_OUTPUT).warm(reader);
            }

            long act1 = RamUsageTester.sizeOf(reader2, new VisibleAccumulator(reader2));
            long act2 = RamUsageTester.sizeOf(reader1, new VisibleAccumulator(reader1));
            final long measuredBytes = act1 - act2;

            long reported1 = ((SegmentReader) reader2).ramBytesUsed();
            long reported2 = ((SegmentReader) reader1).ramBytesUsed();
            final long reportedBytes = reported1 - reported2;

            final long absoluteError = Math.abs(measuredBytes - reportedBytes);
            final double relativeError = (double) absoluteError / measuredBytes;
            final String message = String.format(Locale.ROOT,
                "RamUsageTester reports %d bytes but ramBytesUsed() returned %d (%.1f error). " +
                " [Measured: %d, %d. Reported: %d, %d]",
                measuredBytes,
                reportedBytes,
                (100 * relativeError),
                act1, act2,
                reported1, reported2);

            assertTrue(message, relativeError < 0.20d || absoluteError < 1000);

            reader1.close();
            reader2.close();
            dir.close();
        } finally {
            TestFDBDirectory.setFullBufferToSurviveDeletes(false);
        }
    }
}
