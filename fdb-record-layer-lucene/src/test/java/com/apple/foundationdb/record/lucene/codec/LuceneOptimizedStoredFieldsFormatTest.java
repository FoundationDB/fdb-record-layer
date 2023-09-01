package com.apple.foundationdb.record.lucene.codec;

import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseFactory;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.compressing.CompressingCodec;
import org.apache.lucene.index.BaseStoredFieldsFormatTestCase;

import java.io.IOException;
import java.util.Random;

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
            return new TestingCodec();
        } else {
            return CompressingCodec.randomInstance(new Random());
        }
    }

    @Override
    public void testNumericField() throws Exception {
        TestingCodec.setDisableLaziness(true);
        try {
            super.testNumericField();
        } finally {
            TestingCodec.setDisableLaziness(false);
        }
    }

    @Override
    public void testRamBytesUsed() throws IOException {
        TestingCodec.setDisableLaziness(true);
        TestFDBDirectory.setFullBufferToSurviveDeletes(true);
        try {
            super.testRamBytesUsed();
        } finally {
            TestFDBDirectory.setFullBufferToSurviveDeletes(true);
            TestingCodec.setDisableLaziness(false);
        }
    }
}
