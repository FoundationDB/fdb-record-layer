package com.apple.foundationdb.record.lucene.codec;

import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseFactory;
import com.carrotsearch.randomizedtesting.annotations.Seed;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.compressing.CompressingCodec;
import org.apache.lucene.index.BaseStoredFieldsFormatTestCase;

import java.util.Random;

@ThreadLeakFilters(defaultFilters = true, filters = {
        FDBThreadFilter.class
})
@Seed("BABFF3B9D3064125")
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
}
