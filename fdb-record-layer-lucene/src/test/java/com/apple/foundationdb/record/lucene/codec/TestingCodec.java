package com.apple.foundationdb.record.lucene.codec;

import com.google.auto.service.AutoService;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.CompoundFormat;
import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.FieldInfosFormat;
import org.apache.lucene.codecs.LiveDocsFormat;
import org.apache.lucene.codecs.NormsFormat;
import org.apache.lucene.codecs.PointsFormat;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.SegmentInfoFormat;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.codecs.StoredFieldsWriter;
import org.apache.lucene.codecs.TermVectorsFormat;
import org.apache.lucene.codecs.lucene87.Lucene87Codec;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;

import java.io.IOException;


@AutoService(Codec.class)
public class TestingCodec extends Codec {
    private LuceneOptimizedCodec underlying;
    private static boolean disableLaziness;

    public TestingCodec() {
        super("RLT");
        underlying = new LuceneOptimizedCodec(Lucene87Codec.Mode.BEST_SPEED);
    }

    public static void setDisableLaziness(boolean disableLaziness) {
        TestingCodec.disableLaziness = disableLaziness;
    }

    @Override
    public PostingsFormat postingsFormat() {
        return underlying.postingsFormat();
    }

    @Override
    public DocValuesFormat docValuesFormat() {
        final DocValuesFormat underlyingDocValuesFormat = underlying.docValuesFormat();
        if (disableLaziness) {
            return new DocValuesFormat(underlyingDocValuesFormat.getName()) {
                @Override
                public DocValuesConsumer fieldsConsumer(final SegmentWriteState state) throws IOException {
                    return underlyingDocValuesFormat.fieldsConsumer(state);
                }

                @Override
                public DocValuesProducer fieldsProducer(final SegmentReadState state) throws IOException {
                    final DocValuesProducer underlyingProducer = underlyingDocValuesFormat.fieldsProducer(state);
                    underlyingProducer.ramBytesUsed();
                    return underlyingProducer;
                }
            };
        } else {
            return underlyingDocValuesFormat;
        }
    }

    @Override
    public StoredFieldsFormat storedFieldsFormat() {
        final StoredFieldsFormat storedFieldsFormat = underlying.storedFieldsFormat();
        if (disableLaziness) {
            return new StoredFieldsFormat() {
                @Override
                public StoredFieldsReader fieldsReader(final Directory directory, final SegmentInfo si, final FieldInfos fn, final IOContext context) throws IOException {
                    final StoredFieldsReader storedFieldsReader = storedFieldsFormat.fieldsReader(directory, si, fn, context);
                    storedFieldsReader.ramBytesUsed();
                    return storedFieldsReader;
                }

                @Override
                public StoredFieldsWriter fieldsWriter(final Directory directory, final SegmentInfo si, final IOContext context) throws IOException {
                    return storedFieldsFormat.fieldsWriter(directory, si, context);
                }
            };
        } else {
            return storedFieldsFormat;
        }
    }

    @Override
    public TermVectorsFormat termVectorsFormat() {
        return underlying.termVectorsFormat();
    }

    @Override
    public FieldInfosFormat fieldInfosFormat() {
        return underlying.fieldInfosFormat();
    }

    @Override
    public SegmentInfoFormat segmentInfoFormat() {
        return underlying.segmentInfoFormat();
    }

    @Override
    public NormsFormat normsFormat() {
        return underlying.normsFormat();
    }

    @Override
    public LiveDocsFormat liveDocsFormat() {
        return underlying.liveDocsFormat();
    }

    @Override
    public CompoundFormat compoundFormat() {
        return underlying.compoundFormat();
    }

    @Override
    public PointsFormat pointsFormat() {
        return underlying.pointsFormat();
    }
}