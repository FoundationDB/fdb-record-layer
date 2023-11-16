/*
 * LuceneOptimizedStoredFieldsFormat.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2023 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.lucene.codec;

import com.apple.foundationdb.record.lucene.LuceneIndexOptions;
import com.apple.foundationdb.record.lucene.LucenePrimaryKeySegmentIndex;
import com.apple.foundationdb.record.lucene.directory.FDBDirectory;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.codecs.StoredFieldsWriter;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;

import javax.annotation.Nullable;
import java.io.IOException;

/**
 * This class provides a custom KeyValue based reader and writer implementation to limit the amount of
 * data needed to be read from FDB.
 *
 */
public class LuceneOptimizedStoredFieldsFormat extends StoredFieldsFormat {
    public static final String STORED_FIELDS_EXTENSION = "fsf";
    private final StoredFieldsFormat storedFieldsFormat;

    LuceneOptimizedStoredFieldsFormat(StoredFieldsFormat storedFieldsFormat) {
        this.storedFieldsFormat = storedFieldsFormat;
    }

    @Override
    public StoredFieldsReader fieldsReader(final Directory directory, final SegmentInfo si, final FieldInfos fn, final IOContext context) throws IOException {
        FDBDirectory fdbDirectory = toFdbDirectory(directory);

        if (fdbDirectory.getBooleanIndexOption(LuceneIndexOptions.OPTIMIZED_STORED_FIELDS_FORMAT_ENABLED, false)) {
            return new LuceneOptimizedStoredFieldsReader(fdbDirectory, si, fn);
        } else {
            return new LazyStoredFieldsReader(directory, si, fn, context,
                    LazyCloseable.supply(() -> storedFieldsFormat.fieldsReader(directory, si, fn, context)));
        }
    }

    @SuppressWarnings("PMD.CloseResource")
    @Override
    public StoredFieldsWriter fieldsWriter(final Directory directory, final SegmentInfo si, final IOContext context) throws IOException {
        FDBDirectory fdbDirectory = toFdbDirectory(directory);
        @Nullable final LucenePrimaryKeySegmentIndex segmentIndex = fdbDirectory.getPrimaryKeySegmentIndex();
        StoredFieldsWriter storedFieldsWriter;

        // Use TRUE as the default OPTIMIZED_STORED_FIELDS_FORMAT_ENABLED option
        if (fdbDirectory.getBooleanIndexOption(LuceneIndexOptions.OPTIMIZED_STORED_FIELDS_FORMAT_ENABLED, false)) {
            storedFieldsWriter = new LuceneOptimizedStoredFieldsWriter(fdbDirectory, si, context);
        } else {
            storedFieldsWriter = storedFieldsFormat.fieldsWriter(directory, si, context);
        }
        return segmentIndex == null ? storedFieldsWriter : segmentIndex.wrapFieldsWriter(storedFieldsWriter, si);
    }

    @SuppressWarnings("PMD.CloseResource")
    private FDBDirectory toFdbDirectory(Directory directory) {
        Directory delegate = FilterDirectory.unwrap(directory);
        if (delegate instanceof LuceneOptimizedCompoundReader) {
            delegate = ((LuceneOptimizedCompoundReader)delegate).getDirectory();
        }
        if (delegate instanceof LuceneOptimizedWrappedDirectory) {
            delegate = ((LuceneOptimizedWrappedDirectory)delegate).getFdbDirectory();
        }
        if (delegate instanceof FDBDirectory) {
            return (FDBDirectory)delegate;
        } else {
            throw new RuntimeException("Expected FDB Directory " + delegate.getClass());
        }
    }
}
