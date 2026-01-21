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

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.lucene.LuceneExceptions;
import com.apple.foundationdb.record.lucene.LuceneIndexOptions;
import com.apple.foundationdb.record.lucene.LucenePrimaryKeySegmentIndexV1;
import com.apple.foundationdb.record.lucene.directory.FDBDirectory;
import com.apple.foundationdb.record.lucene.directory.FDBDirectoryUtils;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.codecs.StoredFieldsWriter;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.Directory;
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
    // This is the inner format that is used when LuceneIndexOptions.OPTIMIZED_STORED_FIELDS_FORMAT_ENABLED is FALSE
    private final StoredFieldsFormat storedFieldsFormat;

    LuceneOptimizedStoredFieldsFormat(StoredFieldsFormat storedFieldsFormat) {
        this.storedFieldsFormat = storedFieldsFormat;
    }

    @Override
    public StoredFieldsReader fieldsReader(final Directory directory, final SegmentInfo si, final FieldInfos fn, final IOContext context) throws IOException {
        try {
            FDBDirectory fdbDirectory = FDBDirectoryUtils.getFDBDirectory(directory);

            if (fdbDirectory.usesOptimizedStoredFields()) {
                return new LuceneOptimizedStoredFieldsReader(fdbDirectory, si, fn);
            } else {
                return new LazyStoredFieldsReader(directory, si, fn, context,
                        LazyCloseable.supply(() -> storedFieldsFormat.fieldsReader(directory, si, fn, context)));
            }
        } catch (RecordCoreException ex) {
            throw LuceneExceptions.toIoException(ex, null);
        }
    }

    @SuppressWarnings("PMD.CloseResource")
    @Override
    public StoredFieldsWriter fieldsWriter(final Directory directory, final SegmentInfo si, final IOContext context) throws IOException {
        try {
            FDBDirectory fdbDirectory = FDBDirectoryUtils.getFDBDirectory(directory);
            StoredFieldsWriter storedFieldsWriter;

            // Use FALSE as the default OPTIMIZED_STORED_FIELDS_FORMAT_ENABLED option, for backwards compatibility
            if (fdbDirectory.getBooleanIndexOption(LuceneIndexOptions.PRIMARY_KEY_SEGMENT_INDEX_V2_ENABLED, false)) {
                // Create a "dummy" file to tap into the lifecycle management (e.g. be notified when to delete the data)
                directory.createOutput(IndexFileNames.segmentFileName(si.name, "", LuceneOptimizedStoredFieldsFormat.STORED_FIELDS_EXTENSION), context)
                        .close();
                return new PrimaryKeyAndStoredFieldsWriter(si, fdbDirectory);
            } else {
                if (fdbDirectory.getBooleanIndexOption(LuceneIndexOptions.OPTIMIZED_STORED_FIELDS_FORMAT_ENABLED, false)) {
                    // Create a "dummy" file to tap into the lifecycle management (e.g. be notified when to delete the data)
                    directory.createOutput(IndexFileNames.segmentFileName(si.name, "", LuceneOptimizedStoredFieldsFormat.STORED_FIELDS_EXTENSION), context)
                            .close();
                    storedFieldsWriter = new LuceneOptimizedStoredFieldsWriter(fdbDirectory, si);
                } else {
                    storedFieldsWriter = storedFieldsFormat.fieldsWriter(directory, si, context);
                }

                @Nullable final LucenePrimaryKeySegmentIndexV1 segmentIndex = (LucenePrimaryKeySegmentIndexV1)fdbDirectory.getPrimaryKeySegmentIndex();
                return segmentIndex == null ? storedFieldsWriter : segmentIndex.wrapFieldsWriter(storedFieldsWriter, si);
            }
        } catch (RecordCoreException ex) {
            throw LuceneExceptions.toIoException(ex, null);
        }
    }
}
