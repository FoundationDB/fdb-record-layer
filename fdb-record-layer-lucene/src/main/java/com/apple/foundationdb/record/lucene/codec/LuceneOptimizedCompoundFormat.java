/*
 * LuceneOptimizedCompoundFormat.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2021 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.lucene.LuceneLogMessageKeys;
import com.apple.foundationdb.record.lucene.directory.FDBDirectory;
import com.apple.foundationdb.record.lucene.directory.FDBDirectoryUtils;
import com.apple.foundationdb.record.lucene.directory.FDBLuceneFileReference;
import org.apache.lucene.codecs.CompoundDirectory;
import org.apache.lucene.codecs.CompoundFormat;
import org.apache.lucene.codecs.lucene50.Lucene50CompoundFormat;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Wrapper for the {@link Lucene50CompoundFormat} to optimize compound files for sitting on FoundationDB.
 */
public class LuceneOptimizedCompoundFormat extends CompoundFormat {
    /** Extension of compound file. */
    public static final String DATA_EXTENSION = "cfs";
    /** Extension of compound file entries. */
    public static final String ENTRIES_EXTENSION = "cfe";
    public static final String ENTRY_CODEC = "Lucene50CompoundEntries";
    public static final int VERSION_START = 0;
    static final int VERSION_CURRENT = VERSION_START;

    private final CompoundFormat compoundFormat;

    public LuceneOptimizedCompoundFormat(final CompoundFormat underlying) {
        this.compoundFormat = underlying;
    }

    @Override
    public CompoundDirectory getCompoundReader(Directory dir, final SegmentInfo si, final IOContext context) throws IOException {
        return new LuceneOptimizedCompoundReader(dir, si, context);
    }

    @Override
    public void write(Directory dir, final SegmentInfo si, final IOContext context) throws IOException {
        // Make sure all fetches are initiated in advance of the compoundFormat sequentially stepping through them.
        for (String s : si.files()) {
            dir.openInput(s, IOContext.READONCE)
                    // even though we're not interacting with them, make sure we close the file
                    .close();
        }
        final Set<String> filesForAfter = Set.copyOf(si.files());
        // We filter out the FieldInfos and the StoredFields files before passing to underlying compoundFormat.write, because that expects
        // everything to be a "proper" index format (e.g. have a header), but these meta files are empty.
        Set<String> filteredFiles = filterMarkerFiles(si.files());
        si.setFiles(filteredFiles);
        @SuppressWarnings("PMD.CloseResource") // we don't need to close this because it is just extracting from the dir
        final FDBDirectory directory = FDBDirectoryUtils.getFDBDirectoryNotCompound(dir);
        compoundFormat.write(dir, si, context);
        si.setFiles(filesForAfter);
        final String fieldInfosFileName = filesForAfter.stream().filter(FDBDirectory::isFieldInfoFile).findFirst().orElseThrow();
        final FDBLuceneFileReference fieldInfosReference = directory.getFDBLuceneFileReference(fieldInfosFileName);
        String entriesFile = IndexFileNames.segmentFileName(si.name, "", ENTRIES_EXTENSION);
        directory.setFieldInfoId(entriesFile, fieldInfosReference.getFieldInfosId(), fieldInfosReference.getFieldInfosBitSet());
    }

    private Set<String> filterMarkerFiles(Set<String> files) {
        int fieldInfos = 0;
        int storedFields = 0;
        Set<String> filteredFiles = new HashSet<>(files.size());
        for (String file: files) {
            if (FDBDirectory.isFieldInfoFile(file)) {
                fieldInfos++;
            } else if (FDBDirectory.isStoredFieldsFile(file)) {
                storedFields++;
            } else {
                filteredFiles.add(file);
            }
        }

        if (fieldInfos != 1) {
            throw new RecordCoreException("Segment has wrong number of FieldInfos")
                    .addLogInfo(LuceneLogMessageKeys.FILE_LIST, files);
        }
        // There could be no stored field files in some cases
        if (storedFields > 1) {
            throw new RecordCoreException("Segment has wrong number of StoredFields")
                    .addLogInfo(LuceneLogMessageKeys.FILE_LIST, files);
        }

        return filteredFiles;
    }
}
