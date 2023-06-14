/*
 * LuceneOptimizedCompoundReader.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2022 Apple Inc. and the FoundationDB project authors
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

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.CompoundDirectory;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.IOUtils;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Class for accessing a compound stream.
 * This class implements a directory, but is limited to only read operations.
 * Directory methods that would normally modify data throw an exception.
 *
 * Borrowed from Lucene to remove checksum from end of file.
 * This implementation skips the reading and validation of checksum, which could have quite big data size sometimes.
 *
 * @lucene.experimental
 */
final class LuceneOptimizedCompoundReader extends CompoundDirectory {

    private final Directory directory;
    private final String segmentName;
    private final Map<String, FileEntry> entries;
    private final IndexInput handle;

    /** Offset/Length for a slice inside of a compound file. */
    public static final class FileEntry {
        long offset;
        long length;
    }

    /**
     * Create a new CompoundFileDirectory.
     */
    // TODO: we should just pre-strip "entries" and append segment name up-front like simpletext?
    // this need not be a "general purpose" directory anymore (it only writes index files)
    public LuceneOptimizedCompoundReader(Directory directory, SegmentInfo si, IOContext context) throws IOException {
        this.directory = directory;
        this.segmentName = si.name;
        String dataFileName = IndexFileNames.segmentFileName(segmentName, "", LuceneOptimizedCompoundFormat.DATA_EXTENSION);
        String entriesFileName = IndexFileNames.segmentFileName(segmentName, "", LuceneOptimizedCompoundFormat.ENTRIES_EXTENSION);
        if ( !(directory instanceof LuceneOptimizedWrappedDirectory) ) {
            throw new IOException("Directory Must Be Wrapped");
        }
        handle = ((LuceneOptimizedWrappedDirectory) directory).openLazyInput(dataFileName, context, 0, 0L); // attempting not to read
        this.entries = readEntries(si.getId(), directory, entriesFileName); // synchronous
    }

    /** Helper method that reads CFS entries from an input stream. */
    @SuppressWarnings("PMD.AvoidCatchingThrowable")
    private Map<String, FileEntry> readEntries(byte[] segmentID, Directory dir, String entriesFileName) throws IOException {
        Map<String, FileEntry> mapping = null;
        try (ChecksumIndexInput entriesStream = dir.openChecksumInput(entriesFileName, IOContext.READONCE)) {
            Throwable priorE = null;
            try {
                CodecUtil.checkIndexHeader(entriesStream, LuceneOptimizedCompoundFormat.ENTRY_CODEC,
                        LuceneOptimizedCompoundFormat.VERSION_START,
                        LuceneOptimizedCompoundFormat.VERSION_CURRENT, segmentID, "");
                final int numEntries = entriesStream.readVInt();
                mapping = new HashMap<>(numEntries);
                for (int i = 0; i < numEntries; i++) {
                    final FileEntry fileEntry = new FileEntry();
                    final String id = entriesStream.readString();
                    FileEntry previous = mapping.put(id, fileEntry);
                    if (previous != null) {
                        throw new CorruptIndexException("Duplicate cfs entry id=" + id + " in CFS ", entriesStream);
                    }
                    fileEntry.offset = entriesStream.readLong();
                    fileEntry.length = entriesStream.readLong();
                }
            } catch (Throwable exception) {
                priorE = exception;
            } finally {
                CodecUtil.checkFooter(entriesStream, priorE);
            }
        }
        return Collections.unmodifiableMap(mapping);
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(handle);
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        ensureOpen();
        final String id = IndexFileNames.stripSegmentName(name);
        final FileEntry entry = entries.get(id);
        if (entry == null) {
            String datFileName = IndexFileNames.segmentFileName(segmentName, "", LuceneOptimizedCompoundFormat.DATA_EXTENSION);
            throw new FileNotFoundException("No sub-file with id " + id + " found in compound file \"" + datFileName + "\" (fileName=" + name + " files: " + entries.keySet() + ")");
        }
        return handle.slice(name, entry.offset, entry.length);
    }

    /** Returns an array of strings, one for each file in the directory. */
    @Override
    public String[] listAll() {
        ensureOpen();
        String[] res = entries.keySet().toArray(new String[entries.size()]);

        // Add the segment name
        for (int i = 0; i < res.length; i++) {
            res[i] = segmentName + res[i];
        }
        return res;
    }

    /** Returns the length of a file in the directory.
     * @throws IOException if the file does not exist */
    @Override
    public long fileLength(String name) throws IOException {
        ensureOpen();
        FileEntry e = entries.get(IndexFileNames.stripSegmentName(name));
        if (e == null) {
            throw new FileNotFoundException(name);
        }
        return e.length;
    }

    @Override
    public String toString() {
        return "CompoundFileDirectory(segment=\"" + segmentName + "\" in dir=" + directory + ")";
    }

    @Override
    public Set<String> getPendingDeletions() {
        return Collections.emptySet();
    }

    @Override
    public void checkIntegrity() throws IOException {
        CodecUtil.checksumEntireFile(handle);
    }
}
