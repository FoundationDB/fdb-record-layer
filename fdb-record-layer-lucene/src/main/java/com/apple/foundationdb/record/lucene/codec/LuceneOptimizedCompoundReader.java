/*
 * LuceneOptimizedCompoundReader.java
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


import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;
import org.apache.lucene.util.IOUtils;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * Borrowed from Lucene to remove checksum from end of file.
 *
 * Class for accessing a compound stream.
 * This class implements a directory, but is limited to only read operations.
 * Directory methods that would normally modify data throw an exception.
 * @lucene.experimental
 */
final class LuceneOptimizedCompoundReader extends Directory {

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
        String entriesFileName = IndexFileNames.segmentFileName(segmentName, "", LuceneOptimizedCompoundFormat.ENTRIES_EXTENSION);
        this.entries = readEntries(si.getId(), directory, entriesFileName);
        String dataFileName = IndexFileNames.segmentFileName(segmentName, "", LuceneOptimizedCompoundFormat.DATA_EXTENSION);
        handle = directory.openInput(dataFileName, context);
        /* REMOVED TO NOT READ THE CHECKSUM ON EACH SEARCH
        try {
            CodecUtil.checkIndexHeader(handle, LuceneOptimizedCompoundFormat.DATA_CODEC, version, version, si.getId(), "");

            // NOTE: data file is too costly to verify checksum against all the bytes on open,
            // but for now we at least verify proper structure of the checksum footer: which looks
            // for FOOTER_MAGIC + algorithmID. This is cheap and can detect some forms of corruption
            // such as file truncation.
            CodecUtil.retrieveChecksum(handle);

            // We also validate length, because e.g. if you strip 16 bytes off the .cfs we otherwise
            // would not detect it:
            if (handle.length() != expectedLength) {
                throw new CorruptIndexException("length should be " + expectedLength + " bytes, but is " + handle.length() + " instead", handle);
            }

            success = true;
        } finally {
            if (!success) {
                IOUtils.closeWhileHandlingException(handle);
            }
        }
         */
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

    /** Not implemented.
     * @throws UnsupportedOperationException always: not supported by CFS */
    @Override
    public void deleteFile(String name) {
        throw new UnsupportedOperationException();
    }

    /** Not implemented.
     * @throws UnsupportedOperationException always: not supported by CFS */
    @Override
    public void rename(String from, String to) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void syncMetaData() {
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
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void sync(Collection<String> names) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Lock obtainLock(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
        return "CompoundFileDirectory(segment=\"" + segmentName + "\" in dir=" + directory + ")";
    }
}

